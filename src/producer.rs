use std::sync::{Arc, atomic::Ordering};

use crate::{SlotPtr, buffer::Buffer, reservation::SendReservation, spinlock::Spinlock};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    QueueFull,
    BatchTooLarge,
}

pub struct Producer<T, const N: usize> {
    pub(crate) buffer: Arc<Buffer<T, N>>,
    pub(crate) slot_ptr: SlotPtr,
}

impl<T, const N: usize> Producer<T, N> {
    pub(crate) fn new(buffer: &Arc<Buffer<T, N>>) -> Self {
        Self {
            buffer: buffer.clone(),
            slot_ptr: SlotPtr::from((0, 0)),
        }
    }

    pub fn send(&mut self, mut value: T) {
        let mut spinlock = Spinlock::new();

        loop {
            match self.try_send(value) {
                Ok(()) => break,
                Err((returned_value, _)) => {
                    value = returned_value;
                    spinlock.spin_heavy()
                }
            };
        }
    }

    pub fn try_send(&mut self, value: T) -> Result<(), (T, Error)> {
        let (curr_head, curr_cl_head) = self.slot_ptr.into();

        // slow path when trying to wrap around at a cache line border
        // if we finished writing to a cache line in the previous send
        if curr_cl_head == N {
            // Calculate the index of the next cache line by wrapping around buffer bounds using
            // fast modulo since cache_lines is always a power of 2
            let next_head = (curr_head + 1) & self.buffer.cl_mask;

            // Sync with the reader's release when advancing its tail
            let curr_tail = self.buffer.tail.load(Ordering::Acquire);

            if next_head == curr_tail {
                return Err((value, Error::QueueFull));
            }

            let lo_ptr = SlotPtr::from((curr_head, 0));
            let hi_ptr = SlotPtr::from((next_head, 0));
            self.buffer.slot_tracker.mark_used(lo_ptr, hi_ptr);

            // Safety: next_head is verified to not overlap with curr_tail and is within bounds
            let next_cache_line = unsafe { self.buffer.get_cache_line(next_head) };
            unsafe { next_cache_line.write(0, value) };

            self.slot_ptr.set(next_head, 1);

            // Sync the advancement with the read thread
            self.buffer.head.store(next_head, Ordering::Release);
        }
        // fast path for the currently exclusively owned cache line
        else {
            // Safety: curr_head is always within bounds and never overlaps with the read head
            let cache_line = unsafe { self.buffer.get_cache_line(curr_head) };
            unsafe { cache_line.write(curr_cl_head, value) };

            self.slot_ptr.cl_offset = curr_cl_head + 1;
        }

        Ok(())
    }

    pub fn try_send_batch(&mut self, buf: &[T]) -> Result<usize, Error>
    where
        T: Copy,
    {
        let max_batch_size = self.buffer.capacity - N;
        let batch_size = buf.len().min(max_batch_size);
        let continuous_free = self.buffer.slot_tracker.next_free(self.slot_ptr);
        let final_batch_size = batch_size.min(continuous_free);

        if final_batch_size == 0 {
            return Err(Error::QueueFull);
        }

        Ok(unsafe { self.send_batch_exact_unchecked(&buf[0..final_batch_size]) })
    }

    pub fn try_send_batch_exact(&mut self, buf: &[T]) -> Result<usize, Error>
    where
        T: Copy,
    {
        let batch_size = buf.len();
        let max_batch_size = self.buffer.capacity - N;

        if batch_size > max_batch_size {
            return Err(Error::BatchTooLarge);
        }

        let continuous_free = self.buffer.slot_tracker.next_free(self.slot_ptr);

        if batch_size > continuous_free {
            return Err(Error::QueueFull);
        }

        Ok(unsafe { self.send_batch_exact_unchecked(buf) })
    }

    // # Safety: The caller has to make sure to validate that there are buf.len()
    // items free to write to the buffer
    unsafe fn send_batch_exact_unchecked(&mut self, buf: &[T]) -> usize
    where
        T: Copy,
    {
        let (curr_cl_index, curr_cl_offset) = self.slot_ptr.into();
        let batch_size = buf.len();
        let last_abs_index = self.buffer.capacity;
        let from_abs_index = (curr_cl_index * N) + curr_cl_offset;
        let to_abs_index = from_abs_index + batch_size;

        if to_abs_index < last_abs_index {
            let s_ptr = unsafe { self.get_slice_ptr(curr_cl_index, curr_cl_offset) };
            unsafe { core::ptr::copy_nonoverlapping(buf.as_ptr(), s_ptr, batch_size) };
        } else {
            let s1_len = last_abs_index - from_abs_index;
            let s1_ptr = unsafe { self.get_slice_ptr(curr_cl_index, curr_cl_offset) };
            unsafe { core::ptr::copy_nonoverlapping(buf.as_ptr(), s1_ptr, s1_len) };

            let s2_len = batch_size - s1_len;
            let s2_ptr = unsafe { self.get_slice_ptr(0, 0) };
            unsafe { core::ptr::copy_nonoverlapping(buf.as_ptr().add(s1_len), s2_ptr, s2_len) };
        }

        let final_abs_index = to_abs_index % self.buffer.capacity;
        let mut next_cl_index = (final_abs_index / N) & self.buffer.cl_mask;
        let mut next_cl_offset = final_abs_index % N;

        if next_cl_offset == 0 && to_abs_index > 0 {
            next_cl_index = (next_cl_index.wrapping_sub(1)) & self.buffer.cl_mask;
            next_cl_offset = N;
        }

        self.slot_ptr.set(next_cl_index, next_cl_offset);
        let mut i = curr_cl_index;

        while i != next_cl_index {
            let curr = i;
            let next = (i + 1) & self.buffer.cl_mask;
            let lo_ptr = SlotPtr::from((curr, 0));
            let hi_ptr = SlotPtr::from((next, 0));
            self.buffer.slot_tracker.mark_used(lo_ptr, hi_ptr);
            i = next;
        }

        self.buffer.head.store(next_cl_index, Ordering::Release);

        batch_size
    }

    pub fn try_reserve(&mut self, size: usize) -> Result<SendReservation<'_, T, N>, Error>
    where
        T: Copy,
    {
        let max_batch_size = self.buffer.capacity - N;
        let continuous_free = self.buffer.slot_tracker.next_free(self.slot_ptr);
        let reservation_size = size.min(max_batch_size).min(continuous_free);

        if reservation_size == 0 {
            return Err(Error::QueueFull);
        }

        Ok(unsafe { self.reserve_exact_unchecked(reservation_size) })
    }

    pub fn try_reserve_exact(&mut self, size: usize) -> Result<SendReservation<'_, T, N>, Error>
    where
        T: Copy,
    {
        let max_batch_size = self.buffer.capacity - N;

        if size > max_batch_size {
            return Err(Error::BatchTooLarge);
        }

        let continous_free = self.buffer.slot_tracker.next_free(self.slot_ptr);

        if size > continous_free {
            return Err(Error::QueueFull);
        }

        Ok(unsafe { self.reserve_exact_unchecked(size) })
    }

    unsafe fn reserve_exact_unchecked(&mut self, size: usize) -> SendReservation<'_, T, N>
    where
        T: Copy,
    {
        let (curr_cl_index, curr_cl_offset) = self.slot_ptr.into();
        let last_abs_index = self.buffer.capacity;
        let from_abs_index = (curr_cl_index * N) + curr_cl_offset;
        let to_abs_index = from_abs_index + size;

        let (s1, s1_remaining, s2, s2_remaining) = if to_abs_index < last_abs_index {
            let s_ptr = unsafe { self.get_slice_ptr(curr_cl_index, curr_cl_offset) };
            (s_ptr, size, std::ptr::null_mut(), 0)
        } else {
            let s1_len = last_abs_index - from_abs_index;
            let s1_ptr = unsafe { self.get_slice_ptr(curr_cl_index, curr_cl_offset) };
            let s2_len = size - s1_len;
            let s2_ptr = unsafe { self.get_slice_ptr(0, 0) };

            (s1_ptr, s1_len, s2_ptr, s2_len)
        };

        SendReservation {
            tx: self,
            s1,
            s1_remaining,
            s2,
            s2_remaining,
            total_reserved: size,
            start_cl_index: curr_cl_index,
            start_cl_offset: curr_cl_offset,
        }
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        let (curr_cl_index, curr_cl_offset) = self.slot_ptr.into();
        let next_head = (curr_cl_index + 1) & self.buffer.cl_mask;

        // Sync with the reader's release when advancing its tail
        let curr_tail = self.buffer.tail.load(Ordering::Acquire);

        if next_head == curr_tail {
            return Err(Error::QueueFull);
        }

        if curr_cl_offset == N {
            let lo_ptr = SlotPtr::from((curr_cl_index, 0));
            let hi_ptr = SlotPtr::from((next_head, 0));
            self.buffer.slot_tracker.mark_used(lo_ptr, hi_ptr);
        } else {
            let lo_ptr = SlotPtr::from((curr_cl_index, 0));
            let hi_ptr = SlotPtr::from((curr_cl_index, curr_cl_offset));
            self.buffer.slot_tracker.mark_used(lo_ptr, hi_ptr);
        }

        self.slot_ptr.set(next_head, 0);
        self.buffer.head.store(next_head, Ordering::Release);

        Ok(())
    }

    // # Safety: the caller has to make sure that the index start_pos = (cl_index * N) + cl_offset
    // is within the ring buffers bounds
    #[inline]
    unsafe fn get_slice_ptr(&self, cl_index: usize, cl_offset: usize) -> *mut T {
        unsafe {
            self.buffer
                .inner
                .get_unchecked(cl_index)
                .get_item_ptr(cl_offset)
                .cast::<T>()
        }
    }
}

// #[cfg(test)]
// mod producer_tests {
//     use crate::channel;
//     use std::sync::atomic::Ordering;
//
//     const CL_CAPACITY: usize = 8;
//     const TOTAL_CAPACITY: usize = 4 * CL_CAPACITY;
//
//     type TestMessage = usize;
//
//     #[test]
//     fn test_free_slots_one_cl() {
//         let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);
//
//         assert_eq!(tx.free_slots(), TOTAL_CAPACITY - CL_CAPACITY);
//
//         assert!(tx.try_send_batch(&[0; CL_CAPACITY]).is_ok());
//         assert!(tx.flush().is_ok());
//         assert_eq!(tx.free_slots(), TOTAL_CAPACITY - 2 * CL_CAPACITY);
//
//         assert!(rx.try_recv_batch(&mut [0; CL_CAPACITY]).is_ok());
//         assert_eq!(tx.free_slots(), TOTAL_CAPACITY - CL_CAPACITY);
//     }
//
//     #[test]
//     fn test_free_slots_mul_cl() {
//         let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);
//
//         assert_eq!(tx.free_slots(), TOTAL_CAPACITY - CL_CAPACITY);
//
//         assert!(tx.try_send_batch(&[0; CL_CAPACITY / 2]).is_ok());
//         assert!(tx.flush().is_ok());
//         assert_eq!(tx.free_slots(), TOTAL_CAPACITY - 2 * CL_CAPACITY);
//
//         assert!(rx.try_recv_batch(&mut [0; CL_CAPACITY]).is_ok());
//         assert_eq!(tx.free_slots(), TOTAL_CAPACITY - CL_CAPACITY);
//     }
//
//     #[test]
//     fn test_flush_cl_full() {
//         let (mut tx, _) = channel!(TestMessage, TOTAL_CAPACITY);
//         let cl_index = tx.cl_index;
//         assert!(tx.try_send_batch(&[0; CL_CAPACITY]).is_ok());
//
//         let before_flush = unsafe { tx.buffer.write_counts.get_unchecked(cl_index).get().read() };
//         assert_eq!(before_flush, 0);
//
//         assert!(tx.flush().is_ok());
//
//         let after_flush = unsafe { tx.buffer.write_counts.get_unchecked(cl_index).get().read() };
//         assert_eq!(after_flush, CL_CAPACITY);
//     }
//
//     #[test]
//     fn test_flush_cl_partial() {
//         let (mut tx, _) = channel!(TestMessage, TOTAL_CAPACITY);
//         let cl_index = tx.cl_index;
//         assert!(tx.try_send_batch(&[0; CL_CAPACITY / 2]).is_ok());
//
//         let before_flush = unsafe { tx.buffer.write_counts.get_unchecked(cl_index).get().read() };
//         assert_eq!(before_flush, 0);
//
//         assert!(tx.flush().is_ok());
//
//         let after_flush = unsafe { tx.buffer.write_counts.get_unchecked(cl_index).get().read() };
//         assert_eq!(after_flush, CL_CAPACITY / 2);
//     }
//
//     #[test]
//     fn test_wrapping_is_lazy() {
//         let (mut tx, _) = channel!(TestMessage, TOTAL_CAPACITY);
//         let cl_index = tx.cl_index;
//
//         assert!(tx.try_send_batch(&[0; CL_CAPACITY]).is_ok());
//         assert_eq!(tx.cl_index, cl_index);
//         assert_eq!(tx.cl_index, tx.buffer.head.load(Ordering::Acquire));
//         assert_eq!(tx.cl_offset, CL_CAPACITY);
//
//         assert!(tx.try_send(7).is_ok());
//         assert_eq!(tx.cl_index, cl_index + 1);
//         assert_eq!(tx.cl_index, tx.buffer.head.load(Ordering::Acquire));
//         assert_eq!(tx.cl_offset, 1);
//     }
//
//     #[test]
//     fn test_advance_cl_index_forward() {
//         let (mut tx, _) = channel!(TestMessage, TOTAL_CAPACITY);
//         let cl_index = tx.cl_index;
//
//         assert_eq!(tx.cl_index, tx.buffer.head.load(Ordering::Acquire));
//         assert!(tx.try_send_batch(&[0; CL_CAPACITY + 1]).is_ok());
//         assert_eq!(tx.cl_index, cl_index + 1);
//         assert_eq!(tx.cl_index, tx.buffer.head.load(Ordering::Acquire));
//     }
//
//     #[test]
//     fn test_advance_cl_index_wrapping() {
//         let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);
//         let last_cl_index = 3;
//
//         tx.cl_index = last_cl_index;
//         tx.cl_offset = CL_CAPACITY;
//         tx.buffer.head.store(last_cl_index, Ordering::Release);
//
//         rx.cl_index = last_cl_index - 1;
//         rx.cl_offset = CL_CAPACITY;
//         rx.buffer.tail.store(last_cl_index - 1, Ordering::Release);
//
//         assert_eq!(tx.cl_index, tx.buffer.head.load(Ordering::Acquire));
//         assert_eq!(tx.cl_index, last_cl_index);
//         assert!(tx.try_send(7).is_ok());
//         assert_eq!(tx.cl_index, tx.buffer.head.load(Ordering::Acquire));
//         assert_eq!(tx.cl_index, 0);
//     }
//
//     #[test]
//     fn test_advance_cl_offset_forward() {
//         let (mut tx, _) = channel!(TestMessage, TOTAL_CAPACITY);
//         let cl_index = tx.cl_index;
//         let cl_offset = tx.cl_offset;
//
//         assert!(tx.try_send(7).is_ok());
//         assert_eq!(tx.cl_index, cl_index);
//         assert_eq!(tx.cl_offset, cl_offset + 1);
//     }
//
//     #[test]
//     fn test_advance_cl_offset_wrapping() {
//         let (mut tx, _) = channel!(TestMessage, TOTAL_CAPACITY);
//         let cl_index = tx.cl_index;
//
//         assert!(tx.try_send_batch(&[0; CL_CAPACITY]).is_ok());
//         assert_eq!(tx.cl_index, cl_index);
//         assert_eq!(tx.cl_offset, CL_CAPACITY);
//
//         assert!(tx.try_send(7).is_ok());
//         assert_eq!(tx.cl_index, cl_index + 1);
//         assert_eq!(tx.cl_offset, 1);
//     }
// }
