use core::sync::atomic::Ordering;
use std::sync::Arc;

use crate::{SlotPtr, buffer::Buffer, spinlock::Spinlock};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    QueueFull,
    BatchTooLarge,
}

pub struct Producer<T, const N: usize> {
    pub(crate) buffer: Arc<Buffer<T, N>>,
    pub(crate) slot_ptr: SlotPtr,
    cached_cl_read: SlotPtr,
}

impl<T, const N: usize> Producer<T, N> {
    pub(crate) fn new(buffer: &Arc<Buffer<T, N>>) -> Self {
        Self {
            buffer: buffer.clone(),
            slot_ptr: SlotPtr::from((0, 0)),
            cached_cl_read: SlotPtr::from((buffer.cl_mask, N)),
        }
    }

    pub fn send(&mut self, mut value: T) {
        let mut spinlock = Spinlock::new();

        loop {
            match self.try_send(value) {
                Ok(()) => break,
                Err((returned_value, _)) => {
                    value = returned_value;
                    spinlock.spin_heavy();
                }
            }
        }
    }

    pub fn try_send(&mut self, value: T) -> Result<(), (T, Error)> {
        let (curr_head, curr_cl_head) = self.slot_ptr.into();

        // slow path when trying to wrap around at a cache line border
        if curr_cl_head == N {
            let next_head = (curr_head + 1) & self.buffer.cl_mask;

            if next_head == self.cached_cl_read.cl_index {
                self.refresh_cached_cl_read();
            }

            if next_head == self.cached_cl_read.cl_index {
                return Err((value, Error::QueueFull));
            }

            // Safety: next_head is verified to not overlap with the reader and is within bounds
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
        let final_batch_size = self.clamp_batch_size(buf.len())?;

        Ok(unsafe { self.send_batch_exact_unchecked(&buf[..final_batch_size]) })
    }

    pub fn try_send_batch_exact(&mut self, buf: &[T]) -> Result<usize, Error>
    where
        T: Copy,
    {
        self.validate_batch_size_exact(buf.len())?;

        Ok(unsafe { self.send_batch_exact_unchecked(buf) })
    }

    // Safety: The caller has to make sure to validate that there are buf.len()
    // items free to write to the buffer
    unsafe fn send_batch_exact_unchecked(&mut self, buf: &[T]) -> usize
    where
        T: Copy,
    {
        let batch_size = buf.len();
        let (s1, s2) = unsafe { self.buffer.as_slice_mut(self.slot_ptr, batch_size) };

        let s1_len = s1.len();

        unsafe {
            core::ptr::copy_nonoverlapping(buf.as_ptr(), s1.as_mut_ptr(), s1_len);
            core::ptr::copy_nonoverlapping(buf.as_ptr().add(s1_len), s2.as_mut_ptr(), s2.len());
        };

        self.buffer.advance_slot_ptr(
            &mut self.slot_ptr,
            batch_size,
            &self.buffer.head,
            Ordering::Release,
        );

        batch_size
    }

    pub fn try_send_with<F>(&mut self, size: usize, with: F) -> Result<usize, Error>
    where
        F: FnOnce(&mut [T], &mut [T]),
    {
        let size = self.clamp_batch_size(size)?;

        let (s1, s2) = unsafe { self.buffer.as_slice_mut(self.slot_ptr, size) };
        with(s1, s2);

        self.buffer.advance_slot_ptr(
            &mut self.slot_ptr,
            size,
            &self.buffer.head,
            Ordering::Release,
        );

        Ok(size)
    }

    pub fn try_send_exact_with<F>(&mut self, size: usize, with: F) -> Result<usize, Error>
    where
        F: FnOnce(&mut [T], &mut [T]),
    {
        let size = self.validate_batch_size_exact(size)?;

        let (s1, s2) = unsafe { self.buffer.as_slice_mut(self.slot_ptr, size) };
        with(s1, s2);

        self.buffer.advance_slot_ptr(
            &mut self.slot_ptr,
            size,
            &self.buffer.head,
            Ordering::Release,
        );

        Ok(size)
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        let (curr_cl_index, curr_cl_offset) = self.slot_ptr.into();

        if curr_cl_offset == 0 {
            return Ok(());
        }

        let next_head = (curr_cl_index + 1) & self.buffer.cl_mask;

        if next_head == self.cached_cl_read.cl_index {
            self.refresh_cached_cl_read();
        }

        if next_head == self.cached_cl_read.cl_index {
            return Err(Error::QueueFull);
        }

        if curr_cl_offset < N {
            self.buffer.interruptions.push(self.slot_ptr);
        }

        self.slot_ptr.set(next_head, 0);
        self.buffer.head.store(next_head, Ordering::Release);

        Ok(())
    }

    #[inline]
    fn continuous_free(&self) -> usize {
        let (head_cl_index, head_cl_offset) = self.slot_ptr.into();
        let cached_cl_index = self.cached_cl_read.cl_index;

        let free_cache_lines =
            cached_cl_index.wrapping_sub(head_cl_index).wrapping_sub(1) & self.buffer.cl_mask;

        let free_slots_total = free_cache_lines * N;
        let free_slots_curr_cl = N - head_cl_offset;

        free_slots_total + free_slots_curr_cl
    }

    #[inline]
    fn refresh_cached_cl_read(&mut self) {
        let curr_tail = self.buffer.tail.load(Ordering::Acquire);
        self.cached_cl_read.set(curr_tail, N);
    }

    #[inline]
    fn clamp_batch_size(&mut self, requested: usize) -> Result<usize, Error> {
        self.refresh_cached_cl_read();

        crate::buffer::clamp_batch_size(
            requested,
            self.buffer.max_size(),
            self.continuous_free(),
            Error::QueueFull,
        )
    }

    #[inline]
    fn validate_batch_size_exact(&mut self, requested: usize) -> Result<usize, Error> {
        self.refresh_cached_cl_read();

        crate::buffer::validate_exact_batch_size(
            requested,
            self.buffer.max_size(),
            self.continuous_free(),
            Error::QueueFull,
            Error::BatchTooLarge,
        )
    }
}

#[cfg(test)]
mod producer_tests {
    use crate::channel;
    use std::sync::atomic::Ordering;

    const CL_CAPACITY: usize = 8;
    const TOTAL_CAPACITY: usize = 4 * CL_CAPACITY;

    type TestMessage = usize;

    // TODO: add updated flush tests

    #[test]
    fn test_wrapping_is_lazy() {
        let (mut tx, _) = channel!(TestMessage, TOTAL_CAPACITY);
        let cl_index = tx.slot_ptr.cl_index;

        assert!(tx.try_send_batch(&[0; CL_CAPACITY]).is_ok());
        assert_eq!(tx.slot_ptr.cl_index, cl_index);
        assert_eq!(tx.slot_ptr.cl_index, tx.buffer.head.load(Ordering::Acquire));
        assert_eq!(tx.slot_ptr.cl_offset, CL_CAPACITY);

        assert!(tx.try_send(7).is_ok());
        assert_eq!(tx.slot_ptr.cl_index, cl_index + 1);
        assert_eq!(tx.slot_ptr.cl_index, tx.buffer.head.load(Ordering::Acquire));
        assert_eq!(tx.slot_ptr.cl_offset, 1);
    }

    #[test]
    fn test_advance_cl_index_forward() {
        let (mut tx, _) = channel!(TestMessage, TOTAL_CAPACITY);
        let cl_index = tx.slot_ptr.cl_index;

        assert_eq!(tx.slot_ptr.cl_index, tx.buffer.head.load(Ordering::Acquire));
        assert!(tx.try_send_batch(&[0; CL_CAPACITY + 1]).is_ok());
        assert_eq!(tx.slot_ptr.cl_index, cl_index + 1);
        assert_eq!(tx.slot_ptr.cl_index, tx.buffer.head.load(Ordering::Acquire));
    }

    #[test]
    fn test_advance_cl_index_wrapping() {
        let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);
        let last_cl_index = 3;

        tx.slot_ptr.set(last_cl_index, CL_CAPACITY);
        tx.buffer.head.store(last_cl_index, Ordering::Release);

        rx.slot_ptr.set(last_cl_index - 1, CL_CAPACITY);
        rx.buffer.tail.store(last_cl_index - 1, Ordering::Release);

        assert_eq!(tx.slot_ptr.cl_index, tx.buffer.head.load(Ordering::Acquire));
        assert_eq!(tx.slot_ptr.cl_index, last_cl_index);
        assert!(tx.try_send(7).is_ok());
        assert_eq!(tx.slot_ptr.cl_index, tx.buffer.head.load(Ordering::Acquire));
        assert_eq!(tx.slot_ptr.cl_index, 0);
    }

    #[test]
    fn test_advance_cl_offset_forward() {
        let (mut tx, _) = channel!(TestMessage, TOTAL_CAPACITY);
        let cl_index = tx.slot_ptr.cl_index;
        let cl_offset = tx.slot_ptr.cl_offset;

        assert!(tx.try_send(7).is_ok());
        assert_eq!(tx.slot_ptr.cl_index, cl_index);
        assert_eq!(tx.slot_ptr.cl_offset, cl_offset + 1);
    }

    #[test]
    fn test_advance_cl_offset_wrapping() {
        let (mut tx, _) = channel!(TestMessage, TOTAL_CAPACITY);
        let cl_index = tx.slot_ptr.cl_index;

        assert!(tx.try_send_batch(&[0; CL_CAPACITY]).is_ok());
        assert_eq!(tx.slot_ptr.cl_index, cl_index);
        assert_eq!(tx.slot_ptr.cl_offset, CL_CAPACITY);

        assert!(tx.try_send(7).is_ok());
        assert_eq!(tx.slot_ptr.cl_index, cl_index + 1);
        assert_eq!(tx.slot_ptr.cl_offset, 1);
    }
}
