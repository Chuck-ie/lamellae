use std::sync::{Arc, atomic::Ordering};

use crate::{SlotPtr, buffer::Buffer, spinlock::Spinlock};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    QueueEmpty,
    BatchTooLarge,
}

pub struct Consumer<T, const N: usize, const CLS: usize = 0> {
    pub(crate) buffer: Arc<Buffer<T, N>>,
    pub(crate) slot_ptr: SlotPtr,
    cached_cl_write: SlotPtr,
}

impl<T, const N: usize> Consumer<T, N> {
    pub(crate) fn new(buffer: &Arc<Buffer<T, N>>) -> Self {
        let curr_ptr = SlotPtr::from((buffer.cl_mask, N));
        Self {
            buffer: buffer.clone(),
            slot_ptr: curr_ptr,
            cached_cl_write: curr_ptr,
        }
    }

    pub fn recv(&mut self) -> T {
        let mut spinlock = Spinlock::new();

        loop {
            match self.try_recv() {
                Ok(value) => return value,
                Err(_) => spinlock.spin_heavy(),
            };
        }
    }

    pub fn try_recv(&mut self) -> Result<T, Error> {
        let (curr_cl_index, curr_cl_offset) = self.slot_ptr.into();

        // slow path: refresh cache, jump past gap
        if self.slot_ptr == self.cached_cl_write {
            let next_cl_index = (curr_cl_index + 1) & self.buffer.cl_mask;
            let curr_head = self.buffer.head.load(Ordering::Acquire);
            if next_cl_index == curr_head {
                return Err(Error::QueueEmpty);
            }

            if let Some(intr) = self.buffer.interruptions.pop_next() {
                self.cached_cl_write = intr;
            } else {
                let prev_head = curr_head.wrapping_sub(1) & self.buffer.cl_mask;
                self.cached_cl_write = SlotPtr::from((prev_head, N));
            }

            let next_cache_line = unsafe { self.buffer.get_cache_line(next_cl_index) };
            let value = unsafe { next_cache_line.read(0) };
            self.slot_ptr.set(next_cl_index, 1);
            self.buffer.tail.store(next_cl_index, Ordering::Release);
            Ok(value)
        }
        // fast path: cl crossing inside cached range
        else if curr_cl_offset == N {
            let next_cl_index = (curr_cl_index + 1) & self.buffer.cl_mask;
            let next_cache_line = unsafe { self.buffer.get_cache_line(next_cl_index) };
            let value = unsafe { next_cache_line.read(0) };
            self.slot_ptr.set(next_cl_index, 1);
            self.buffer.tail.store(next_cl_index, Ordering::Release);
            Ok(value)
        }
        // fast path: same cl read
        else {
            let cache_line = unsafe { self.buffer.get_cache_line(curr_cl_index) };
            let value = unsafe { cache_line.read(curr_cl_offset) };
            self.slot_ptr.cl_offset = curr_cl_offset + 1;
            Ok(value)
        }
    }

    pub fn try_recv_batch(&mut self, buf: &mut [T]) -> Result<usize, Error>
    where
        T: Copy,
    {
        let max_batch_size = self.buffer.max_size();
        let size = buf.len().min(max_batch_size);

        if size == 0 {
            return Err(Error::QueueEmpty);
        }

        let final_batch_size = size.min(self.continuous_used());

        if final_batch_size == 0 {
            return Err(Error::QueueEmpty);
        }

        // Safety: final_batch_size <= continuous which was verified readable above
        unsafe { self.recv_batch_exact_unchecked(&mut buf[..final_batch_size]) };

        Ok(final_batch_size)
    }

    pub fn try_recv_batch_exact(&mut self, buf: &mut [T]) -> Result<usize, Error>
    where
        T: Copy,
    {
        let size = buf.len();

        if size > self.buffer.max_size() {
            return Err(Error::BatchTooLarge);
        }

        if size > self.continuous_used() {
            return Err(Error::QueueEmpty);
        }

        // Safety: batch_size <= continuous which was verified readable above
        unsafe { self.recv_batch_exact_unchecked(buf) };

        Ok(size)
    }

    // Safety: The caller has to make sure to validate that there are buf.len()
    // items available to read inside the buffer
    unsafe fn recv_batch_exact_unchecked(&mut self, buf: &mut [T]) -> usize
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
            unsafe { core::ptr::copy_nonoverlapping(s_ptr, buf.as_mut_ptr(), batch_size) };
        } else {
            let s1_len = last_abs_index - from_abs_index;
            let s1_ptr = unsafe { self.get_slice_ptr(curr_cl_index, curr_cl_offset) };
            unsafe { core::ptr::copy_nonoverlapping(s1_ptr, buf.as_mut_ptr(), s1_len) };

            let s2_len = batch_size - s1_len;
            let s2_ptr = unsafe { self.get_slice_ptr(0, 0) };
            unsafe { core::ptr::copy_nonoverlapping(s2_ptr, buf.as_mut_ptr().add(s1_len), s2_len) };
        }

        let final_abs_index = to_abs_index % self.buffer.capacity;
        let mut next_cl_index = (final_abs_index / N) & self.buffer.cl_mask;
        let mut next_cl_offset = final_abs_index % N;

        if next_cl_offset == 0 && to_abs_index > 0 {
            next_cl_index = (next_cl_index.wrapping_sub(1)) & self.buffer.cl_mask;
            next_cl_offset = N;
        }

        self.slot_ptr.set(next_cl_index, next_cl_offset);
        self.buffer.tail.store(next_cl_index, Ordering::Release);

        batch_size
    }

    fn continuous_used(&mut self) -> usize {
        self.try_extend_cached_cl_write();

        let used = self.buffer.flat_dist(self.slot_ptr, self.cached_cl_write);

        if used != 0 {
            return used;
        }

        let (curr_cl_index, _) = self.slot_ptr.into();
        let next_cl_index = (curr_cl_index + 1) & self.buffer.cl_mask;
        let curr_head = self.buffer.head.load(Ordering::Acquire);

        if next_cl_index == curr_head {
            return 0;
        }

        if let Some(interruption) = self.buffer.interruptions.pop_next() {
            self.cached_cl_write = interruption;
        } else {
            let prev_head = curr_head.wrapping_sub(1) & self.buffer.cl_mask;
            self.cached_cl_write = SlotPtr::from((prev_head, N));
        }

        self.slot_ptr.set(next_cl_index, 0);
        self.buffer.flat_dist(self.slot_ptr, self.cached_cl_write)
    }

    fn try_extend_cached_cl_write(&mut self) {
        if self.cached_cl_write.cl_offset != N {
            return;
        }

        let curr_head = self.buffer.head.load(Ordering::Acquire);
        let new_bound = SlotPtr::from((curr_head.wrapping_sub(1) & self.buffer.cl_mask, N));

        let extension = self.buffer.flat_dist(self.cached_cl_write, new_bound);

        if extension == 0 {
            return;
        }

        if let Some(intr) = self.buffer.interruptions.peek_next() {
            let dist_to_intr = self.buffer.flat_dist(self.cached_cl_write, intr);

            if dist_to_intr > 0 && dist_to_intr <= extension {
                self.cached_cl_write = self.buffer.interruptions.pop_next().unwrap();
                return;
            }
        }

        self.cached_cl_write = new_bound;
    }

    // # Safety:
    //
    // the caller has to make sure that the index start_pos = (cl_index * N) + cl_offset
    // is within the ring buffers bounds
    #[inline]
    unsafe fn get_slice_ptr(&self, cl_index: usize, cl_offset: usize) -> *const T {
        unsafe {
            self.buffer
                .inner
                .get_unchecked(cl_index)
                .get_item_ptr(cl_offset)
                .cast::<T>()
                .cast_const()
        }
    }
}

#[cfg(test)]
mod consumer_tests {
    use std::sync::atomic::Ordering;

    use crate::channel;
    use crate::consumer::Error;

    const CL_CAPACITY: usize = 8;
    const TOTAL_CAPACITY: usize = 4 * CL_CAPACITY;

    type TestMessage = usize;

    #[test]
    fn test_written_slots_one_cl() {
        let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

        let mut buf = [0usize; CL_CAPACITY];
        assert_eq!(rx.try_recv_batch(&mut buf), Err(Error::QueueEmpty));

        assert!(tx.try_send_batch(&[0; CL_CAPACITY]).is_ok());
        assert!(tx.flush().is_ok());

        assert_eq!(rx.try_recv_batch(&mut buf), Ok(CL_CAPACITY));
    }

    #[test]
    fn test_written_slots_mul_cl() {
        let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

        assert!(tx.try_send_batch(&[0; CL_CAPACITY / 2]).is_ok());
        assert!(tx.flush().is_ok());
        assert!(tx.try_send_batch(&[0; CL_CAPACITY / 2]).is_ok());
        assert!(tx.flush().is_ok());

        let mut buf = [0usize; CL_CAPACITY];
        assert_eq!(rx.try_recv_batch(&mut buf), Ok(CL_CAPACITY / 2));
        assert_eq!(rx.try_recv_batch(&mut buf), Ok(CL_CAPACITY / 2));
    }

    #[test]
    fn test_batch_stops_at_interruption() {
        let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

        assert!(tx.try_send_batch(&[0; CL_CAPACITY / 2]).is_ok());
        assert!(tx.flush().is_ok());

        assert!(tx.try_send_batch(&[0; CL_CAPACITY]).is_ok());
        assert!(tx.flush().is_ok());

        let mut buf = [0usize; 2 * CL_CAPACITY];
        let received = rx
            .try_recv_batch(&mut buf)
            .expect("batch should not be empty");
        assert_eq!(received, CL_CAPACITY / 2);
    }

    #[test]
    fn test_wrapping_is_lazy() {
        let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

        assert!(tx.try_send_batch(&[0; 2 * CL_CAPACITY]).is_ok());
        assert!(tx.flush().is_ok());

        assert!(rx.try_recv_batch(&mut [0; CL_CAPACITY]).is_ok());
        assert_eq!(rx.slot_ptr.cl_index, rx.buffer.tail.load(Ordering::Acquire));
        let cl_index = rx.slot_ptr.cl_index;

        assert_eq!(rx.slot_ptr.cl_offset, CL_CAPACITY);
        assert!(rx.try_recv().is_ok());
        assert_eq!(rx.slot_ptr.cl_index, cl_index + 1);
        assert_eq!(rx.slot_ptr.cl_index, rx.buffer.tail.load(Ordering::Acquire));
        assert_eq!(rx.slot_ptr.cl_offset, 1);
    }

    #[test]
    fn test_advance_cl_index_forward() {
        let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

        assert!(tx.try_send_batch(&[0; 2 * CL_CAPACITY]).is_ok());
        assert!(tx.flush().is_ok());

        assert!(rx.try_recv_batch(&mut [0; CL_CAPACITY]).is_ok());
        let cl_index = rx.slot_ptr.cl_index;
        assert_eq!(rx.slot_ptr.cl_offset, CL_CAPACITY);
        assert!(rx.try_recv().is_ok());
        assert_eq!(rx.slot_ptr.cl_index, cl_index + 1);
        assert_eq!(rx.slot_ptr.cl_index, 1);
    }

    #[test]
    fn test_advance_cl_index_wrapping() {
        let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

        assert!(tx.try_send_batch(&[0; 2 * CL_CAPACITY]).is_ok());
        assert!(tx.flush().is_ok());

        let cl_index = rx.slot_ptr.cl_index;
        assert!(rx.try_recv_batch(&mut [0; CL_CAPACITY]).is_ok());
        assert_ne!(rx.slot_ptr.cl_index, cl_index);
        assert!(rx.slot_ptr.cl_index < cl_index);
    }

    #[test]
    fn test_advance_cl_offset_forward() {
        let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);
        assert!(tx.try_send_batch(&[0; 2 * CL_CAPACITY]).is_ok());
        assert!(tx.flush().is_ok());

        assert!(rx.try_recv().is_ok());
        let cl_index = rx.slot_ptr.cl_index;
        let cl_offset = rx.slot_ptr.cl_offset;
        assert!(rx.try_recv().is_ok());

        assert_eq!(rx.slot_ptr.cl_index, cl_index);
        assert_eq!(rx.slot_ptr.cl_offset, cl_offset + 1);
    }

    #[test]
    fn test_advance_cl_offset_wrapping() {
        let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);
        assert!(tx.try_send_batch(&[0; 2 * CL_CAPACITY]).is_ok());
        assert!(tx.flush().is_ok());

        assert_eq!(rx.slot_ptr.cl_offset, CL_CAPACITY);
        assert!(rx.try_recv().is_ok());
        assert_eq!(rx.slot_ptr.cl_offset, 1);
        assert_ne!(rx.slot_ptr.cl_offset, CL_CAPACITY);
    }
}
