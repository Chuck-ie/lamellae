use core::sync::atomic::Ordering;
use std::sync::Arc;

use crate::{SlotPtr, buffer::Buffer, spinlock::Spinlock};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    QueueEmpty,
    BatchTooLarge,
}

pub struct Consumer<T, const N: usize> {
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
            }
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

            self.refresh_cached_cl_write(curr_head);

            Ok(unsafe { self.read_into_next_cl(curr_cl_index) })
        }
        // fast path: cl crossing inside cached range
        else if curr_cl_offset == N {
            // Safety: the next cache line lies inside `cached_cl_write`, which
            // was previously verified against the producer's head.
            Ok(unsafe { self.read_into_next_cl(curr_cl_index) })
        }
        // fast path: same cl read
        else {
            let cache_line = unsafe { self.buffer.get_cache_line(curr_cl_index) };
            let value = unsafe { cache_line.read(curr_cl_offset) };
            self.slot_ptr.cl_offset = curr_cl_offset + 1;
            Ok(value)
        }
    }

    #[inline]
    fn refresh_cached_cl_write(&mut self, curr_head: usize) {
        if let Some(intr) = self.buffer.interruptions.pop_next() {
            self.cached_cl_write = intr;
        } else {
            let prev_head = curr_head.wrapping_sub(1) & self.buffer.cl_mask;
            self.cached_cl_write = SlotPtr::from((prev_head, N));
        }
    }

    #[inline]
    unsafe fn read_into_next_cl(&mut self, curr_cl_index: usize) -> T {
        let next_cl_index = (curr_cl_index + 1) & self.buffer.cl_mask;
        let next_cache_line = unsafe { self.buffer.get_cache_line(next_cl_index) };
        let value = unsafe { next_cache_line.read(0) };

        self.slot_ptr.set(next_cl_index, 1);
        self.buffer.tail.store(next_cl_index, Ordering::Release);

        value
    }

    pub fn try_recv_batch(&mut self, buf: &mut [T]) -> Result<usize, Error>
    where
        T: Copy,
    {
        let final_batch_size = self.clamp_batch_size(buf.len())?;

        // Safety: final_batch_size <= continuous which was verified readable above
        unsafe { self.recv_batch_exact_unchecked(&mut buf[..final_batch_size]) };

        Ok(final_batch_size)
    }

    pub fn try_recv_batch_exact(&mut self, buf: &mut [T]) -> Result<usize, Error>
    where
        T: Copy,
    {
        let size = self.validate_batch_size_exact(buf.len())?;

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
        let batch_size = buf.len();
        let (s1, s2) = unsafe { self.buffer.as_slice(self.slot_ptr, batch_size) };

        let s1_len = s1.len();

        unsafe {
            core::ptr::copy_nonoverlapping(s1.as_ptr(), buf.as_mut_ptr(), s1_len);
            core::ptr::copy_nonoverlapping(s2.as_ptr(), buf.as_mut_ptr().add(s1_len), s2.len());
        };

        self.buffer.advance_slot_ptr(
            &mut self.slot_ptr,
            batch_size,
            &self.buffer.tail,
            Ordering::Release,
        );

        batch_size
    }

    pub fn try_recv_with<F>(&mut self, size: usize, with: F) -> Result<usize, Error>
    where
        F: FnOnce(&[T], &[T]),
    {
        let size = self.clamp_batch_size(size)?;

        let (s1, s2) = unsafe { self.buffer.as_slice(self.slot_ptr, size) };
        with(s1, s2);

        self.buffer.advance_slot_ptr(
            &mut self.slot_ptr,
            size,
            &self.buffer.tail,
            Ordering::Release,
        );

        Ok(size)
    }

    pub fn try_recv_exact_with<F>(&mut self, size: usize, with: F) -> Result<usize, Error>
    where
        F: FnOnce(&[T], &[T]),
    {
        let size = self.validate_batch_size_exact(size)?;

        let (s1, s2) = unsafe { self.buffer.as_slice(self.slot_ptr, size) };
        with(s1, s2);

        self.buffer.advance_slot_ptr(
            &mut self.slot_ptr,
            size,
            &self.buffer.tail,
            Ordering::Release,
        );

        Ok(size)
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

        self.refresh_cached_cl_write(curr_head);

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

    fn clamp_batch_size(&mut self, requested: usize) -> Result<usize, Error> {
        crate::buffer::clamp_batch_size(
            requested,
            self.buffer.max_size(),
            self.continuous_used(),
            Error::QueueEmpty,
        )
    }

    fn validate_batch_size_exact(&mut self, requested: usize) -> Result<usize, Error> {
        crate::buffer::validate_exact_batch_size(
            requested,
            self.buffer.max_size(),
            self.continuous_used(),
            Error::QueueEmpty,
            Error::BatchTooLarge,
        )
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
