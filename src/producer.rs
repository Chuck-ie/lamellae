use std::sync::{Arc, atomic::Ordering};

use crate::{buffer::Buffer, reservation::SendReservation, spinlock::Spinlock};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    QueueFull,
    BatchTooLarge,
}

pub struct Producer<T, const N: usize> {
    pub(crate) buffer: Arc<Buffer<T, N>>,
    pub(crate) cl_index: usize,
    pub(crate) cl_offset: usize,
}

impl<T, const N: usize> Producer<T, N> {
    pub(crate) fn new(buffer: &Arc<Buffer<T, N>>) -> Self {
        Self {
            cl_index: 0,
            cl_offset: 0,
            buffer: buffer.clone(),
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
        let curr_head = self.cl_index;
        let curr_cl_head = self.cl_offset;

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

            // Safety: curr_head is exclusively owned by the writer and is within bounds
            unsafe {
                self.buffer
                    .write_counts
                    .get_unchecked(curr_head)
                    .get()
                    .write(N);
            };

            // Safety: next_head is verified to not overlap with curr_tail and is within bounds
            let next_cache_line = unsafe { self.buffer.get_cache_line(next_head) };
            unsafe { next_cache_line.write(0, value) };

            self.cl_index = next_head;
            self.cl_offset = 1;

            // Sync the advancement with the read thread
            self.buffer.head.store(next_head, Ordering::Release);
        }
        // fast path for the currently exclusively owned cache line
        else {
            // Safety: curr_head is always within bounds and never overlaps with the read head
            let cache_line = unsafe { self.buffer.get_cache_line(curr_head) };
            unsafe { cache_line.write(curr_cl_head, value) };

            self.cl_offset = curr_cl_head + 1;
        }

        Ok(())
    }

    pub fn try_send_batch(&mut self, buf: &[T]) -> Result<usize, Error>
    where
        T: Copy,
    {
        let max_batch_size = self.buffer.capacity - N;
        let batch_size = buf.len().min(max_batch_size);
        let final_batch_size = batch_size.min(self.free_slots());

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

        if batch_size > max_batch_size || batch_size > self.free_slots() {
            return Err(Error::BatchTooLarge);
        }

        Ok(unsafe { self.send_batch_exact_unchecked(buf) })
    }

    // # Safety: The caller has to make sure to validate that there are buf.len()
    // items free to write to the buffer
    unsafe fn send_batch_exact_unchecked(&mut self, buf: &[T]) -> usize
    where
        T: Copy,
    {
        let batch_size = buf.len();
        let curr_cl_index = self.cl_index;
        let curr_cl_offset = self.cl_offset;
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
        let next_cl_index = (final_abs_index / N) & self.buffer.cl_mask;
        let next_cl_offset = final_abs_index % N;

        self.cl_index = next_cl_index;
        self.cl_offset = next_cl_offset;

        let mut i = curr_cl_index;

        while i != next_cl_index {
            unsafe { self.buffer.write_counts.get_unchecked(i).get().write(N) }
            i = (i + 1) & self.buffer.cl_mask;
        }

        self.buffer.head.store(next_cl_index, Ordering::Release);

        batch_size
    }

    pub fn try_reserve(&mut self, size: usize) -> Result<SendReservation<'_, T, N>, Error>
    where
        T: Copy,
    {
        let max_batch_size = self.buffer.capacity - N;
        let reservation_size = size.min(max_batch_size).min(self.free_slots());

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

        if size > max_batch_size || size > self.free_slots() {
            return Err(Error::BatchTooLarge);
        }

        Ok(unsafe { self.reserve_exact_unchecked(size) })
    }

    unsafe fn reserve_exact_unchecked(&mut self, size: usize) -> SendReservation<'_, T, N>
    where
        T: Copy,
    {
        let curr_cl_index = self.cl_index;
        let curr_cl_offset = self.cl_offset;
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
        let curr_cl_index = self.cl_index;
        let next_head = (curr_cl_index + 1) & self.buffer.cl_mask;

        // Sync with the reader's release when advancing its tail
        let curr_tail = self.buffer.tail.load(Ordering::Acquire);

        if next_head == curr_tail {
            return Err(Error::QueueFull);
        }

        self.cl_index = next_head;
        self.cl_offset = 0;

        // case(curr_cl_head == 0): means 0 has not yet been written
        // case(curr_cl_head == 1): means 1 has not yet been written
        // case(curr_cl_head == N): means N has not yet been written
        // Safety: curr_head is exclusively owned by the writer and is within bounds
        unsafe {
            self.buffer
                .write_counts
                .get_unchecked(curr_cl_index)
                .get()
                .write(curr_cl_index);
        }

        self.buffer.head.store(next_head, Ordering::Release);

        Ok(())
    }

    #[inline]
    fn free_slots(&self) -> usize {
        // [CL0, CL1, CL2, CL3]
        // writer: cl_index=2, cl_offset=N
        // reader: cl_index=3, cl_offset=N
        // -> free_cache_lines = (3 - 2) & mask -> 1
        let head_cl_index = self.cl_index;
        let head_cl_offset = self.cl_offset;
        let tail_cl_index = self.buffer.tail.load(Ordering::Acquire);

        let free_cache_lines =
            tail_cl_index.wrapping_sub(head_cl_index).wrapping_sub(1) & self.buffer.cl_mask;

        let free_slots_total = free_cache_lines * N;
        let free_slots_curr_cl = N - head_cl_offset;

        free_slots_total + free_slots_curr_cl
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
