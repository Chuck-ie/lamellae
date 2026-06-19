use std::{
    ptr,
    sync::{Arc, atomic::Ordering},
};

use crate::{buffer::Buffer, reservation::RecvReservation, spinlock::Spinlock};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    QueueEmpty,
    BatchTooLarge,
}

pub struct Consumer<T, const N: usize, const CLS: usize = 0> {
    pub(crate) buffer: Arc<Buffer<T, N>>,
    pub(crate) cl_index: usize,
    pub(crate) cl_offset: usize,
    cl_write_count: usize,
}

impl<T, const N: usize> Consumer<T, N> {
    pub(crate) fn new(buffer: &Arc<Buffer<T, N>>) -> Self {
        Self {
            cl_index: buffer.cl_mask,
            cl_offset: N,
            buffer: buffer.clone(),
            cl_write_count: N,
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
        let curr_tail = self.cl_index;
        let curr_cl_tail = self.cl_offset;
        let curr_cl_write_count = self.cl_write_count;

        // If we finished reading from a cache line in the previous recv
        if curr_cl_tail == curr_cl_write_count {
            // Calculate the index of the next cache line by wrapping around buffer bounds using
            // fast modulo since cache_lines is always a power of 2
            let next_tail = (curr_tail + 1) & self.buffer.cl_mask;

            // Sync with the writer's release when advancing its head
            let curr_head = self.buffer.head.load(Ordering::Acquire);

            if next_tail == curr_head {
                return Err(Error::QueueEmpty);
            }

            // Safety: curr_tail is verified against curr_head and is within bounds
            unsafe {
                self.buffer
                    .write_counts
                    .get_unchecked(curr_tail)
                    .get()
                    .write(0);
            }

            // Safety: next_tail is verified against curr_head and is within bounds
            let next_cache_line = unsafe { self.buffer.get_cache_line(next_tail) };
            let value = unsafe { next_cache_line.read(0) };

            let next_write_count = unsafe {
                self.buffer
                    .write_counts
                    .get_unchecked(next_tail)
                    .get()
                    .read()
            };

            self.cl_write_count = next_write_count;
            self.cl_index = next_tail;
            self.cl_offset = 1;

            // Sync the advancement with the write thread
            self.buffer.tail.store(next_tail, Ordering::Release);

            Ok(value)
        } else {
            // Safety: curr_tail is always within bounds and guaranteed not to
            // reach the write head because we checked for empty state previously.
            let cache_line = unsafe { self.buffer.get_cache_line(curr_tail) };
            let value = unsafe { cache_line.read(curr_cl_tail) };
            self.cl_offset = curr_cl_tail + 1;

            Ok(value)
        }
    }

    pub fn try_recv_batch(&mut self, buf: &mut [T]) -> Result<usize, Error>
    where
        T: Copy,
    {
        let max_batch_size = self.buffer.capacity - N;
        let batch_size = buf.len().min(max_batch_size);
        let final_batch_size = batch_size.min(self.written_slots());

        if final_batch_size == 0 {
            return Err(Error::QueueEmpty);
        }

        Ok(unsafe { self.recv_batch_exact_unchecked(&mut buf[0..final_batch_size]) })
    }

    pub fn try_recv_batch_exact(&mut self, buf: &mut [T]) -> Result<usize, Error>
    where
        T: Copy,
    {
        let batch_size = buf.len();
        let max_batch_size = self.buffer.capacity - N;

        if batch_size > max_batch_size || batch_size > self.written_slots() {
            return Err(Error::BatchTooLarge);
        }

        Ok(unsafe { self.recv_batch_exact_unchecked(buf) })
    }

    // # Safety: The caller has to make sure to validate that there are buf.len()
    // items available to read inside the buffer
    unsafe fn recv_batch_exact_unchecked(&mut self, buf: &mut [T]) -> usize
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
            unsafe { ptr::copy_nonoverlapping(s_ptr, buf.as_mut_ptr(), batch_size) };
        } else {
            let s1_len = last_abs_index - from_abs_index;
            let s1_ptr = unsafe { self.get_slice_ptr(curr_cl_index, curr_cl_offset) };
            unsafe { ptr::copy_nonoverlapping(s1_ptr, buf.as_mut_ptr(), s1_len) };

            let s2_len = batch_size - s1_len;
            let s2_ptr = unsafe { self.get_slice_ptr(0, 0) };
            unsafe { ptr::copy_nonoverlapping(s2_ptr, buf.as_mut_ptr().add(s1_len), s2_len) };
        }

        let final_abs_index = to_abs_index % self.buffer.capacity;
        let next_cl_index = (final_abs_index / N) & self.buffer.cl_mask;
        let next_cl_offset = final_abs_index % N;

        self.cl_index = next_cl_index;
        self.cl_offset = next_cl_offset;

        let mut i = curr_cl_index;

        while i != next_cl_index {
            unsafe { self.buffer.write_counts.get_unchecked(i).get().write(0) }
            i = (i + 1) & self.buffer.cl_mask;
        }

        self.buffer.tail.store(next_cl_index, Ordering::Release);

        batch_size
    }

    pub fn try_reserve(&mut self, size: usize) -> Result<RecvReservation<'_, T, N>, Error>
    where
        T: Copy,
    {
        let max_batch_size = self.buffer.capacity - N;
        let reservation_size = size.min(max_batch_size).min(self.written_slots());

        if reservation_size == 0 {
            return Err(Error::QueueEmpty);
        }

        Ok(unsafe { self.reserve_exact_unchecked(reservation_size) })
    }

    pub fn try_reserve_exact(&mut self, size: usize) -> Result<RecvReservation<'_, T, N>, Error>
    where
        T: Copy,
    {
        let max_batch_size = self.buffer.capacity - N;

        if size > max_batch_size || size > self.written_slots() {
            return Err(Error::BatchTooLarge);
        }

        Ok(unsafe { self.reserve_exact_unchecked(size) })
    }

    unsafe fn reserve_exact_unchecked(&mut self, size: usize) -> RecvReservation<'_, T, N>
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
            (s_ptr.cast::<T>(), size, std::ptr::null(), 0)
        } else {
            let s1_len = last_abs_index - from_abs_index;
            let s1_ptr = unsafe { self.get_slice_ptr(curr_cl_index, curr_cl_offset) };
            let s2_len = size - s1_len;
            let s2_ptr = unsafe { self.get_slice_ptr(0, 0) };

            (s1_ptr.cast::<T>(), s1_len, s2_ptr.cast::<T>(), s2_len)
        };

        RecvReservation {
            rx: self,
            s1,
            s1_remaining,
            s2,
            s2_remaining,
            total_reserved: size,
            start_cl_index: curr_cl_index,
            start_cl_offset: curr_cl_offset,
        }
    }

    #[inline]
    fn written_slots(&self) -> usize {
        // let tail_cl_index = self.cl_index;
        // let tail_cl_offset = self.cl_offset;
        // let head_cl_index = self.buffer.head.load(Ordering::Acquire);
        //
        // let next_tail_index = tail_cl_index.wrapping_add(1);
        //
        // let full_written_cache_lines =
        //     head_cl_index.wrapping_sub(next_tail_index) & self.buffer.cl_mask;
        //
        // let written_items_total = full_written_cache_lines * N;
        // let written_items_curr_cl = tail_cl_offset;
        //
        // written_items_total + written_items_curr_cl

        // [CL0, CL1, CL2, CL3]
        // writer: cl_index=2, cl_offset=N
        // reader: cl_index=3, cl_offset=N
        let tail_cl_index = self.cl_index;
        let tail_cl_offset = self.cl_offset;
        let head_cl_index = self.buffer.head.load(Ordering::Acquire);

        let written_items_curr_cl = unsafe { self.buffer.get_write_count(tail_cl_index) };

        let mut written_slots_total = written_items_curr_cl.saturating_sub(tail_cl_offset);
        let mut i = (tail_cl_index + 1) & self.buffer.cl_mask;

        while i != head_cl_index {
            written_slots_total += unsafe { self.buffer.get_write_count(i) };

            i = (i + 1) & self.buffer.cl_mask;
        }

        written_slots_total
    }

    // # Safety: the caller has to make sure that the index start_pos = (cl_index * N) + cl_offset
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
