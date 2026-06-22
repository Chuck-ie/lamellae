use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::{
    SlotPtr,
    consumer::Consumer,
    interruption_tracker::InterruptionTracker,
    producer::Producer,
    wrapper::{CacheLine, CachePadded},
};

unsafe impl<T: Send, const N: usize> Send for Buffer<T, N> {}
unsafe impl<T: Sync, const N: usize> Sync for Buffer<T, N> {}

pub struct Buffer<T, const N: usize> {
    pub(crate) head: CachePadded<AtomicUsize>,
    pub(crate) tail: CachePadded<AtomicUsize>,
    pub(crate) inner: Box<[CacheLine<T, N>]>,
    pub(crate) interruptions: InterruptionTracker,
    pub(crate) cl_mask: usize,
    pub(crate) capacity: usize,
}

impl<T, const N: usize> Buffer<T, N> {
    #[must_use]
    pub fn with_capacity(capacity: usize) -> (Producer<T, N>, Consumer<T, N>) {
        let cache_lines = capacity / N;
        let inner: Box<[CacheLine<T, N>]> =
            (0..cache_lines).map(|_| CacheLine::default()).collect();

        let cache_line_mask = cache_lines - 1;

        let buffer = Arc::new(Self {
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(cache_line_mask)),
            inner,
            interruptions: InterruptionTracker::new(),
            cl_mask: cache_line_mask,
            capacity,
        });

        let producer = Producer::new(&buffer);
        let consumer = Consumer::new(&buffer);

        (producer, consumer)
    }

    // Safety: the caller has to make sure that index is within bounds of the buffer
    #[inline]
    pub(crate) unsafe fn get_cache_line(&self, index: usize) -> &CacheLine<T, N> {
        unsafe { self.inner.get_unchecked(index) }
    }

    #[inline]
    pub(crate) const fn flat_dist(&self, lo: SlotPtr, hi: SlotPtr) -> usize {
        let slot_mask = self.capacity - 1;
        let flat_lo = (lo.cl_index * N + lo.cl_offset) & slot_mask;
        let flat_hi = (hi.cl_index * N + hi.cl_offset) & slot_mask;
        flat_hi.wrapping_sub(flat_lo) & slot_mask
    }

    #[inline]
    pub(crate) const fn max_size(&self) -> usize {
        self.capacity - N
    }

    #[inline]
    pub(crate) fn advance_slot_ptr(
        &self,
        slot_ptr: &mut SlotPtr,
        by: usize,
        counter: &AtomicUsize,
        ordering: Ordering,
    ) {
        let (curr_cl_index, curr_cl_offset) = (*slot_ptr).into();
        let from_abs_index = (curr_cl_index * N) + curr_cl_offset;
        let to_abs_index = from_abs_index + by;
        let final_abs_index = to_abs_index % self.capacity;

        let cl_index = (final_abs_index / N) & self.cl_mask;
        let cl_offset = final_abs_index % N;

        let (next_cl_index, next_cl_offset) = if cl_offset == 0 && to_abs_index > 0 {
            (cl_index.wrapping_sub(1) & self.cl_mask, N)
        } else {
            (cl_index, cl_offset)
        };

        slot_ptr.set(next_cl_index, next_cl_offset);
        counter.store(next_cl_index, ordering);
    }

    pub(crate) unsafe fn as_slice(&self, from: SlotPtr, len: usize) -> (&[T], &[T]) {
        let (s1_ptr, s1_len, s2_ptr, s2_len) = unsafe { self.slice_ptr_pair(from, len) };

        unsafe {
            (
                core::slice::from_raw_parts(s1_ptr, s1_len),
                core::slice::from_raw_parts(s2_ptr, s2_len),
            )
        }
    }

    pub(crate) unsafe fn as_slice_mut<'a>(
        &self,
        from: SlotPtr,
        len: usize,
    ) -> (&'a mut [T], &'a mut [T]) {
        let (s1_ptr, s1_len, s2_ptr, s2_len) = unsafe { self.slice_ptr_pair(from, len) };

        unsafe {
            (
                core::slice::from_raw_parts_mut(s1_ptr, s1_len),
                core::slice::from_raw_parts_mut(s2_ptr, s2_len),
            )
        }
    }

    #[inline]
    unsafe fn slice_ptr_pair(&self, from: SlotPtr, len: usize) -> (*mut T, usize, *mut T, usize) {
        let (curr_cl_index, curr_cl_offset) = from.into();
        let from_abs_index = (curr_cl_index * N) + curr_cl_offset;
        let to_abs_index = from_abs_index + len;

        let s1_ptr = unsafe { self.get_slice_ptr_mut(curr_cl_index, curr_cl_offset) };
        let s2_ptr = unsafe { self.get_slice_ptr_mut(0, 0) };

        if to_abs_index <= self.capacity {
            (s1_ptr, len, s2_ptr, 0)
        } else {
            let s1_len = self.capacity - from_abs_index;
            let s2_len = len - s1_len;
            (s1_ptr, s1_len, s2_ptr, s2_len)
        }
    }

    // Safety:
    //
    // the caller has to make sure that the index start_pos = (cl_index * N) + cl_offset
    // is within the ring buffers bounds
    #[inline]
    unsafe fn get_slice_ptr_mut(&self, cl_index: usize, cl_offset: usize) -> *mut T {
        unsafe {
            self.inner
                .get_unchecked(cl_index)
                .get_slot_ptr(cl_offset)
                .cast::<T>()
        }
    }
}

#[inline]
pub(crate) fn clamp_batch_size<E>(
    requested: usize,
    max_size: usize,
    available: usize,
    empty_err: E,
) -> Result<usize, E> {
    match requested.min(max_size).min(available) {
        0 => Err(empty_err),
        size => Ok(size),
    }
}

#[inline]
pub(crate) fn validate_exact_batch_size<E>(
    requested: usize,
    max_size: usize,
    available: usize,
    empty_err: E,
    too_large_err: E,
) -> Result<usize, E> {
    if requested > max_size {
        Err(too_large_err)
    } else if requested > available {
        Err(empty_err)
    } else {
        Ok(requested)
    }
}
