use std::sync::{Arc, atomic::AtomicUsize};

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

    // # Safety:
    //
    // the caller has to make sure that the index start_pos = (cl_index * N) + cl_offset
    // is within the ring buffers bounds
    #[inline]
    pub(crate) unsafe fn get_slice_ptr(&self, cl_index: usize, cl_offset: usize) -> *const T {
        unsafe {
            self.inner
                .get_unchecked(cl_index)
                .get_item_ptr(cl_offset)
                .cast::<T>()
                .cast_const()
        }
    }

    // # Safety:
    //
    // the caller has to make sure that the index start_pos = (cl_index * N) + cl_offset
    // is within the ring buffers bounds
    #[inline]
    pub(crate) unsafe fn get_slice_ptr_mut(&self, cl_index: usize, cl_offset: usize) -> *mut T {
        unsafe {
            self.inner
                .get_unchecked(cl_index)
                .get_item_ptr(cl_offset)
                .cast::<T>()
        }
    }

    pub(crate) unsafe fn as_slice(&self, from: SlotPtr, len: usize) -> (&[T], &[T]) {
        let (curr_cl_index, curr_cl_offset) = from.into();
        let last_abs_index = self.capacity;
        let from_abs_index = (curr_cl_index * N) + curr_cl_offset;
        let to_abs_index = from_abs_index + len;

        let slices: (&[T], &[T]) = if to_abs_index < last_abs_index {
            let s_ptr = unsafe { self.get_slice_ptr_mut(curr_cl_index, curr_cl_offset) };
            let s_slice = unsafe { core::slice::from_raw_parts(s_ptr, len) };

            (s_slice, &[])
        } else {
            let s1_len = last_abs_index - from_abs_index;
            let s1_ptr = unsafe { self.get_slice_ptr_mut(curr_cl_index, curr_cl_offset) };
            let s1_slice = unsafe { core::slice::from_raw_parts(s1_ptr, s1_len) };

            let s2_len = len - s1_len;
            let s2_ptr = unsafe { self.get_slice_ptr_mut(0, 0) };
            let s2_slice = unsafe { core::slice::from_raw_parts(s2_ptr, s2_len) };

            (s1_slice, s2_slice)
        };

        slices
    }

    pub(crate) unsafe fn as_slice_mut<'a>(
        &self,
        from: SlotPtr,
        len: usize,
    ) -> (&'a mut [T], &'a mut [T]) {
        let (curr_cl_index, curr_cl_offset) = from.into();
        let last_abs_index = self.capacity;
        let from_abs_index = (curr_cl_index * N) + curr_cl_offset;
        let to_abs_index = from_abs_index + len;

        let slices: (&mut [T], &mut [T]) = if to_abs_index < last_abs_index {
            let s_ptr = unsafe { self.get_slice_ptr_mut(curr_cl_index, curr_cl_offset) };
            let s_slice = unsafe { core::slice::from_raw_parts_mut(s_ptr, len) };

            (s_slice, &mut [])
        } else {
            let s1_len = last_abs_index - from_abs_index;
            let s1_ptr = unsafe { self.get_slice_ptr_mut(curr_cl_index, curr_cl_offset) };
            let s1_slice = unsafe { core::slice::from_raw_parts_mut(s1_ptr, s1_len) };

            let s2_len = len - s1_len;
            let s2_ptr = unsafe { self.get_slice_ptr_mut(0, 0) };
            let s2_slice = unsafe { core::slice::from_raw_parts_mut(s2_ptr, s2_len) };

            (s1_slice, s2_slice)
        };

        slices
    }
}
