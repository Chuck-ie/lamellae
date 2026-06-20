use std::sync::{Arc, atomic::AtomicUsize};

use crate::{
    consumer::Consumer,
    producer::Producer,
    slot_tracker::SlotTracker,
    wrapper::{CacheLine, CachePadded},
};

unsafe impl<T: Send, const N: usize> Send for Buffer<T, N> {}
unsafe impl<T: Sync, const N: usize> Sync for Buffer<T, N> {}

// TODO: replace write_counts with pairs of contigious
pub struct Buffer<T, const N: usize> {
    pub(crate) head: CachePadded<AtomicUsize>,
    pub(crate) tail: CachePadded<AtomicUsize>,
    pub(crate) inner: Box<[CacheLine<T, N>]>,
    slot_tracker: SlotTracker<N>,
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
            slot_tracker: SlotTracker::new(capacity, cache_line_mask * N, 0),
            cl_mask: cache_line_mask,
            capacity,
        });

        let producer = Producer::new(&buffer);
        let consumer = Consumer::new(&buffer);

        (producer, consumer)
    }

    // # Safety: the caller has to make sure that index is within bounds of the buffer
    #[inline]
    pub(crate) unsafe fn get_cache_line(&self, index: usize) -> &CacheLine<T, N> {
        unsafe { self.inner.get_unchecked(index) }
    }

    #[inline]
    pub(crate) fn occupied_in_cl(&self, cl_index: usize) -> usize {
        self.slot_tracker.occupied_in_cl(cl_index)
    }

    #[inline]
    pub(crate) fn mark_occupied(&self, cl_index: usize, cl_offset: usize) {
        let lo_idx = cl_index * N;
        let hi_idx = self.flatten_index(cl_index, cl_offset);

        self.slot_tracker.mark_occupied(lo_idx, hi_idx);
    }

    #[inline]
    pub(crate) fn mark_free(&self, cl_index: usize, cl_offset: usize) {
        let hi_idx = self.flatten_index(cl_index, cl_offset);

        println!("buffer mark_free: {hi_idx}");
        self.slot_tracker.mark_free(hi_idx);
    }

    #[inline]
    pub(crate) fn continuous_occupied(&self, cl_index: usize, cl_offset: usize) -> usize {
        let flat_idx = self.flatten_index(cl_index, cl_offset);

        self.slot_tracker.continuous_occupied(flat_idx)
    }

    #[inline]
    pub(crate) fn continuous_free(&self, cl_index: usize, cl_offset: usize) -> usize {
        let flat_idx = self.flatten_index(cl_index, cl_offset);

        self.slot_tracker.continuous_free(flat_idx)
    }

    #[inline]
    const fn flatten_index(&self, cl_index: usize, cl_offset: usize) -> usize {
        (cl_index * N + cl_offset) & (self.capacity - 1)
    }
}
