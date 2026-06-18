use std::{
    cell::UnsafeCell,
    sync::{Arc, atomic::AtomicUsize},
};

use crate::{
    consumer::Consumer,
    producer::Producer,
    wrapper::{CacheLine, CachePadded},
};

unsafe impl<T: Send, const N: usize> Send for Buffer<T, N> {}
unsafe impl<T: Sync, const N: usize> Sync for Buffer<T, N> {}

pub struct Buffer<T, const N: usize> {
    pub(crate) head: CachePadded<AtomicUsize>,
    pub(crate) tail: CachePadded<AtomicUsize>,
    pub(crate) inner: Box<[CacheLine<T, N>]>,
    pub(crate) write_counts: Box<[UnsafeCell<usize>]>,
    pub(crate) cl_mask: usize,
    pub(crate) capacity: usize,
}

impl<T, const N: usize> Buffer<T, N> {
    #[must_use]
    pub fn with_capacity(capacity: usize) -> (Producer<T, N>, Consumer<T, N>) {
        let cache_lines = capacity / N;
        let inner: Box<[CacheLine<T, N>]> =
            (0..cache_lines).map(|_| CacheLine::default()).collect();

        let write_counts: Box<[UnsafeCell<usize>]> =
            (0..cache_lines).map(|_| UnsafeCell::new(0)).collect();

        let cache_line_mask = cache_lines - 1;

        let buffer = Arc::new(Self {
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(cache_line_mask)),
            inner,
            write_counts,
            cl_mask: cache_line_mask,
            capacity,
        });

        let producer = Producer::new(&buffer);
        let consumer = Consumer::new(&buffer);

        (producer, consumer)
    }

    // # Safety: the caller has to make sure that index is within bounds of the buffer
    pub(crate) unsafe fn get_cache_line(&self, index: usize) -> &CacheLine<T, N> {
        unsafe { self.inner.get_unchecked(index) }
    }
}
