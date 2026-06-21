use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

use crate::SlotPtr;

pub struct SlotTracker<const N: usize> {
    // tail: AtomicPtr<Span>,
    // head: AtomicPtr<Span>,
    tail: UnsafeCell<*mut Span>,
    head: UnsafeCell<*mut Span>,
    cl_mask: usize,
}

impl<const N: usize> Drop for SlotTracker<N> {
    fn drop(&mut self) {
        let mut curr = unsafe { self.tail.get().read() };

        while !curr.is_null() {
            // # Safety: this pointer is allowed to be null and will get checked in the next
            // iteration of the loop, which makes this safe
            let next = unsafe { curr.read().next.load(Ordering::Relaxed) };

            // # Safety: we checked if curr is not null, so this is safe to clear up the memory
            let _ = unsafe { Box::from_raw(curr) };
            curr = next;
        }
    }
}

impl<const N: usize> SlotTracker<N> {
    const OFFSET_BITS: usize = 8;
    const OFFSET_MASK: usize = (1 << Self::OFFSET_BITS) - 1;

    #[inline]
    const fn pack(ptr: SlotPtr) -> usize {
        (ptr.cl_index << Self::OFFSET_BITS) | (ptr.cl_offset & Self::OFFSET_MASK)
    }

    #[inline]
    const fn unpack(packed: usize) -> (usize, usize) {
        let cl_index = packed >> Self::OFFSET_BITS;
        let cl_offset = packed & Self::OFFSET_MASK;
        (cl_index, cl_offset)
    }

    pub fn new(capacity: usize, tail_cl_index: usize, head_cl_index: usize) -> Self {
        let tail_packed = Self::pack(SlotPtr::from((tail_cl_index, 0)));
        let head_packed = Self::pack(SlotPtr::from((head_cl_index, 0)));
        let initial = Span::new(tail_packed, head_packed);

        Self {
            tail: UnsafeCell::new(initial),
            head: UnsafeCell::new(initial),
            cl_mask: capacity - 1,
        }
    }

    pub fn mark_used(&self, lo_ptr: SlotPtr, hi_ptr: SlotPtr) {
        let lo = Self::pack(lo_ptr);
        let hi = Self::pack(hi_ptr);

        let head = unsafe { self.head.get().read() };
        let head_hi = unsafe { (*head).hi.load(Ordering::Acquire) };

        // extend head
        if lo == head_hi {
            unsafe { (*head).hi.store(hi, Ordering::Release) };
        }
        // add new span
        else {
            std::hint::cold_path();
            let new_span = Span::new(lo, hi);
            unsafe { (*head).next.store(new_span, Ordering::Release) };
            unsafe { self.head.get().write(new_span) }
        }
    }

    pub fn mark_free(&self, hi_ptr: SlotPtr) {
        let hi = Self::pack(hi_ptr);
        let tail = unsafe { self.tail.get().read() };
        let tail_hi = unsafe { (*tail).hi.load(Ordering::Acquire) };

        unsafe { (*tail).lo.store(hi, Ordering::Release) };
        let next = unsafe { (*tail).next.load(Ordering::Acquire) };

        // remove fully read spans if theres more than one present
        if hi == tail_hi && !next.is_null() {
            let _ = unsafe { Box::from_raw(tail) };
            unsafe { self.tail.get().write(next) }
        }
    }

    pub fn next_used(&self, r_ptr: SlotPtr) -> usize {
        let tail = unsafe { self.tail.get().read() };
        let tail_hi = unsafe { (*tail).hi.load(Ordering::Acquire) };
        let (r_cl_index, r_cl_offset) = r_ptr.into();
        let (w_cl_index, w_cl_offset) = Self::unpack(tail_hi);

        // with: cachelines = 4, N = 8, self.capacity = 32
        //
        // ex1(init state): r_cl=(3,8), w_cl=(0,0)
        // - flat reader = (3 * 8 + 8) & 31 = 0
        // - flat writer = (0 * 8 + 0) & 31 = 0
        // - 0.wrapping_sub(0) & 31 = 0
        //
        // ex2(mid state):  r_cl=(3,4), w_cl=(1,2)
        // - flat reader = (3 * 8 + 4) & 31 = 28
        // - flat writer = (1 * 8 + 2) & 31 = 10
        // - 10.wrapping_sub(28) & 31 = 14
        let flat_w = (w_cl_index * N + w_cl_offset) & self.cl_mask;
        let flat_r = (r_cl_index * N + r_cl_offset) & self.cl_mask;
        flat_w.wrapping_sub(flat_r) & self.cl_mask
    }

    pub fn next_free(&self, w_ptr: SlotPtr) -> usize {
        let tail = unsafe { self.tail.get().read() };
        let tail_lo = unsafe { (*tail).lo.load(Ordering::Acquire) };
        let (r_cl_index, r_cl_offset) = Self::unpack(tail_lo);
        let (w_cl_index, w_cl_offset) = w_ptr.into();

        // with: cachelines = 4, N = 8, self.capacity = 32
        //
        // ex1(init state): r_cl=(3,0), w_cl=(0,0)
        // - Flat Writer = (0 * 8 + 0) & 31 = 0
        // - Flat Reader = (3 * 8 + 0) & 31 = 24
        // - 24.wrapping_sub(0) & 31 = 24
        //
        // ex2(mid state):  r_cl=(3,4), w_cl=(1,2)
        // - Flat Writer = (1 * 8 + 2) & 31 = 10
        // - Flat Reader = (3 * 8 + 4) & 31 = 28
        // - 28.wrapping_sub(10) & 31 = 18
        let flat_w = (w_cl_index * N + w_cl_offset) & self.cl_mask;
        let flat_r = (r_cl_index * N + r_cl_offset) & self.cl_mask;
        flat_r.wrapping_sub(flat_w) & self.cl_mask
    }

    pub fn occupied_in_cl(&self, cl_index: usize) -> usize {
        let mut curr = unsafe { self.tail.get().read() };
        let cl_start = cl_index * N;

        while !curr.is_null() {
            let tail_lo = unsafe { (*curr).lo.load(Ordering::Relaxed) };
            let tail_hi = unsafe { (*curr).hi.load(Ordering::Relaxed) };

            let (r_cl_index, r_cl_offset) = Self::unpack(tail_lo);
            let (w_cl_index, w_cl_offset) = Self::unpack(tail_hi);

            let flat_r = r_cl_index * N + r_cl_offset;
            let flat_w = w_cl_index * N + w_cl_offset;

            let span_len = flat_w.wrapping_sub(flat_r) & self.cl_mask;
            let dist_to_cl = cl_start.wrapping_sub(flat_r) & self.cl_mask;

            if dist_to_cl < span_len {
                let remaining = flat_w.wrapping_sub(cl_start) & self.cl_mask;
                return std::cmp::min(remaining, N);
            }

            curr = unsafe { (*curr).next.load(Ordering::Relaxed) };
        }

        0
    }

    pub fn spans(&self) -> usize {
        let mut curr = unsafe { self.tail.get().read() };
        let mut spans = 0;

        while !curr.is_null() {
            spans += 1;

            // # Safety: this pointer is allowed to be null and will get checked in the next
            // iteration of the loop, which makes this safe
            curr = unsafe { curr.read().next.load(Ordering::Relaxed) };
        }

        spans
    }

    pub fn len(&self) -> usize {
        let mut curr = unsafe { self.tail.get().read() };
        let mut len = 0;

        while !curr.is_null() {
            len += unsafe { (*curr).len::<N>(self.cl_mask) };

            // # Safety: this pointer is allowed to be null and will get checked in the next
            // iteration of the loop, which makes this safe
            curr = unsafe { curr.read().next.load(Ordering::Relaxed) };
        }

        len.saturating_sub(N)
    }
}

struct Span {
    lo: AtomicUsize,
    hi: AtomicUsize,
    next: AtomicPtr<Self>,
}

impl Span {
    pub fn new(lo: usize, hi: usize) -> *mut Self {
        Box::into_raw(Box::new(Self {
            lo: AtomicUsize::new(lo),
            hi: AtomicUsize::new(hi),
            next: AtomicPtr::new(core::ptr::null_mut()),
        }))
    }

    pub fn len<const N: usize>(&self, slot_mask: usize) -> usize {
        debug_assert!(
            (slot_mask + 1).is_power_of_two(),
            "capacity isnt a power of 2"
        );

        let lo = self.lo.load(Ordering::Relaxed);
        let hi = self.hi.load(Ordering::Relaxed);
        let (lo_cl_index, lo_cl_offset) = SlotTracker::<N>::unpack(lo);
        let (hi_cl_index, hi_cl_offset) = SlotTracker::<N>::unpack(hi);
        let flat_lo = (lo_cl_index * N + lo_cl_offset) & slot_mask;
        let flat_hi = (hi_cl_index * N + hi_cl_offset) & slot_mask;

        flat_hi.wrapping_sub(flat_lo) & slot_mask
    }
}

// #[cfg(test)]
// mod slot_tracker_tests {
//     use std::sync::atomic::Ordering;
//
//     use crate::slot_tracker::SlotTracker;
//
//     #[test]
//     fn test_init() {
//         let st = SlotTracker::<8>::new(16, 0, 0);
//
//         let head = st.head.load(Ordering::Relaxed);
//         let tail = st.tail.load(Ordering::Relaxed);
//         assert_eq!(head, tail);
//
//         let (lo, hi) = unsafe {
//             (
//                 (*head).lo.load(Ordering::Relaxed),
//                 (*head).hi.load(Ordering::Relaxed),
//             )
//         };
//         assert_eq!(lo, hi);
//     }
//
//     #[test]
//     fn test_mark_occupied_extend() {
//         let st = SlotTracker::<8>::new(16, 0, 0);
//
//         let lo = 0;
//         let hi = 4;
//         st.mark_occupied(lo, hi);
//         assert_eq!(st.spans(), 1);
//
//         let head = st.head.load(Ordering::Relaxed);
//         let tail = st.tail.load(Ordering::Relaxed);
//         assert_eq!(head, tail);
//
//         let (new_lo, new_hi) = unsafe {
//             (
//                 (*head).lo.load(Ordering::Relaxed),
//                 (*head).hi.load(Ordering::Relaxed),
//             )
//         };
//         assert_eq!(lo, new_lo);
//         assert_eq!(hi, new_hi);
//     }
//
//     #[test]
//     fn test_mark_occupied_split() {
//         let st = SlotTracker::<8>::new(16, 0, 0);
//
//         let lo_1 = 0;
//         let hi_1 = 4;
//         st.mark_occupied(lo_1, hi_1);
//         assert_eq!(st.spans(), 1);
//
//         let lo_2 = hi_1 + 2;
//         let hi_2 = hi_1 + 4;
//         st.mark_occupied(lo_2, hi_2);
//         assert_eq!(st.spans(), 2);
//
//         let head = st.head.load(Ordering::Relaxed);
//         let tail = st.tail.load(Ordering::Relaxed);
//         assert_ne!(head, tail);
//
//         let (tail_lo, tail_hi) = unsafe {
//             (
//                 (*tail).lo.load(Ordering::Relaxed),
//                 (*tail).hi.load(Ordering::Relaxed),
//             )
//         };
//         assert_eq!(lo_1, tail_lo);
//         assert_eq!(hi_1, tail_hi);
//
//         let (head_lo, head_hi) = unsafe {
//             (
//                 (*head).lo.load(Ordering::Relaxed),
//                 (*head).hi.load(Ordering::Relaxed),
//             )
//         };
//         assert_eq!(lo_2, head_lo);
//         assert_eq!(hi_2, head_hi);
//     }
//
//     #[test]
//     fn test_mark_free_partial() {
//         let st = SlotTracker::<8>::new(16, 0, 0);
//         st.mark_occupied(0, 4);
//
//         st.mark_free(2);
//         assert_eq!(st.spans(), 1);
//
//         let tail = st.tail.load(Ordering::Relaxed);
//         let (tail_lo, tail_hi) = unsafe {
//             (
//                 (*tail).lo.load(Ordering::Relaxed),
//                 (*tail).hi.load(Ordering::Relaxed),
//             )
//         };
//         assert_eq!(tail_lo, 2);
//         assert_eq!(tail_hi, 4);
//     }
//
//     #[test]
//     fn test_mark_free_complete_one_span() {
//         let st = SlotTracker::<8>::new(16, 0, 0);
//         st.mark_occupied(0, 4);
//
//         st.mark_free(4);
//         assert_eq!(st.spans(), 1);
//
//         let tail = st.tail.load(Ordering::Relaxed);
//         let (tail_lo, tail_hi) = unsafe {
//             (
//                 (*tail).lo.load(Ordering::Relaxed),
//                 (*tail).hi.load(Ordering::Relaxed),
//             )
//         };
//         assert_eq!(tail_lo, 4);
//         assert_eq!(tail_hi, 4);
//     }
//
//     #[test]
//     fn test_mark_free_advances_tail() {
//         let st = SlotTracker::<8>::new(16, 0, 0);
//         st.mark_occupied(0, 4);
//         st.mark_occupied(6, 10);
//         assert_eq!(st.spans(), 2);
//
//         st.mark_free(4);
//         assert_eq!(st.spans(), 1);
//
//         let tail = st.tail.load(Ordering::Relaxed);
//         let head = st.head.load(Ordering::Relaxed);
//         assert_eq!(tail, head);
//
//         let (tail_lo, tail_hi) = unsafe {
//             (
//                 (*tail).lo.load(Ordering::Relaxed),
//                 (*tail).hi.load(Ordering::Relaxed),
//             )
//         };
//         assert_eq!(tail_lo, 6);
//         assert_eq!(tail_hi, 10);
//     }
//
//     // [ 0, 1 | 2, 3 | 4, 5 | 6, 7 ]
//     //1: ^rw                        -> [Span(0,0)]
//     //2: ^r     ^w                  -> [Span(0,2)]
//     //3:    ^r  ^w                  -> [Span(1,2)]
//     //4:    ^r         ^w           -> [Span(1,2), Span(4,4)] (flushed)
//     //5:        ^r     ^w           -> [Span(4,4)]
//     //TODO: add wrapping examples
//     #[test]
//     fn test_sequence_example() {
//         let st = SlotTracker::<8>::new(16, 0, 0);
//
//         st.mark_occupied(0, 2);
//         let tail = st.tail.load(Ordering::Relaxed);
//         let (lo, hi) = unsafe {
//             (
//                 (*tail).lo.load(Ordering::Relaxed),
//                 (*tail).hi.load(Ordering::Relaxed),
//             )
//         };
//         assert_eq!(lo, 0);
//         assert_eq!(hi, 2);
//
//         st.mark_free(1);
//         let (lo, hi) = unsafe {
//             (
//                 (*tail).lo.load(Ordering::Relaxed),
//                 (*tail).hi.load(Ordering::Relaxed),
//             )
//         };
//         assert_eq!(lo, 1);
//         assert_eq!(hi, 2);
//
//         st.mark_occupied(4, 4);
//         assert_eq!(st.spans(), 2);
//
//         st.mark_free(2);
//         assert_eq!(st.spans(), 1);
//
//         let tail = st.tail.load(Ordering::Relaxed);
//         let (lo, hi) = unsafe {
//             (
//                 (*tail).lo.load(Ordering::Relaxed),
//                 (*tail).hi.load(Ordering::Relaxed),
//             )
//         };
//         assert_eq!(lo, 4);
//         assert_eq!(hi, 4);
//     }
// }
