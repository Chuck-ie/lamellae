use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

pub struct SlotTracker<const N: usize> {
    head: AtomicPtr<Span>,
    tail: AtomicPtr<Span>,
    capacity: usize,
}

impl<const N: usize> Drop for SlotTracker<N> {
    fn drop(&mut self) {
        let mut curr = self.tail.load(Ordering::Relaxed);

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
    pub fn new(capacity: usize, tail: usize, head: usize) -> Self {
        let initial = Span::new(tail, head);

        Self {
            head: AtomicPtr::new(initial),
            tail: AtomicPtr::new(initial),
            capacity,
        }
    }

    pub fn mark_occupied(&self, lo: usize, hi: usize) {
        let head = self.head.load(Ordering::Relaxed);
        let head_hi = unsafe { (*head).hi.load(Ordering::Relaxed) };

        // extend head
        if lo == head_hi {
            unsafe { (*head).hi.store(hi, Ordering::Release) };
        }
        // add new span
        else {
            let new_span = Span::new(lo, hi);
            unsafe { (*head).next.store(new_span, Ordering::Release) };
            self.head.store(new_span, Ordering::Release);
        }
    }

    pub fn mark_free(&self, hi: usize) {
        let tail = self.tail.load(Ordering::Relaxed);
        let tail_hi = unsafe { (*tail).hi.load(Ordering::Acquire) };

        unsafe { (*tail).lo.store(hi, Ordering::Release) };

        let next = unsafe { (*tail).next.load(Ordering::Acquire) };

        println!("slot_tracker mark_free: {tail_hi}");

        // remove fully read spans if theres more than one present
        if hi == tail_hi && !next.is_null() {
            let _ = unsafe { Box::from_raw(tail) };
            self.tail.store(next, Ordering::Release);
        }
    }

    pub fn spans(&self) -> usize {
        let mut curr = self.tail.load(Ordering::Relaxed);
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
        let mut curr = self.tail.load(Ordering::Relaxed);
        let mut len = 0;

        while !curr.is_null() {
            // # Safety: this pointer is allowed to be null and will get checked in the next
            // iteration of the loop, which makes this safe
            len += unsafe { (*curr).len(self.capacity) };

            curr = unsafe { curr.read().next.load(Ordering::Relaxed) };
        }

        len
    }

    pub fn occupied_in_cl(&self, cl_index: usize) -> usize {
        let mut curr = self.tail.load(Ordering::Relaxed);
        let lo = cl_index * N;

        while !curr.is_null() {
            let (curr_lo, curr_hi) = unsafe {
                (
                    (*curr).lo.load(Ordering::Relaxed),
                    (*curr).hi.load(Ordering::Relaxed),
                )
            };

            if lo >= curr_lo && lo <= curr_hi {
                return curr_hi.wrapping_sub(lo) & (self.capacity - 1);
            }

            curr = unsafe { (*curr).next.load(Ordering::Relaxed) };
        }

        0
    }

    pub fn continuous_occupied(&self, flat_idx: usize) -> usize {
        let tail = self.tail.load(Ordering::Relaxed);
        let hi = unsafe { (*tail).hi.load(Ordering::Relaxed) };

        if hi >= flat_idx {
            hi.wrapping_sub(flat_idx)
        } else {
            0
        }
    }

    pub fn continuous_free(&self, flat_idx: usize) -> usize {
        let head = self.head.load(Ordering::Relaxed);
        let hi = unsafe { (*head).hi.load(Ordering::Relaxed) };
        let total_occupied = hi.saturating_sub(flat_idx);

        println!("slot_tracker continuous_free: {flat_idx}, {hi}, {total_occupied}");

        if self.capacity >= total_occupied {
            self.capacity.wrapping_sub(total_occupied)
        } else {
            0
        }
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

    pub fn len(&self, capacity: usize) -> usize {
        debug_assert!(capacity.is_power_of_two(), "capacity isnt a power of 2");

        let lo = self.lo.load(Ordering::Relaxed);
        let hi = self.hi.load(Ordering::Relaxed);

        hi.wrapping_sub(lo) & (capacity - 1)
    }
}

#[cfg(test)]
mod slot_tracker_tests {
    use std::sync::atomic::Ordering;

    use crate::slot_tracker::SlotTracker;

    #[test]
    fn test_init() {
        let st = SlotTracker::<8>::new(16, 0, 0);

        let head = st.head.load(Ordering::Relaxed);
        let tail = st.tail.load(Ordering::Relaxed);
        assert_eq!(head, tail);

        let (lo, hi) = unsafe {
            (
                (*head).lo.load(Ordering::Relaxed),
                (*head).hi.load(Ordering::Relaxed),
            )
        };
        assert_eq!(lo, hi);
    }

    #[test]
    fn test_mark_occupied_extend() {
        let st = SlotTracker::<8>::new(16, 0, 0);

        let lo = 0;
        let hi = 4;
        st.mark_occupied(lo, hi);
        assert_eq!(st.spans(), 1);

        let head = st.head.load(Ordering::Relaxed);
        let tail = st.tail.load(Ordering::Relaxed);
        assert_eq!(head, tail);

        let (new_lo, new_hi) = unsafe {
            (
                (*head).lo.load(Ordering::Relaxed),
                (*head).hi.load(Ordering::Relaxed),
            )
        };
        assert_eq!(lo, new_lo);
        assert_eq!(hi, new_hi);
    }

    #[test]
    fn test_mark_occupied_split() {
        let st = SlotTracker::<8>::new(16, 0, 0);

        let lo_1 = 0;
        let hi_1 = 4;
        st.mark_occupied(lo_1, hi_1);
        assert_eq!(st.spans(), 1);

        let lo_2 = hi_1 + 2;
        let hi_2 = hi_1 + 4;
        st.mark_occupied(lo_2, hi_2);
        assert_eq!(st.spans(), 2);

        let head = st.head.load(Ordering::Relaxed);
        let tail = st.tail.load(Ordering::Relaxed);
        assert_ne!(head, tail);

        let (tail_lo, tail_hi) = unsafe {
            (
                (*tail).lo.load(Ordering::Relaxed),
                (*tail).hi.load(Ordering::Relaxed),
            )
        };
        assert_eq!(lo_1, tail_lo);
        assert_eq!(hi_1, tail_hi);

        let (head_lo, head_hi) = unsafe {
            (
                (*head).lo.load(Ordering::Relaxed),
                (*head).hi.load(Ordering::Relaxed),
            )
        };
        assert_eq!(lo_2, head_lo);
        assert_eq!(hi_2, head_hi);
    }

    #[test]
    fn test_mark_free_partial() {
        let st = SlotTracker::<8>::new(16, 0, 0);
        st.mark_occupied(0, 4);

        st.mark_free(2);
        assert_eq!(st.spans(), 1);

        let tail = st.tail.load(Ordering::Relaxed);
        let (tail_lo, tail_hi) = unsafe {
            (
                (*tail).lo.load(Ordering::Relaxed),
                (*tail).hi.load(Ordering::Relaxed),
            )
        };
        assert_eq!(tail_lo, 2);
        assert_eq!(tail_hi, 4);
    }

    #[test]
    fn test_mark_free_complete_one_span() {
        let st = SlotTracker::<8>::new(16, 0, 0);
        st.mark_occupied(0, 4);

        st.mark_free(4);
        assert_eq!(st.spans(), 1);

        let tail = st.tail.load(Ordering::Relaxed);
        let (tail_lo, tail_hi) = unsafe {
            (
                (*tail).lo.load(Ordering::Relaxed),
                (*tail).hi.load(Ordering::Relaxed),
            )
        };
        assert_eq!(tail_lo, 4);
        assert_eq!(tail_hi, 4);
    }

    #[test]
    fn test_mark_free_advances_tail() {
        let st = SlotTracker::<8>::new(16, 0, 0);
        st.mark_occupied(0, 4);
        st.mark_occupied(6, 10);
        assert_eq!(st.spans(), 2);

        st.mark_free(4);
        assert_eq!(st.spans(), 1);

        let tail = st.tail.load(Ordering::Relaxed);
        let head = st.head.load(Ordering::Relaxed);
        assert_eq!(tail, head);

        let (tail_lo, tail_hi) = unsafe {
            (
                (*tail).lo.load(Ordering::Relaxed),
                (*tail).hi.load(Ordering::Relaxed),
            )
        };
        assert_eq!(tail_lo, 6);
        assert_eq!(tail_hi, 10);
    }

    // [ 0, 1 | 2, 3 | 4, 5 | 6, 7 ]
    //1: ^rw                        -> [Span(0,0)]
    //2: ^r     ^w                  -> [Span(0,2)]
    //3:    ^r  ^w                  -> [Span(1,2)]
    //4:    ^r         ^w           -> [Span(1,2), Span(4,4)] (flushed)
    //5:        ^r     ^w           -> [Span(4,4)]
    //TODO: add wrapping examples
    #[test]
    fn test_sequence_example() {
        let st = SlotTracker::<8>::new(16, 0, 0);

        st.mark_occupied(0, 2);
        let tail = st.tail.load(Ordering::Relaxed);
        let (lo, hi) = unsafe {
            (
                (*tail).lo.load(Ordering::Relaxed),
                (*tail).hi.load(Ordering::Relaxed),
            )
        };
        assert_eq!(lo, 0);
        assert_eq!(hi, 2);

        st.mark_free(1);
        let (lo, hi) = unsafe {
            (
                (*tail).lo.load(Ordering::Relaxed),
                (*tail).hi.load(Ordering::Relaxed),
            )
        };
        assert_eq!(lo, 1);
        assert_eq!(hi, 2);

        st.mark_occupied(4, 4);
        assert_eq!(st.spans(), 2);

        st.mark_free(2);
        assert_eq!(st.spans(), 1);

        let tail = st.tail.load(Ordering::Relaxed);
        let (lo, hi) = unsafe {
            (
                (*tail).lo.load(Ordering::Relaxed),
                (*tail).hi.load(Ordering::Relaxed),
            )
        };
        assert_eq!(lo, 4);
        assert_eq!(hi, 4);
    }
}
