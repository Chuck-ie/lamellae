use core::sync::atomic::{AtomicPtr, Ordering};

use crate::SlotPtr;

pub struct InterruptionTracker {
    head: AtomicPtr<Interruption>,
    tail: AtomicPtr<Interruption>,
}

impl InterruptionTracker {
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(core::ptr::null_mut()),
            tail: AtomicPtr::new(core::ptr::null_mut()),
        }
    }

    pub fn push(&self, slot_ptr: SlotPtr) {
        let new_head = Interruption::from_slot_ptr(slot_ptr);
        let head = self.head.load(Ordering::Acquire);

        if head.is_null() {
            // First push: initialize both head and tail so a concurrent
            // `pop_next` can observe the node through `tail`.
            self.tail.store(new_head, Ordering::Release);
        } else {
            // Safety: `head` was loaded above and is non-null; in SPSC only the
            // producer mutates `head`, so it remains valid here.
            unsafe {
                (*head).next.store(new_head, Ordering::Release);
            }
        }

        self.head.store(new_head, Ordering::Release);
    }

    pub fn pop_next(&self) -> Option<SlotPtr> {
        let tail = self.tail.load(Ordering::Acquire);

        if tail.is_null() {
            None
        } else {
            let slot_ptr = unsafe { (*tail).slot_ptr };
            let next = unsafe { (*tail).next.load(Ordering::Acquire) };

            let _ = unsafe { Box::from_raw(tail) };
            self.tail.store(next, Ordering::Release);

            Some(slot_ptr)
        }
    }

    pub fn peek_next(&self) -> Option<SlotPtr> {
        let tail = self.tail.load(Ordering::Acquire);

        if tail.is_null() {
            None
        } else {
            Some(unsafe { (*tail).slot_ptr })
        }
    }
}

pub struct Interruption {
    slot_ptr: SlotPtr,
    next: AtomicPtr<Self>,
}

impl Interruption {
    pub fn from_slot_ptr(slot_ptr: SlotPtr) -> *mut Self {
        Box::into_raw(Box::new(Self {
            slot_ptr,
            next: AtomicPtr::new(core::ptr::null_mut()),
        }))
    }
}
