use std::{mem::MaybeUninit, sync::atomic::Ordering};

use crate::{consumer::Consumer, producer::Producer};

pub struct SendReservation<'a, T, const N: usize> {
    pub(crate) tx: &'a mut Producer<T, N>,
    pub(crate) s1: *mut T,
    pub(crate) s1_remaining: usize,
    pub(crate) s2: *mut T,
    pub(crate) s2_remaining: usize,
    pub(crate) total_reserved: usize,
    pub(crate) start_cl_index: usize,
    pub(crate) start_cl_offset: usize,
}

impl<T, const N: usize> Drop for SendReservation<'_, T, N> {
    fn drop(&mut self) {
        unsafe { self.finalize_reservation() }
    }
}

impl<T, const N: usize> SendReservation<'_, T, N> {
    pub fn send(&mut self, value: T) -> Option<()> {
        if self.s1_remaining > 0 {
            unsafe {
                self.s1.write(value);
                self.s1 = self.s1.add(1);
            }

            self.s1_remaining -= 1;
            Some(())
        } else if self.s2_remaining > 0 {
            unsafe {
                self.s2.write(value);
                self.s2 = self.s2.add(1);
            }

            self.s2_remaining -= 1;
            Some(())
        } else {
            None
        }
    }

    unsafe fn finalize_reservation(&mut self) {
        let total_remaining = self.s1_remaining + self.s2_remaining;
        let total_sent = self.total_reserved - total_remaining;

        if total_sent == 0 {
            return;
        }

        let from_abs_index = (self.start_cl_index * N) + self.start_cl_offset;
        let to_abs_index = from_abs_index + total_sent;

        let final_abs_index = to_abs_index % self.tx.buffer.capacity;
        let mut next_cl_index = (final_abs_index / N) & self.tx.buffer.cl_mask;
        let mut next_cl_offset = final_abs_index % N;

        if next_cl_offset == 0 && to_abs_index > 0 {
            next_cl_index = (next_cl_index.wrapping_sub(1)) & self.tx.buffer.cl_mask;
            next_cl_offset = N;
        }

        self.tx.slot_ptr.set(next_cl_index, next_cl_offset);
        self.tx.buffer.head.store(next_cl_index, Ordering::Release);
    }

    pub const fn as_mut_slices(
        &mut self,
    ) -> (
        &mut [std::mem::MaybeUninit<T>],
        &mut [std::mem::MaybeUninit<T>],
    ) {
        unsafe {
            let first =
                std::slice::from_raw_parts_mut(self.s1.cast::<MaybeUninit<T>>(), self.s1_remaining);
            let second =
                std::slice::from_raw_parts_mut(self.s2.cast::<MaybeUninit<T>>(), self.s2_remaining);
            (first, second)
        }
    }

    pub fn send_slice(&mut self, src: &[T]) -> usize
    where
        T: Copy,
    {
        let to_copy = std::cmp::min(src.len(), self.s1_remaining + self.s2_remaining);
        if to_copy == 0 {
            return 0;
        }

        let mut src_offset = 0;

        if self.s1_remaining > 0 {
            let c1 = std::cmp::min(to_copy, self.s1_remaining);
            unsafe {
                std::ptr::copy_nonoverlapping(src.as_ptr().add(src_offset), self.s1, c1);
                self.s1 = self.s1.add(c1);
            }
            self.s1_remaining -= c1;
            src_offset += c1;
        }

        let remaining = to_copy - src_offset;

        if remaining > 0 && self.s2_remaining > 0 {
            let c2 = std::cmp::min(remaining, self.s2_remaining);
            unsafe {
                std::ptr::copy_nonoverlapping(src.as_ptr().add(src_offset), self.s2, c2);
                self.s2 = self.s2.add(c2);
            }
            self.s2_remaining -= c2;
        }

        to_copy
    }
}

pub struct RecvReservation<'a, T: Copy, const N: usize> {
    pub(crate) rx: &'a mut Consumer<T, N>,
    pub(crate) s1: *const T,
    pub(crate) s1_remaining: usize,
    pub(crate) s2: *const T,
    pub(crate) s2_remaining: usize,
    pub(crate) total_reserved: usize,
    pub(crate) start_cl_index: usize,
    pub(crate) start_cl_offset: usize,
}

impl<T: Copy, const N: usize> Drop for RecvReservation<'_, T, N> {
    fn drop(&mut self) {
        unsafe { self.finalize_reservation() }
    }
}

impl<T: Copy, const N: usize> RecvReservation<'_, T, N> {
    pub const fn recv(&mut self) -> Option<T> {
        if self.s1_remaining > 0 {
            let value = unsafe { self.s1.read() };
            unsafe { self.s1 = self.s1.add(1) };
            self.s1_remaining -= 1;
            Some(value)
        } else if self.s2_remaining > 0 {
            let value = unsafe { self.s2.read() };
            unsafe { self.s2 = self.s2.add(1) };
            self.s2_remaining -= 1;
            Some(value)
        } else {
            None
        }
    }

    unsafe fn finalize_reservation(&mut self) {
        let total_remaining = self.s1_remaining + self.s2_remaining;
        let total_received = self.total_reserved - total_remaining;

        if total_received == 0 {
            return;
        }

        let from_abs_index = (self.start_cl_index * N) + self.start_cl_offset;
        let to_abs_index = from_abs_index + total_received;

        let final_abs_index = to_abs_index % self.rx.buffer.capacity;
        let mut next_cl_index = (final_abs_index / N) & self.rx.buffer.cl_mask;
        let mut next_cl_offset = final_abs_index % N;

        if next_cl_offset == 0 && to_abs_index > 0 {
            next_cl_index = (next_cl_index.wrapping_sub(1)) & self.rx.buffer.cl_mask;
            next_cl_offset = N;
        }

        self.rx.slot_ptr.set(next_cl_index, next_cl_offset);
        self.rx.buffer.tail.store(next_cl_index, Ordering::Release);
    }

    #[must_use]
    pub const fn as_slices(&self) -> (&[T], &[T]) {
        unsafe {
            let first = std::slice::from_raw_parts(self.s1, self.s1_remaining);
            let second = std::slice::from_raw_parts(self.s2, self.s2_remaining);
            (first, second)
        }
    }

    pub fn recv_slice(&mut self, dst: &mut [T]) -> usize {
        let to_copy = std::cmp::min(dst.len(), self.s1_remaining + self.s2_remaining);
        if to_copy == 0 {
            return 0;
        }

        let mut dst_offset = 0;

        if self.s1_remaining > 0 {
            let c1 = std::cmp::min(to_copy, self.s1_remaining);
            unsafe {
                std::ptr::copy_nonoverlapping(self.s1, dst.as_mut_ptr().add(dst_offset), c1);
                self.s1 = self.s1.add(c1);
            }
            self.s1_remaining -= c1;
            dst_offset += c1;
        }

        let remaining = to_copy - dst_offset;
        if remaining > 0 && self.s2_remaining > 0 {
            let c2 = std::cmp::min(remaining, self.s2_remaining);
            unsafe {
                std::ptr::copy_nonoverlapping(self.s2, dst.as_mut_ptr().add(dst_offset), c2);
                self.s2 = self.s2.add(c2);
            }
            self.s2_remaining -= c2;
        }

        to_copy
    }
}
