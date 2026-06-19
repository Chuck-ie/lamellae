use std::sync::atomic::Ordering;

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

        self.tx.cl_index = next_cl_index;
        self.tx.cl_offset = next_cl_offset;

        let mut i = self.start_cl_index;

        while i != next_cl_index {
            unsafe { self.tx.buffer.write_counts.get_unchecked(i).get().write(N) }
            i = (i + 1) & self.tx.buffer.cl_mask;
        }

        self.tx.buffer.head.store(next_cl_index, Ordering::Release);
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

        self.rx.cl_index = next_cl_index;
        self.rx.cl_offset = next_cl_offset;

        let mut i = self.start_cl_index;

        while i != next_cl_index {
            unsafe { self.rx.buffer.write_counts.get_unchecked(i).get().write(0) }
            i = (i + 1) & self.rx.buffer.cl_mask;
        }

        self.rx.buffer.tail.store(next_cl_index, Ordering::Release);
    }
}
