use std::{array, cell::UnsafeCell, mem::MaybeUninit, ops::Deref};

#[repr(align(64))]
pub struct CachePadded<T>(pub T);

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[repr(C, align(64))]
pub struct CacheLine<T, const N: usize> {
    cell: UnsafeCell<[MaybeUninit<T>; N]>,
}

impl<T, const N: usize> Default for CacheLine<T, N> {
    fn default() -> Self {
        Self {
            cell: UnsafeCell::new(array::from_fn(|_| MaybeUninit::uninit())),
        }
    }
}

impl<T, const N: usize> CacheLine<T, N> {
    /// # Safety
    ///
    /// The caller has to make sure that writing is allowed and wont cause any race conditions with other threads
    #[inline]
    pub const unsafe fn write(&self, index: usize, value: T) {
        let item_ptr = unsafe { self.get_item_ptr(index) };

        unsafe {
            item_ptr.write(MaybeUninit::new(value));
        }
    }

    /// # Safety
    ///
    /// The caller has to make sure that reading is allowed and wont cause any race conditions with other threads
    #[inline]
    pub const unsafe fn read(&self, index: usize) -> T {
        let item_ptr = unsafe { self.get_item_ptr(index) };

        unsafe { item_ptr.read().assume_init() }
    }

    #[inline]
    pub const unsafe fn get_item_ptr(&self, index: usize) -> *mut MaybeUninit<T> {
        let array_ptr = self.cell.get();
        unsafe { array_ptr.cast::<MaybeUninit<T>>().add(index) }
    }
}
