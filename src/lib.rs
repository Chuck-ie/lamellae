pub mod buffer;
pub mod consumer;
pub mod producer;
pub mod reservation;
mod slot_tracker;
mod spinlock;
mod wrapper;

#[macro_export]
macro_rules! channel {
    ($ty:ty, $capacity:expr) => {{
        // TODO: This 64-byte magic number should ideally be determined based on the target architecture.
        const CACHE_LINE_SIZE: usize = 64;
        const ELEMENT_SIZE: usize = core::mem::size_of::<$ty>();

        // Validate type size constraints at compile time
        const VALIDATED_ELEMENT_SIZE: usize = {
            assert!(
                ELEMENT_SIZE <= CACHE_LINE_SIZE,
                "Compile Error: Type size cannot be greater than the cache line size (64 bytes)!"
            );
            assert!(
                ELEMENT_SIZE > 0,
                "Compile Error: Zero-Sized Types (ZSTs) are not allowed!"
            );

            ELEMENT_SIZE
        };

        const ELEMENTS_PER_CACHE_LINE: usize = CACHE_LINE_SIZE / VALIDATED_ELEMENT_SIZE;
        const TARGET_CAPACITY: usize = $capacity;

        // Validate capacity constraints at compile time
        const _: () = {
            assert!(
                TARGET_CAPACITY.is_power_of_two(),
                "Compile Error: Capacity must be a power of 2!"
            );
            assert!(
                TARGET_CAPACITY >= 4 * ELEMENTS_PER_CACHE_LINE,
                "Compile Error: Capacity is too small! It must be at least four times the elements per cache line."
            );
        };

        $crate::buffer::Buffer::<$ty, ELEMENTS_PER_CACHE_LINE>::with_capacity(TARGET_CAPACITY)
    }};
}

#[derive(Clone, Copy)]
struct SlotPtr {
    cl_index: usize,
    cl_offset: usize,
}

impl From<(usize, usize)> for SlotPtr {
    #[inline]
    fn from(tuple: (usize, usize)) -> Self {
        Self {
            cl_index: tuple.0,
            cl_offset: tuple.1,
        }
    }
}

impl From<SlotPtr> for (usize, usize) {
    #[inline]
    fn from(slot: SlotPtr) -> Self {
        (slot.cl_index, slot.cl_offset)
    }
}

impl SlotPtr {
    #[inline]
    pub const fn set(&mut self, new_cl_index: usize, new_cl_offset: usize) {
        self.cl_index = new_cl_index;
        self.cl_offset = new_cl_offset;
    }
}

// #[cfg(test)]
// mod unit_tests {
//     #[test]
//     fn test_advance_cl_offset() {
//         let (_, mut rx) = channel!(usize, 32);
//     }
// }
