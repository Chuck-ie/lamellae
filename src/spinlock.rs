const SOFT_LIMIT: usize = 6;
const HARD_LIMIT: usize = 12;

pub struct Spinlock {
    spin_count: usize,
}

impl Default for Spinlock {
    fn default() -> Self {
        Self::new()
    }
}

impl Spinlock {
    #[must_use]
    pub const fn new() -> Self {
        Self { spin_count: 0 }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn spin(&mut self) -> bool {
        let spins = 1 << self.spin_count.min(SOFT_LIMIT);

        for _ in 0..spins {
            core::hint::spin_loop();
        }

        if self.spin_count <= SOFT_LIMIT {
            self.spin_count += 1;
        }

        self.spin_count <= SOFT_LIMIT
    }

    #[inline]
    pub fn spin_heavy(&mut self) -> bool {
        let spins = 1 << self.spin_count.min(SOFT_LIMIT);

        if self.spin_count <= SOFT_LIMIT {
            for _ in 0..spins {
                core::hint::spin_loop();
            }
        } else {
            for _ in 0..spins {
                core::hint::spin_loop();
            }

            std::thread::yield_now();
        }

        if self.spin_count <= HARD_LIMIT {
            self.spin_count += 1;
        }

        self.spin_count <= HARD_LIMIT
    }
}
