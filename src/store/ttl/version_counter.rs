// ============================================================================
// Version Counter
// ============================================================================

/// Monotonically increasing, lock-free version generator.
///
/// A new version is allocated each time a key is created or changes type.
/// Sub-keys carry the version in their encoding, which allows the TTL
/// sweeper to issue a single `DeleteRange` per namespace to reclaim all
/// sub-keys that belong to an expired (key, version) pair.
pub struct VersionCounter {
    counter: AtomicU64,
    reserved_until: AtomicU64,
    reservation_lock: AtomicBool,
}

impl VersionCounter {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
            reserved_until: AtomicU64::new(0),
            reservation_lock: AtomicBool::new(false),
        }
    }

    /// Allocate the next version number.
    #[inline]
    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Allocate the next version, reserving a durable high-water mark in blocks.
    ///
    /// The reservation callback runs before a new block is made visible to
    /// other allocators. Crashes may leave gaps, but versions are only required
    /// to be unique across restarts so old sub-key namespaces are never reused.
    pub fn next_reserved<F>(&self, mut persist_high_water: F) -> u64
    where
        F: FnMut(u64),
    {
        loop {
            let current = self.counter.load(Ordering::Relaxed);
            let reserved_until = self.reserved_until.load(Ordering::Acquire);
            if current < reserved_until {
                if self
                    .counter
                    .compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return current + 1;
                }
                continue;
            }

            let _guard = self.acquire_reservation_lock_blocking();
            let current = self.counter.load(Ordering::Relaxed);
            let reserved_until = self.reserved_until.load(Ordering::Acquire);
            if current >= reserved_until {
                let high_water = current.saturating_add(VERSION_RESERVATION_BLOCK);
                persist_high_water(high_water);
                self.reserved_until.store(high_water, Ordering::Release);
            }
        }
    }

    pub async fn next_reserved_async<F, Fut>(&self, mut persist_high_water: F) -> u64
    where
        F: FnMut(u64) -> Fut,
        Fut: Future<Output = ()>,
    {
        loop {
            let current = self.counter.load(Ordering::Relaxed);
            let reserved_until = self.reserved_until.load(Ordering::Acquire);
            if current < reserved_until {
                if self
                    .counter
                    .compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return current + 1;
                }
                continue;
            }

            let _guard = self.acquire_reservation_lock_async().await;
            let current = self.counter.load(Ordering::Relaxed);
            let reserved_until = self.reserved_until.load(Ordering::Acquire);
            if current >= reserved_until {
                let high_water = current.saturating_add(VERSION_RESERVATION_BLOCK);
                persist_high_water(high_water).await;
                self.reserved_until.store(high_water, Ordering::Release);
            }
        }
    }

    fn acquire_reservation_lock_blocking(&self) -> VersionReservationGuard<'_> {
        while self
            .reservation_lock
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::thread::yield_now();
        }
        VersionReservationGuard {
            lock: &self.reservation_lock,
        }
    }

    async fn acquire_reservation_lock_async(&self) -> VersionReservationGuard<'_> {
        while self
            .reservation_lock
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            tokio::task::yield_now().await;
        }
        VersionReservationGuard {
            lock: &self.reservation_lock,
        }
    }

    /// Return the most-recently-observed maximum version.
    #[inline]
    pub fn current(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }

    /// Update the high-water mark if `v` exceeds the current maximum.
    ///
    /// Called during startup rebuild so the counter picks up where it left off.
    pub fn observe(&self, v: u64) {
        let _ = self
            .counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                if v > cur { Some(v) } else { None }
            });
        let _ = self
            .reserved_until
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                if v > cur { Some(v) } else { None }
            });
    }
}

struct VersionReservationGuard<'a> {
    lock: &'a AtomicBool,
}

impl Drop for VersionReservationGuard<'_> {
    fn drop(&mut self) {
        self.lock.store(false, Ordering::Release);
    }
}
