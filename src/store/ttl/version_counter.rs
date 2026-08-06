// ============================================================================
// Version Counter
// ============================================================================

/// Monotonically increasing, lock-free version generator.
///
/// A new version is allocated each time a key is created or changes type.
/// Sub-keys carry the version in their encoding. Every committed structured
/// version has an owner record in the same atomic batch, so startup can
/// rebuild this counter without a separate high-water write. New allocations
/// also have a wall-clock floor, preventing version reuse after compaction has
/// already removed an old owner record.
pub struct VersionCounter {
    counter: AtomicU64,
}

const VERSION_TIMESTAMP_COUNTER_BITS: u32 = 11;

impl Default for VersionCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionCounter {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }

    /// Allocate the next version number.
    pub fn next(&self) -> u64 {
        let timestamp_micros: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX >> VERSION_TIMESTAMP_COUNTER_BITS);
        let timestamp_floor = timestamp_micros
            .min(u64::MAX >> VERSION_TIMESTAMP_COUNTER_BITS)
            << VERSION_TIMESTAMP_COUNTER_BITS;
        loop {
            let current = self.counter.load(Ordering::Relaxed);
            let next = current.saturating_add(1).max(timestamp_floor);
            assert!(next > current, "version counter exhausted");
            if self
                .counter
                .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return next;
            }
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
    }
}
