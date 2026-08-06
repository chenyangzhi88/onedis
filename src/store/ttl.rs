//! Industrial-grade Redis TTL expiration engine.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                        TtlManager                            │
//! │                                                              │
//! │  ┌───────────────────┐    ┌────────────────────────────────┐ │
//! │  │ TTL namespace     │    │  Background Sweeper (tokio)    │ │
//! │  │ in kv_engine      │───►│  ┌──────────────────────────┐  │ │
//! │  │ ordered by        │    │  │ 1. scan expired entries  │  │ │
//! │  │ (db, expire, key) │    │  │ 2. Lazy Double Check     │  │ │
//! │  └───────────────────┘    │  │ 3. delete meta + index   │  │ │
//! │                           │  └──────────────────────────┘  │ │
//! │                           └────────────────────────────────┘ │
//! │  ┌──────────────────────────────────────────────────────┐    │
//! │  │  Notify — wake sweeper during shutdown               │    │
//! │  └──────────────────────────────────────────────────────┘    │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Design Decisions
//!
//! - **Append-only index**: EXPIRE on an existing key appends a new entry;
//!   the stale entry is filtered during sweep via Double Check. Write paths do
//!   not coordinate through an in-memory TTL tree or key-to-deadline map.
//!
//! - **Lazy Double Check**: Before physical delete the sweeper verifies
//!   (1) meta key still exists, (2) stored expire_ms matches the index entry.
//!   This eliminates all races with user DEL / PERSIST / re-EXPIRE commands.
//!
//! - **Version-based compaction cleanup**: Expiration only removes the main
//!   metadata and TTL index. Versioned sub-keys become unreachable immediately
//!   and are physically reclaimed by the engine compaction filter.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use common::types::write_batch::WriteBatch;
use log::{debug, info};

use super::kv_store::KvStore;
use crate::observability::metrics::{elapsed_us, global_metrics};
use crate::store::db::{TtlKeyEncoding, ttl_key_encoding_for_store};

include!("ttl/constants.rs");
include!("ttl/meta_header.rs");
include!("ttl/version_counter.rs");
include!("ttl/sub_key_ranges.rs");
include!("ttl/manager_types.rs");
include!("ttl/manager_core.rs");
include!("ttl/manager_index.rs");
include!("ttl/manager_sweeper.rs");
include!("ttl/key_helpers.rs");

#[cfg(test)]
mod tests {
    include!("ttl/tests.rs");
}
