use std::{
    fmt::Write as _,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::observability::metrics::global_metrics;

use tokio::sync::Notify;

use crate::{
    args::ResolvedArgs,
    store::db::{Db, KeyMutationTracker},
    store::kv_store::KvStore,
    store::ttl::{TYPE_HASH, TYPE_JSON, TtlConfig, TtlManager, VersionCounter},
};
use common::types::options::{FileConfig, Options};

const STORAGE_ENGINE_PROPERTIES: &[&str] = &[
    "db.num-immutable-memtables",
    "db.memtable-memory-backed-immutables",
    "db.memtable-store-backed-immutables",
    "db.memtable-compaction-referenced-immutables",
    "db.memtable-pressure-immutables",
    "db.memtable-merge-layer-count",
    "db.memtable-merge-layer-pressure-units",
    "db.memtable-active-entries",
    "db.memtable-active-bytes",
    "db.memtable-immutable-entries",
    "db.memtable-immutable-bytes",
    "db.immutable-page-target-size",
    "db.immutable-page-hard-max-size",
    "db.immutable-index-page-count",
    "db.immutable-index-page-avg-bytes",
    "db.immutable-normal-page-count",
    "db.immutable-user-key-continuation-page-count",
    "db.immutable-oversized-value-page-count",
    "db.immutable-max-user-key-run-pages",
    "db.num-visible-tablets",
    "db.cur-compaction-version",
    "db.next-sequence",
    "db.num-wal-files",
    "db.block-cache-entries",
    "db.block-cache-bytes",
    "db.meta-cache-entries",
    "db.meta-cache-bytes",
    "db.page-cache-entries",
    "db.page-cache-bytes",
    "db.io-scheduler-queued",
    "db.io-scheduler-inflight",
    "db.io-scheduler-completed",
    "db.write-thread-stats",
    "db.get-stats",
    "db.get-detail-stats",
    "db.read-path-stats",
    "db.block-cache-hit-stats",
    "db.read-path-detail-stats",
    "db.memtable-lifecycle-stats",
    "db.memtable-active-storage-stats",
    "db.memtable-immutable-storage-stats",
];

/// DB 管理器
///
/// 所有逻辑数据库共享同一个底层 KvStore（kv_engine 实例），
/// 通过 key 前缀（db_index）实现数据隔离。
pub struct DatabaseManager {
    dbs: Vec<Arc<Db>>,
    store: KvStore,
    options: Options,
    version_counter: Arc<VersionCounter>,
    ttl_manager: Arc<TtlManager>,
    fulltext_shutdown: Arc<AtomicBool>,
    retired_gc_shutdown: Arc<AtomicBool>,
    list_notify: Arc<Notify>,
    zset_notify: Arc<Notify>,
    stream_notify: Arc<Notify>,
}

impl DatabaseManager {
    pub async fn new_async(args: Arc<ResolvedArgs>) -> Self {
        let options = FileConfig::load_from_path(std::path::Path::new(&args.config))
            .and_then(FileConfig::into_options)
            .unwrap_or_else(|_| Options::default());
        std::fs::create_dir_all(&options.db_path)
            .expect("failed to create onedis kv_engine db dir");
        std::fs::create_dir_all(&options.wal_dir)
            .expect("failed to create onedis kv_engine wal dir");
        let store = KvStore::open(options.clone());

        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        let mutation_tracker = Arc::new(KeyMutationTracker::default());
        let list_notify = Arc::new(Notify::new());
        let zset_notify = Arc::new(Notify::new());
        let stream_notify = Arc::new(Notify::new());

        // Rebuild TTL index and recover version counter from existing data
        ttl_manager
            .rebuild_from_store_async(args.databases as u16, &version_counter)
            .await;

        let mut dbs = Vec::new();
        for id in 0..args.databases {
            let db = Arc::new(Db::new_with_mutation_tracker(
                id as u16,
                store.clone(),
                version_counter.clone(),
                ttl_manager.clone(),
                mutation_tracker.clone(),
            ));
            dbs.push(db);
        }

        let fulltext_dbs = dbs.iter().map(Arc::downgrade).collect::<Vec<Weak<Db>>>();
        ttl_manager.set_expire_hook(Arc::new(move |db_index, key, type_tag, batch| {
            let Some(db) = fulltext_dbs.get(db_index as usize).and_then(Weak::upgrade) else {
                return false;
            };
            let result = match type_tag {
                TYPE_HASH => db.fulltext_enqueue_hash_delete_to_batch(batch, key),
                TYPE_JSON => db.fulltext_enqueue_json_delete_to_batch(batch, key),
                _ => return true,
            };
            if let Err(err) = result {
                log::error!("failed to enqueue fulltext delete for expired {key}: {err}");
                return false;
            }
            true
        }));

        let fulltext_shutdown = Arc::new(AtomicBool::new(false));
        let fulltext_worker_shutdown = fulltext_shutdown.clone();
        let fulltext_worker_dbs = dbs.clone();
        tokio::spawn(async move {
            while !fulltext_worker_shutdown.load(Ordering::Acquire) {
                for db in &fulltext_worker_dbs {
                    if let Err(err) = db.fulltext_maintenance_tick() {
                        log::error!("fulltext maintenance failed: {err}");
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        let retired_gc_shutdown = Arc::new(AtomicBool::new(false));
        let retired_gc_worker_shutdown = retired_gc_shutdown.clone();
        let retired_gc_worker_dbs = dbs.clone();
        tokio::spawn(async move {
            while !retired_gc_worker_shutdown.load(Ordering::Acquire) {
                for db in &retired_gc_worker_dbs {
                    let reclaimed = db.retired_version_gc_tick();
                    if reclaimed > 0 {
                        log::debug!(
                            "retired version GC reclaimed {reclaimed} version namespace(s)"
                        );
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        // Start background TTL sweeper
        ttl_manager.start_sweeper();

        DatabaseManager {
            dbs,
            store,
            options,
            version_counter,
            ttl_manager,
            fulltext_shutdown,
            retired_gc_shutdown,
            list_notify,
            zset_notify,
            stream_notify,
        }
    }

    pub fn get_db(&self, idx: usize) -> Arc<Db> {
        self.dbs[idx].clone()
    }

    pub fn get_all_dbs(&self) -> &[Arc<Db>] {
        &self.dbs
    }

    pub fn store(&self) -> &KvStore {
        &self.store
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    pub fn version_counter(&self) -> &Arc<VersionCounter> {
        &self.version_counter
    }

    pub fn ttl_manager(&self) -> &Arc<TtlManager> {
        &self.ttl_manager
    }

    pub fn list_notify(&self) -> &Arc<Notify> {
        &self.list_notify
    }

    pub fn notify_list_waiters(&self) {
        self.list_notify.notify_waiters();
    }

    pub fn zset_notify(&self) -> &Arc<Notify> {
        &self.zset_notify
    }

    pub fn notify_zset_waiters(&self) {
        self.zset_notify.notify_waiters();
    }

    pub fn stream_notify(&self) -> &Arc<Notify> {
        &self.stream_notify
    }

    pub fn notify_stream_waiters(&self) {
        self.stream_notify.notify_waiters();
    }

    pub fn render_observability_prometheus(&self) -> String {
        let mut out = String::new();
        let mut expired_keys = 0;
        let mut ttl_stale_entries = 0;
        let mut ttl_sweep_cycles = 0;
        let mut fulltext_creating = 0;
        let mut fulltext_backfilling = 0;
        let mut fulltext_ready = 0;
        let mut fulltext_dirty = 0;
        let mut fulltext_rebuilding = 0;
        let mut fulltext_dropping = 0;
        let mut fulltext_outbox_pending = 0;
        let mut fulltext_backfill_pending = 0;
        let mut stream_groups = 0;
        let mut stream_pending_entries = 0;
        let mut vector_indexes = 0;

        let _ = writeln!(out, "# TYPE onedis_db_keys gauge");
        let _ = writeln!(out, "# TYPE onedis_db_expires gauge");
        let _ = writeln!(out, "# TYPE onedis_db_avg_ttl_milliseconds gauge");
        for (db_index, db) in self.dbs.iter().enumerate() {
            let ttl = db.ttl_observability_snapshot();
            expired_keys = ttl.expired_keys;
            ttl_stale_entries = ttl.stale_entries_skipped;
            ttl_sweep_cycles = ttl.sweep_cycles;
            let _ = writeln!(out, "onedis_db_keys{{db=\"{db_index}\"}} {}", db.len());
            let _ = writeln!(
                out,
                "onedis_db_expires{{db=\"{db_index}\"}} {}",
                ttl.expires
            );
            let _ = writeln!(
                out,
                "onedis_db_avg_ttl_milliseconds{{db=\"{db_index}\"}} {}",
                ttl.avg_ttl_millis
            );

            let fulltext = db.fulltext_observability_snapshot();
            fulltext_creating += fulltext.creating;
            fulltext_backfilling += fulltext.backfilling;
            fulltext_ready += fulltext.ready;
            fulltext_dirty += fulltext.dirty;
            fulltext_rebuilding += fulltext.rebuilding;
            fulltext_dropping += fulltext.dropping;
            fulltext_outbox_pending += fulltext.outbox_pending;
            fulltext_backfill_pending += fulltext.backfill_pending;

            let stream = db.stream_observability_snapshot();
            stream_groups += stream.groups;
            stream_pending_entries += stream.pending_entries;

            let vector = db.vector_observability_snapshot();
            vector_indexes += vector.indexes;
        }
        let metrics = global_metrics();
        metrics.set_stream_snapshot(stream_groups, stream_pending_entries);
        metrics.set_vector_indexes(vector_indexes);

        let _ = writeln!(out, "# TYPE onedis_expired_keys_total counter");
        let _ = writeln!(out, "onedis_expired_keys_total {expired_keys}");
        let _ = writeln!(out, "# TYPE onedis_ttl_sweep_cycles_total counter");
        let _ = writeln!(out, "onedis_ttl_sweep_cycles_total {ttl_sweep_cycles}");
        let _ = writeln!(out, "# TYPE onedis_ttl_stale_entries_skipped_total counter");
        let _ = writeln!(
            out,
            "onedis_ttl_stale_entries_skipped_total {ttl_stale_entries}"
        );

        let _ = writeln!(out, "# TYPE onedis_fulltext_indexes_total gauge");
        for (state, value) in [
            ("creating", fulltext_creating),
            ("backfilling", fulltext_backfilling),
            ("ready", fulltext_ready),
            ("dirty", fulltext_dirty),
            ("rebuilding", fulltext_rebuilding),
            ("dropping", fulltext_dropping),
        ] {
            let _ = writeln!(
                out,
                "onedis_fulltext_indexes_total{{state=\"{state}\"}} {value}"
            );
        }
        let _ = writeln!(out, "# TYPE onedis_fulltext_outbox_pending gauge");
        let _ = writeln!(
            out,
            "onedis_fulltext_outbox_pending {fulltext_outbox_pending}"
        );
        let _ = writeln!(out, "# TYPE onedis_fulltext_backfill_pending gauge");
        let _ = writeln!(
            out,
            "onedis_fulltext_backfill_pending {fulltext_backfill_pending}"
        );
        self.render_storage_engine_properties(&mut out);
        out
    }

    fn render_storage_engine_properties(&self, out: &mut String) {
        let _ = writeln!(out, "# TYPE onedis_storage_engine_property gauge");
        for property in STORAGE_ENGINE_PROPERTIES {
            let Ok(Some(value)) = self.store.get_property(property) else {
                continue;
            };
            if let Some(number) = parse_property_number(&value) {
                let _ = writeln!(
                    out,
                    "onedis_storage_engine_property{{property=\"{property}\"}} {number}"
                );
                continue;
            }
            for (field, number) in parse_property_fields(&value) {
                let _ = writeln!(
                    out,
                    "onedis_storage_engine_property{{property=\"{property}.{field}\"}} {number}"
                );
            }
        }
    }
}

fn parse_property_number(value: &str) -> Option<f64> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

fn parse_property_fields(value: &str) -> Vec<(String, f64)> {
    value
        .split_ascii_whitespace()
        .filter_map(|part| {
            let (key, raw_value) = part.split_once('=')?;
            let raw_value = raw_value.trim_end_matches('%');
            let value = raw_value
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite())?;
            Some((sanitize_property_field(key), value))
        })
        .collect()
}

fn sanitize_property_field(field: &str) -> String {
    field
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

impl Drop for DatabaseManager {
    fn drop(&mut self) {
        self.fulltext_shutdown.store(true, Ordering::Release);
        self.retired_gc_shutdown.store(true, Ordering::Release);
        self.ttl_manager.shutdown();
    }
}
