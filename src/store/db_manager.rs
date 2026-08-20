use std::{
    collections::BTreeMap,
    fmt::Write as _,
    future::Future,
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::observability::metrics::global_metrics;
use anyhow::{Context, Error};

use crate::{
    args::ResolvedArgs,
    store::db::{CounterCacheRuntime, Db, KeyMutationTracker, VectorRuntimeRegistry},
    store::kv_store::KvStore,
    store::ttl::{TYPE_HASH, TYPE_JSON, TYPE_VECTOR, TtlConfig, TtlManager, VersionCounter},
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

#[derive(Clone, Debug, Default)]
pub struct BackgroundTaskSnapshot {
    pub running: bool,
    pub failures: u64,
    pub last_success_ms: u64,
    pub last_error: Option<String>,
}

#[derive(Default)]
pub struct BackgroundTaskHealth {
    tasks: RwLock<BTreeMap<&'static str, BackgroundTaskSnapshot>>,
    fatal_reason: RwLock<Option<String>>,
}

impl BackgroundTaskHealth {
    fn register(&self, name: &'static str) {
        self.tasks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                name,
                BackgroundTaskSnapshot {
                    running: true,
                    ..BackgroundTaskSnapshot::default()
                },
            );
    }

    fn record_success(&self, name: &'static str) {
        if let Some(task) = self
            .tasks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(name)
        {
            task.last_success_ms = unix_time_ms();
        }
    }

    fn record_error(&self, name: &'static str, error: impl Into<String>) {
        if let Some(task) = self
            .tasks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(name)
        {
            task.failures = task.failures.saturating_add(1);
            task.last_error = Some(error.into());
        }
    }

    fn record_stopped(&self, name: &'static str, expected: bool) {
        if let Some(task) = self
            .tasks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(name)
        {
            task.running = false;
            if !expected {
                task.failures = task.failures.saturating_add(1);
                task.last_error = Some("task exited unexpectedly".to_string());
            }
        }
        if !expected {
            let mut reason = self
                .fatal_reason
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if reason.is_none() {
                *reason = Some(format!("background task {name} exited unexpectedly"));
            }
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.fatal_reason
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    }

    pub fn degraded_reason(&self) -> Option<String> {
        self.fatal_reason
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn snapshot(&self) -> BTreeMap<&'static str, BackgroundTaskSnapshot> {
        self.tasks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

struct BackgroundTaskGuard {
    name: &'static str,
    shutdown: Arc<AtomicBool>,
    health: Arc<BackgroundTaskHealth>,
}

impl Drop for BackgroundTaskGuard {
    fn drop(&mut self) {
        self.health
            .record_stopped(self.name, self.shutdown.load(Ordering::Acquire));
    }
}

struct ManagedBackgroundTask {
    name: &'static str,
    handle: tokio::task::JoinHandle<()>,
}

fn spawn_background_task<F>(
    name: &'static str,
    shutdown: Arc<AtomicBool>,
    health: Arc<BackgroundTaskHealth>,
    future: F,
) -> ManagedBackgroundTask
where
    F: Future<Output = ()> + Send + 'static,
{
    health.register(name);
    let guard_health = Arc::clone(&health);
    let guard_shutdown = Arc::clone(&shutdown);
    let handle = tokio::spawn(async move {
        let _guard = BackgroundTaskGuard {
            name,
            shutdown: guard_shutdown,
            health: guard_health,
        };
        future.await;
    });
    ManagedBackgroundTask { name, handle }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

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
    mutation_tracker: Arc<KeyMutationTracker>,
    background_health: Arc<BackgroundTaskHealth>,
    fulltext_shutdown: Arc<AtomicBool>,
    version_scan_shutdown: Arc<AtomicBool>,
    background_tasks: std::sync::Mutex<Vec<ManagedBackgroundTask>>,
}

impl DatabaseManager {
    fn request_shutdown(&self) {
        self.fulltext_shutdown.store(true, Ordering::Release);
        self.version_scan_shutdown.store(true, Ordering::Release);
        self.ttl_manager.shutdown();
        self.mutation_tracker.notify_all_waiters();
    }

    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.request_shutdown();
        let tasks = {
            let mut tasks = self
                .background_tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *tasks)
        };
        let deadline = tokio::time::Instant::now() + timeout;
        let mut shutdown_errors = Vec::new();
        for mut task in tasks {
            match tokio::time::timeout_at(deadline, &mut task.handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    shutdown_errors.push(format!("{} join failed: {error}", task.name));
                }
                Err(_) => {
                    task.handle.abort();
                    let _ = task.handle.await;
                    shutdown_errors.push(format!("{} exceeded shutdown deadline", task.name));
                }
            }
        }
        for db in &self.dbs {
            db.shutdown_fulltext_runtime();
        }
        if shutdown_errors.is_empty() {
            Ok(())
        } else {
            Err(Error::msg(shutdown_errors.join("; ")))
        }
    }

    pub async fn new_async(args: Arc<ResolvedArgs>) -> Self {
        Self::try_new_async(args)
            .await
            .expect("failed to initialize OneDis database manager")
    }

    pub async fn try_new_async(args: Arc<ResolvedArgs>) -> Result<Self, Error> {
        let options = FileConfig::load_from_path(std::path::Path::new(&args.config))
            .with_context(|| format!("failed to load kv-engine config {}", args.config))?
            .into_options()
            .with_context(|| format!("invalid kv-engine config {}", args.config))?;
        std::fs::create_dir_all(&options.db_path).with_context(|| {
            format!(
                "failed to create kv-engine db dir {}",
                options.db_path.display()
            )
        })?;
        std::fs::create_dir_all(&options.wal_dir).with_context(|| {
            format!(
                "failed to create kv-engine WAL dir {}",
                options.wal_dir.display()
            )
        })?;
        let store = KvStore::try_open(options.clone())?;

        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        let mutation_tracker = Arc::new(KeyMutationTracker::default());
        let vector_runtimes = Arc::new(VectorRuntimeRegistry::default());
        let counter_cache = Arc::new(CounterCacheRuntime::default());

        // Rebuild TTL index and recover version counter from existing data
        ttl_manager
            .rebuild_from_store_async(args.databases as u16, &version_counter)
            .await?;

        let mut dbs = Vec::new();
        for id in 0..args.databases {
            let db = Arc::new(Db::try_new_with_mutation_tracker_and_vector_runtimes(
                id as u16,
                store.clone(),
                version_counter.clone(),
                ttl_manager.clone(),
                mutation_tracker.clone(),
                vector_runtimes.clone(),
                counter_cache.clone(),
            )?);
            dbs.push(db);
        }

        for db in &dbs {
            let startup_db = Arc::clone(db);
            tokio::task::spawn_blocking(move || startup_db.load_vector_runtimes_for_startup())
                .await
                .map_err(|error| Error::msg(format!("vector startup worker failed: {error}")))??;
        }

        let weak_vector_runtimes = Arc::downgrade(&vector_runtimes);
        let expire_observer_dbs = dbs.iter().map(Arc::downgrade).collect::<Vec<Weak<Db>>>();
        ttl_manager.set_expire_observer(Arc::new(move |db_index, key, type_tag, version| {
            if type_tag == TYPE_VECTOR
                && let Some(vector_runtimes) = weak_vector_runtimes.upgrade()
            {
                vector_runtimes.remove_expired(db_index, key, version);
            }
            let Some(db) = expire_observer_dbs
                .get(db_index as usize)
                .and_then(Weak::upgrade)
            else {
                return;
            };
            let result = match type_tag {
                TYPE_HASH => db.fulltext_observe_external_source_commit(
                    key,
                    crate::store::db::FullTextSourceType::Hash,
                ),
                TYPE_JSON => db.fulltext_observe_external_source_commit(
                    key,
                    crate::store::db::FullTextSourceType::Json,
                ),
                _ => Ok(()),
            };
            if let Err(err) = result {
                log::error!("failed to observe committed fulltext expiry for {key}: {err}");
            }
        }));

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
        let background_health = Arc::new(BackgroundTaskHealth::default());
        let fulltext_worker_shutdown = fulltext_shutdown.clone();
        let fulltext_worker_dbs = dbs.clone();
        let fulltext_health = Arc::clone(&background_health);
        let fulltext_task = spawn_background_task(
            "fulltext-maintenance",
            Arc::clone(&fulltext_shutdown),
            Arc::clone(&background_health),
            async move {
                while !fulltext_worker_shutdown.load(Ordering::Acquire) {
                    for db in &fulltext_worker_dbs {
                        if let Err(err) = db.fulltext_maintenance_tick_async().await {
                            fulltext_health.record_error(
                                "fulltext-maintenance",
                                format!("db={}: {err}", db.db_index()),
                            );
                            log::error!("fulltext maintenance failed db={}: {err}", db.db_index());
                        }
                    }
                    fulltext_health.record_success("fulltext-maintenance");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
        );

        let vector_worker_shutdown = fulltext_shutdown.clone();
        let vector_worker_dbs = dbs.clone();
        let vector_health = Arc::clone(&background_health);
        let vector_task = spawn_background_task(
            "vector-maintenance",
            Arc::clone(&fulltext_shutdown),
            Arc::clone(&background_health),
            async move {
                while !vector_worker_shutdown.load(Ordering::Acquire) {
                    for db in &vector_worker_dbs {
                        if let Err(err) = db.vector_maintenance_tick_async().await {
                            vector_health.record_error(
                                "vector-maintenance",
                                format!("db={}: {err}", db.db_index()),
                            );
                            log::error!("vector maintenance failed db={}: {err}", db.db_index());
                        }
                    }
                    vector_health.record_success("vector-maintenance");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
        );

        let version_scan_shutdown = Arc::new(AtomicBool::new(false));
        let version_scan_worker_shutdown = version_scan_shutdown.clone();
        let version_scan_worker_dbs = dbs.clone();
        let version_health = Arc::clone(&background_health);
        let version_scan_task = spawn_background_task(
            "version-compaction",
            Arc::clone(&version_scan_shutdown),
            Arc::clone(&background_health),
            async move {
                while !version_scan_worker_shutdown.load(Ordering::Acquire) {
                    for db in &version_scan_worker_dbs {
                        let retired = match db.refresh_retired_versions_for_compaction() {
                            Ok(retired) => retired,
                            Err(error) => {
                                version_health.record_error(
                                    "version-compaction",
                                    format!("db={}: {error}", db.db_index()),
                                );
                                log::warn!("version compaction refresh failed: {error}");
                                continue;
                            }
                        };
                        if retired > 0 {
                            log::debug!(
                                "marked {retired} retired version namespace(s) for compaction"
                            );
                        }
                    }
                    version_health.record_success("version-compaction");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            },
        );

        // Start background TTL sweeper
        let ttl_inner_task = ttl_manager.start_sweeper();
        let ttl_health = Arc::clone(&background_health);
        let ttl_task = spawn_background_task(
            "ttl-sweeper",
            Arc::clone(&fulltext_shutdown),
            Arc::clone(&background_health),
            async move {
                match ttl_inner_task.await {
                    Ok(()) => ttl_health.record_success("ttl-sweeper"),
                    Err(error) => ttl_health.record_error("ttl-sweeper", error.to_string()),
                }
            },
        );

        Ok(DatabaseManager {
            dbs,
            store,
            options,
            version_counter,
            ttl_manager,
            mutation_tracker,
            background_health,
            fulltext_shutdown,
            version_scan_shutdown,
            background_tasks: std::sync::Mutex::new(vec![
                fulltext_task,
                vector_task,
                version_scan_task,
                ttl_task,
            ]),
        })
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

    pub fn background_health(&self) -> &Arc<BackgroundTaskHealth> {
        &self.background_health
    }

    pub fn render_observability_prometheus(&self) -> Result<String, Error> {
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
        let mut vector_segments = 0;
        let mut vector_pending_segments = 0;
        let mut vector_hnsw_nodes = 0;
        let mut vector_hnsw_deleted_nodes = 0;

        let _ = writeln!(out, "# TYPE onedis_db_keys gauge");
        let _ = writeln!(out, "# TYPE onedis_db_expires gauge");
        let _ = writeln!(out, "# TYPE onedis_db_avg_ttl_milliseconds gauge");
        for (db_index, db) in self.dbs.iter().enumerate() {
            let ttl = db.ttl_observability_snapshot()?;
            expired_keys = ttl.expired_keys;
            ttl_stale_entries = ttl.stale_entries_skipped;
            ttl_sweep_cycles = ttl.sweep_cycles;
            let _ = writeln!(out, "onedis_db_keys{{db=\"{db_index}\"}} {}", db.len()?);
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

            let fulltext = db.fulltext_observability_snapshot()?;
            fulltext_creating += fulltext.creating;
            fulltext_backfilling += fulltext.backfilling;
            fulltext_ready += fulltext.ready;
            fulltext_dirty += fulltext.dirty;
            fulltext_rebuilding += fulltext.rebuilding;
            fulltext_dropping += fulltext.dropping;
            fulltext_outbox_pending += fulltext.outbox_pending;
            fulltext_backfill_pending += fulltext.backfill_pending;

            let stream = db.stream_observability_snapshot()?;
            stream_groups += stream.groups;
            stream_pending_entries += stream.pending_entries;

            let vector = db.vector_observability_snapshot()?;
            vector_indexes += vector.indexes;
            vector_segments += vector.segments;
            vector_pending_segments += vector.pending_segments;
            vector_hnsw_nodes += vector.hnsw_nodes;
            vector_hnsw_deleted_nodes += vector.hnsw_deleted_nodes;
        }
        let metrics = global_metrics();
        metrics.set_stream_snapshot(stream_groups, stream_pending_entries);
        metrics.set_vector_snapshot(
            vector_indexes,
            vector_segments,
            vector_pending_segments,
            vector_hnsw_nodes,
            vector_hnsw_deleted_nodes,
        );

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
        let _ = writeln!(out, "# TYPE onedis_background_task_running gauge");
        let _ = writeln!(out, "# TYPE onedis_background_task_failures_total counter");
        let _ = writeln!(
            out,
            "# TYPE onedis_background_task_last_success_milliseconds gauge"
        );
        for (task, state) in self.background_health.snapshot() {
            let _ = writeln!(
                out,
                "onedis_background_task_running{{task=\"{task}\"}} {}",
                u8::from(state.running)
            );
            let _ = writeln!(
                out,
                "onedis_background_task_failures_total{{task=\"{task}\"}} {}",
                state.failures
            );
            let _ = writeln!(
                out,
                "onedis_background_task_last_success_milliseconds{{task=\"{task}\"}} {}",
                state.last_success_ms
            );
        }
        self.render_storage_engine_properties(&mut out)?;
        Ok(out)
    }

    fn render_storage_engine_properties(&self, out: &mut String) -> Result<(), Error> {
        let _ = writeln!(out, "# TYPE onedis_storage_engine_property gauge");
        for property in STORAGE_ENGINE_PROPERTIES {
            let Some(value) = self.store.get_property(property)? else {
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
        Ok(())
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
        self.request_shutdown();
        let tasks = self
            .background_tasks
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for task in tasks.drain(..) {
            task.handle.abort();
        }
        for db in &self.dbs {
            db.shutdown_fulltext_runtime();
        }
    }
}
