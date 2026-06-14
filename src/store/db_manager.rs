use std::{
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::sync::Notify;

use crate::{
    args::ResolvedArgs,
    store::db::{Db, KeyMutationTracker},
    store::kv_store::KvStore,
    store::ttl::{TYPE_HASH, TYPE_JSON, TtlConfig, TtlManager, VersionCounter},
};
use common::types::options::{FileConfig, Options};

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

        // Start background TTL sweeper
        ttl_manager.start_sweeper();

        DatabaseManager {
            dbs,
            store,
            options,
            version_counter,
            ttl_manager,
            fulltext_shutdown,
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
}

impl Drop for DatabaseManager {
    fn drop(&mut self) {
        self.fulltext_shutdown.store(true, Ordering::Release);
        self.ttl_manager.shutdown();
    }
}
