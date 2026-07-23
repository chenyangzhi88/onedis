use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use common::types::options::FileConfig;
use onedis_server::store::db::{Db, KeyMutationTracker};
use onedis_server::store::kv_store::KvStore;
use onedis_server::store::ttl::{TtlConfig, TtlManager, VersionCounter};

#[derive(Clone, Copy, Debug)]
enum Mode {
    Set,
    Lpush,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let config = value_arg(&args, "--config").unwrap_or("benchmarks/onedis-pressure-20260602.toml");
    let mode = match value_arg(&args, "--mode")
        .unwrap_or("set")
        .to_ascii_lowercase()
        .as_str()
    {
        "set" => Mode::Set,
        "lpush" => Mode::Lpush,
        other => panic!("unknown --mode {other}; expected set or lpush"),
    };
    let ops = value_arg(&args, "--ops")
        .unwrap_or("100000")
        .parse::<u64>()
        .expect("invalid --ops");
    let threads = value_arg(&args, "--threads")
        .unwrap_or("50")
        .parse::<usize>()
        .expect("invalid --threads")
        .max(1);
    let keyspace = value_arg(&args, "--keyspace")
        .unwrap_or("100000")
        .parse::<u64>()
        .expect("invalid --keyspace")
        .max(1);
    let value_size = value_arg(&args, "--value-size")
        .unwrap_or("3")
        .parse::<usize>()
        .expect("invalid --value-size");
    let key_prefix = value_arg(&args, "--key-prefix").unwrap_or(match mode {
        Mode::Set => "bench:set",
        Mode::Lpush => "bench:lpush",
    });
    let use_async = bool_arg(&args, "--async");

    let mut options = FileConfig::load_from_path(Path::new(config))
        .and_then(FileConfig::into_options)
        .expect("failed to load config");
    if let Some(root) = value_arg(&args, "--db-root") {
        let root = PathBuf::from(root);
        options.db_path = root.join("db");
        options.wal_dir = root.join("wal");
    }
    if let Some(size_bytes) = value_arg(&args, "--memtable-size-bytes") {
        options.memtable_size_bytes = size_bytes
            .parse::<usize>()
            .expect("invalid --memtable-size-bytes")
            .max(1);
    }
    std::fs::create_dir_all(&options.db_path).expect("failed to create db dir");
    std::fs::create_dir_all(&options.wal_dir).expect("failed to create wal dir");

    let store = KvStore::open(options);
    let stats_store = store.clone();
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    let db = Arc::new(Db::new_with_mutation_tracker(
        0,
        store,
        version_counter,
        ttl_manager,
        Arc::new(KeyMutationTracker::default()),
    ));

    let started = Instant::now();
    let mut handles = Vec::with_capacity(threads);
    for thread_idx in 0..threads {
        let db = db.clone();
        let value = vec![b'x'; value_size];
        let thread_ops =
            ops / threads as u64 + u64::from((thread_idx as u64) < ops % threads as u64);
        let key_prefix = key_prefix.to_string();
        handles.push(tokio::spawn(async move {
            let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ thread_idx as u64;
            for _ in 0..thread_ops {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = format!("{key_prefix}:{:08}", rng % keyspace);
                if use_async {
                    match mode {
                        Mode::Set => db.insert_string_bytes_refs_async(&[(&key, &value)]).await,
                        Mode::Lpush => {
                            db.list_push_left_bytes_async(&key, &[value.as_slice()], false)
                                .await
                                .expect("lpush failed");
                        }
                    }
                } else {
                    match mode {
                        Mode::Set => db.insert_string_bytes_ref(&key, &value),
                        Mode::Lpush => {
                            db.list_push_left_bytes(&key, &[value.as_slice()], false)
                                .expect("lpush failed");
                        }
                    }
                }
            }
        }));
    }
    for handle in handles {
        handle.await.expect("worker task panicked");
    }
    let elapsed = started.elapsed();
    println!(
        "mode={mode:?} ops={ops} threads={threads} elapsed={:.3}s rps={:.2} avg_us={:.2}",
        elapsed.as_secs_f64(),
        ops as f64 / elapsed.as_secs_f64(),
        elapsed.as_secs_f64() * 1_000_000.0 / ops as f64
    );
    if let Ok(Some(stats)) = stats_store.get_property("db.write-thread-stats") {
        println!("write_thread_stats {stats}");
    }
    for property in [
        "db.get-stats",
        "db.get-detail-stats",
        "db.read-path-detail-stats",
        "db.memtable-lifecycle-stats",
    ] {
        if let Ok(Some(stats)) = stats_store.get_property(property) {
            println!("{property} {stats}");
        }
    }
}

fn value_arg<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.windows(2)
        .find_map(|window| (window[0] == name).then_some(window[1].as_str()))
}

fn bool_arg(args: &[String], name: &str) -> bool {
    args.iter()
        .position(|arg| arg == name)
        .is_some_and(|index| {
            args.get(index + 1).is_none_or(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
        })
}
