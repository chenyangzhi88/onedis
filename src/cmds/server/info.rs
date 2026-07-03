use anyhow::Error;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    frame::Frame,
    observability::metrics::{CommandStatsSnapshot, global_metrics},
    store::db::Db,
};

pub struct Info {
    section: Option<String>,
}

impl Info {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        let section = if args.len() > 1 {
            Some(args[1].to_lowercase())
        } else {
            None
        };

        Ok(Info { section })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let info = self.generate_info(db, db.len());
        Ok(Frame::bulk_string(info))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let info = self.generate_info(db, db.len_async().await);
        Ok(Frame::bulk_string(info))
    }

    fn generate_info(&self, db: &Db, db_size: usize) -> String {
        let mut info = String::new();
        let metrics = global_metrics().snapshot();
        let ttl = db.ttl_observability_snapshot();

        // Default sections to show
        let show_all =
            self.section.is_none() || self.section.as_ref().map_or(false, |s| s == "all");
        let show_default =
            self.section.is_none() || self.section.as_ref().map_or(true, |s| s == "default");
        let show_server =
            show_all || show_default || self.section.as_ref().map_or(false, |s| s == "server");
        let show_clients =
            show_all || show_default || self.section.as_ref().map_or(false, |s| s == "clients");
        let show_memory =
            show_all || show_default || self.section.as_ref().map_or(false, |s| s == "memory");
        let show_persistence =
            show_all || show_default || self.section.as_ref().map_or(false, |s| s == "persistence");
        let show_stats =
            show_all || show_default || self.section.as_ref().map_or(false, |s| s == "stats");
        let show_replication =
            show_all || show_default || self.section.as_ref().map_or(false, |s| s == "replication");
        let show_cpu =
            show_all || show_default || self.section.as_ref().map_or(false, |s| s == "cpu");
        let show_commandstats = show_all
            || show_default
            || self.section.as_ref().map_or(false, |s| s == "commandstats");
        let show_keyspace =
            show_all || show_default || self.section.as_ref().map_or(false, |s| s == "keyspace");

        // Server section
        if show_server {
            info.push_str("# Server\r\n");
            info.push_str("redis_version:0.1.0\r\n");
            info.push_str("redis_git_sha1:00000000\r\n");
            info.push_str("redis_git_dirty:0\r\n");
            info.push_str("redis_build_id:unknown\r\n");
            info.push_str("redis_mode:standalone\r\n");
            info.push_str("os:Rust\r\n");
            info.push_str("arch_bits:64\r\n");
            info.push_str("multiplexing_api:unknown\r\n");
            info.push_str("gcc_version:0.0.0\r\n");
            info.push_str("process_id:0\r\n");

            // Calculate uptime
            if let Ok(startup_time) = SystemTime::now().duration_since(UNIX_EPOCH) {
                let uptime = startup_time.as_secs();
                info.push_str(&format!("uptime_in_seconds:{}\r\n", uptime));
                info.push_str(&format!("uptime_in_days:{}\r\n", uptime / 86400));
            }

            info.push_str("hz:10\r\n");
            info.push_str("configured_hz:10\r\n");
            info.push_str("lru_clock:0\r\n");
            info.push_str("executable:/rudis-server\r\n");
            info.push_str("config_file:config/onedis.toml\r\n\r\n");
        }

        // Clients section
        if show_clients {
            info.push_str("# Clients\r\n");
            info.push_str("connected_clients:1\r\n");
            info.push_str("client_recent_max_input_buffer:0\r\n");
            info.push_str("client_recent_max_output_buffer:0\r\n");
            info.push_str("blocked_clients:0\r\n");
            info.push_str("tracking_clients:0\r\n");
            info.push_str("clients_in_timeout_table:0\r\n\r\n");
        }

        // Memory section
        if show_memory {
            info.push_str("# Memory\r\n");
            // Estimate memory usage based on the number of records
            let memory_used = db_size * 100; // Rough estimate
            info.push_str(&format!("used_memory:{}\r\n", memory_used));
            info.push_str(&format!("used_memory_human:{}B\r\n", memory_used));
            info.push_str("used_memory_rss:0\r\n");
            info.push_str("used_memory_peak:0\r\n");
            info.push_str("used_memory_peak_human:0B\r\n");
            info.push_str("used_memory_lua:0\r\n");
            info.push_str("used_memory_lua_human:0B\r\n");
            info.push_str("maxmemory:0\r\n");
            info.push_str("maxmemory_human:0B\r\n");
            info.push_str("maxmemory_policy:noeviction\r\n");
            info.push_str("mem_fragmentation_ratio:0.00\r\n");
            info.push_str("mem_allocator:jemalloc-0.0.0\r\n\r\n");
        }

        // Persistence section
        if show_persistence {
            info.push_str("# Persistence\r\n");
            info.push_str("persistence_enabled:0\r\n");
            info.push_str("loading:0\r\n");
            info.push_str("rdb_changes_since_last_save:0\r\n");
            info.push_str("rdb_bgsave_in_progress:0\r\n");
            info.push_str("rdb_last_save_time:0\r\n");
            info.push_str("rdb_last_bgsave_status:disabled\r\n");
            info.push_str("rdb_last_bgsave_time_sec:-1\r\n");
            info.push_str("rdb_current_bgsave_time_sec:-1\r\n");
            info.push_str("rdb_last_cow_size:0\r\n");
            info.push_str("aof_enabled:0\r\n");
            info.push_str("aof_rewrite_in_progress:0\r\n");
            info.push_str("aof_rewrite_scheduled:0\r\n");
            info.push_str("aof_last_rewrite_time_sec:-1\r\n");
            info.push_str("aof_current_rewrite_time_sec:-1\r\n");
            info.push_str("aof_last_bgrewrite_status:disabled\r\n");
            info.push_str("aof_last_write_status:disabled\r\n");
            info.push_str("aof_last_cow_size:0\r\n");
            info.push_str("module_fork_in_progress:0\r\n");
            info.push_str("module_fork_last_cow_size:0\r\n\r\n");
        }

        // Stats section
        if show_stats {
            info.push_str("# Stats\r\n");
            info.push_str(&format!(
                "total_connections_received:{}\r\n",
                metrics.total_connections_received
            ));
            info.push_str(&format!(
                "total_commands_processed:{}\r\n",
                metrics.total_commands_processed
            ));
            info.push_str("instantaneous_ops_per_sec:0\r\n");
            info.push_str(&format!(
                "total_net_input_bytes:{}\r\n",
                metrics.total_net_input_bytes
            ));
            info.push_str(&format!(
                "total_net_output_bytes:{}\r\n",
                metrics.total_net_output_bytes
            ));
            info.push_str("instantaneous_input_kbps:0.00\r\n");
            info.push_str("instantaneous_output_kbps:0.00\r\n");
            info.push_str(&format!(
                "rejected_connections:{}\r\n",
                metrics.rejected_connections
            ));
            info.push_str("sync_full:0\r\n");
            info.push_str("sync_partial_ok:0\r\n");
            info.push_str("sync_partial_err:0\r\n");
            info.push_str(&format!("expired_keys:{}\r\n", ttl.expired_keys));
            info.push_str("expired_stale_perc:0.00\r\n");
            info.push_str("expired_time_cap_reached_count:0\r\n");
            info.push_str("expire_cycle_cpu_milliseconds:0\r\n");
            info.push_str("evicted_keys:0\r\n");
            info.push_str("keyspace_hits:0\r\n");
            info.push_str("keyspace_misses:0\r\n");
            info.push_str("pubsub_channels:0\r\n");
            info.push_str("pubsub_patterns:0\r\n");
            info.push_str("latest_fork_usec:0\r\n");
            info.push_str("total_forks:0\r\n");
            info.push_str("migrate_cached_sockets:0\r\n");
            info.push_str("slave_expires_tracked_keys:0\r\n");
            info.push_str("active_defrag_hits:0\r\n");
            info.push_str("active_defrag_misses:0\r\n");
            info.push_str("active_defrag_key_hits:0\r\n");
            info.push_str("active_defrag_key_misses:0\r\n");
            info.push_str("tracking_total_keys:0\r\n");
            info.push_str("tracking_total_items:0\r\n");
            info.push_str("tracking_total_prefixes:0\r\n");
            info.push_str(&format!(
                "unexpected_error_replies:{}\r\n",
                metrics.total_command_errors
            ));
            info.push_str("total_reads_processed:0\r\n");
            info.push_str("total_writes_processed:0\r\n");
            info.push_str("io_threaded_reads_processed:0\r\n");
            info.push_str("io_threaded_writes_processed:0\r\n\r\n");
        }

        // Replication section
        if show_replication {
            info.push_str("# Replication\r\n");
            info.push_str("role:master\r\n");
            info.push_str("connected_slaves:0\r\n");
            info.push_str("master_replid:0000000000000000000000000000000000000000\r\n");
            info.push_str("master_replid2:0000000000000000000000000000000000000000\r\n");
            info.push_str("master_repl_offset:0\r\n");
            info.push_str("second_repl_offset:-1\r\n");
            info.push_str("repl_backlog_active:0\r\n");
            info.push_str("repl_backlog_size:1048576\r\n");
            info.push_str("repl_backlog_first_byte_offset:0\r\n");
            info.push_str("repl_backlog_histlen:0\r\n\r\n");
        }

        // CPU section
        if show_cpu {
            info.push_str("# CPU\r\n");
            info.push_str("used_cpu_sys:0.000000\r\n");
            info.push_str("used_cpu_user:0.000000\r\n");
            info.push_str("used_cpu_sys_children:0.000000\r\n");
            info.push_str("used_cpu_user_children:0.000000\r\n");
            info.push_str("used_cpu_sys_main_thread:0.000000\r\n");
            info.push_str("used_cpu_user_main_thread:0.000000\r\n\r\n");
        }

        // Commandstats section
        if show_commandstats {
            info.push_str("# Commandstats\r\n");
            for command in metrics
                .command_stats
                .iter()
                .filter(|command| command.calls > 0 || command.name == "INFO")
            {
                push_command_stat(&mut info, command);
            }
            info.push_str("\r\n");
        }

        // Keyspace section
        if show_keyspace {
            info.push_str("# Keyspace\r\n");
            info.push_str(&format!(
                "db0:keys={},expires={},avg_ttl={}\r\n",
                db_size, ttl.expires, ttl.avg_ttl_millis
            ));
        }

        info
    }
}

fn push_command_stat(info: &mut String, command: &CommandStatsSnapshot) {
    let usec_per_call = if command.calls == 0 {
        0.0
    } else {
        command.usec as f64 / command.calls as f64
    };
    info.push_str(&format!(
        "cmdstat_{}:calls={},usec={},usec_per_call={:.2}\r\n",
        command.name.to_ascii_lowercase().replace('.', "_"),
        command.calls,
        command.usec,
        usec_per_call
    ));
}

#[cfg(test)]
mod tests {
    use super::Info;
    use crate::command::Command;
    use crate::frame::Frame;
    use crate::store::db::Db;
    use crate::store::kv_store::KvStore;
    use crate::store::ttl::{TtlConfig, TtlManager, VersionCounter};
    use std::sync::Arc;

    fn test_db() -> Db {
        let unique = format!(
            "onedis-info-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("target/onedis-test-data"))
            .join(unique);
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = KvStore::new(db_path, wal_dir, 1);
        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        Db::new(0, store, version_counter, ttl_manager)
    }

    fn command(args: &[&str]) -> Info {
        let frame = Frame::Array(
            args.iter()
                .map(|arg| Frame::bulk_string((*arg).to_string()))
                .collect(),
        );
        match Command::parse_from_frame(frame).unwrap() {
            Command::Info(info) => info,
            other => panic!("expected INFO, got {}", other.name()),
        }
    }

    fn bulk_text(frame: Frame) -> String {
        match frame {
            Frame::BulkString(bytes) => String::from_utf8(bytes).unwrap(),
            other => panic!("expected bulk string, got {}", other.to_string()),
        }
    }

    #[test]
    fn info_default_all_specific_and_unknown_sections_are_rendered() {
        let db = test_db();
        db.insert_string_ref("k1", "v1");
        db.insert_string_ref("k2", "v2");

        let default_info = bulk_text(command(&["info"]).apply(&db).unwrap());
        for section in [
            "# Server",
            "# Clients",
            "# Memory",
            "# Persistence",
            "# Stats",
            "# Replication",
            "# CPU",
            "# Commandstats",
            "# Keyspace",
        ] {
            assert!(default_info.contains(section), "missing {section}");
        }
        assert!(default_info.contains("db0:keys=2,expires=0,avg_ttl=0"));
        assert!(default_info.contains("used_memory:200"));

        let all_info = bulk_text(command(&["info", "all"]).apply(&db).unwrap());
        assert!(all_info.contains("master_replid:"));
        assert!(all_info.contains("cmdstat_info:calls="));

        let server_info = bulk_text(command(&["info", "server"]).apply(&db).unwrap());
        assert!(server_info.contains("# Server"));
        assert!(!server_info.contains("# Clients"));

        let memory_info = bulk_text(command(&["info", "memory"]).apply(&db).unwrap());
        assert!(memory_info.contains("# Memory"));
        assert!(!memory_info.contains("# Server"));

        let keyspace_info = bulk_text(command(&["info", "keyspace"]).apply(&db).unwrap());
        assert_eq!(
            keyspace_info.trim(),
            "# Keyspace\r\ndb0:keys=2,expires=0,avg_ttl=0"
        );

        let unknown_info = bulk_text(command(&["info", "unknown-section"]).apply(&db).unwrap());
        assert!(unknown_info.is_empty());
    }

    #[tokio::test]
    async fn info_async_uses_async_db_size() {
        let db = test_db();
        db.insert_string_ref("async-key", "value");

        let info = bulk_text(
            command(&["info", "keyspace"])
                .apply_async(&db)
                .await
                .unwrap(),
        );
        assert!(info.contains("db0:keys=1"));
    }
}
