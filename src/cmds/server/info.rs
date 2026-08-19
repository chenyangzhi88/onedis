use crate::{
    frame::Frame,
    observability::metrics::{CommandStatsSnapshot, global_metrics},
    store::db::Db,
};
use anyhow::Error;

pub struct Info {
    section: Option<String>,
}

impl Info {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() > 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'info' command",
            ));
        }

        let section = if frame.arg_len() > 1 {
            Some(
                frame
                    .get_arg(1)
                    .ok_or_else(|| Error::msg("ERR invalid UTF-8 INFO section"))?
                    .to_lowercase(),
            )
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
        let show_all = self.section.is_none() || self.section.as_ref().is_some_and(|s| s == "all");
        let show_default =
            self.section.is_none() || self.section.as_ref().is_none_or(|s| s == "default");
        let show_server =
            show_all || show_default || self.section.as_ref().is_some_and(|s| s == "server");
        let show_clients =
            show_all || show_default || self.section.as_ref().is_some_and(|s| s == "clients");
        let show_memory =
            show_all || show_default || self.section.as_ref().is_some_and(|s| s == "memory");
        let show_persistence =
            show_all || show_default || self.section.as_ref().is_some_and(|s| s == "persistence");
        let show_stats =
            show_all || show_default || self.section.as_ref().is_some_and(|s| s == "stats");
        let show_replication =
            show_all || show_default || self.section.as_ref().is_some_and(|s| s == "replication");
        let show_cpu =
            show_all || show_default || self.section.as_ref().is_some_and(|s| s == "cpu");
        let show_commandstats =
            show_all || show_default || self.section.as_ref().is_some_and(|s| s == "commandstats");
        let show_keyspace =
            show_all || show_default || self.section.as_ref().is_some_and(|s| s == "keyspace");

        // Server section
        if show_server {
            info.push_str("# Server\r\n");
            info.push_str(&format!("redis_version:{}\r\n", env!("CARGO_PKG_VERSION")));
            info.push_str(&format!("onedis_version:{}\r\n", env!("CARGO_PKG_VERSION")));
            info.push_str("redis_mode:standalone\r\n");
            info.push_str(&format!("os:{}\r\n", std::env::consts::OS));
            info.push_str(&format!("arch:{}\r\n", std::env::consts::ARCH));
            info.push_str(&format!("arch_bits:{}\r\n", usize::BITS));
            info.push_str("multiplexing_api:tokio\r\n");
            info.push_str(&format!("process_id:{}\r\n", std::process::id()));
            info.push_str(&format!("uptime_in_seconds:{}\r\n", metrics.uptime_seconds));
            info.push_str(&format!(
                "uptime_in_days:{}\r\n",
                metrics.uptime_seconds / 86400
            ));
            info.push_str("executable:onedis-server\r\n\r\n");
        }

        // Clients section
        if show_clients {
            info.push_str("# Clients\r\n");
            info.push_str(&format!(
                "connected_clients:{}\r\n",
                metrics.current_connections
            ));
            info.push_str(&format!(
                "maxclients:{}\r\n\r\n",
                metrics.configured_maxclients
            ));
        }

        // Memory section
        if show_memory {
            info.push_str("# Memory\r\n");
            let memory_used = process_resident_memory_bytes().unwrap_or(0);
            info.push_str(&format!("used_memory:{}\r\n", memory_used));
            info.push_str(&format!("used_memory_human:{}B\r\n", memory_used));
            info.push_str(&format!("used_memory_rss:{}\r\n", memory_used));
            info.push_str("mem_allocator:system\r\n");
            info.push_str("onedis_memory_measurement:process_rss\r\n\r\n");
        }

        // Persistence section
        if show_persistence {
            info.push_str("# Persistence\r\n");
            info.push_str("persistence_enabled:1\r\n");
            info.push_str("storage_engine:kv-engine\r\n");
            info.push_str("redis_rdb_supported:0\r\n");
            info.push_str("redis_aof_supported:0\r\n");
            info.push_str("loading:0\r\n");
            info.push_str("aof_enabled:0\r\n");
            info.push_str("rdb_last_bgsave_status:unsupported\r\n\r\n");
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
            info.push_str(&format!(
                "total_net_input_bytes:{}\r\n",
                metrics.total_net_input_bytes
            ));
            info.push_str(&format!(
                "total_net_output_bytes:{}\r\n",
                metrics.total_net_output_bytes
            ));
            info.push_str(&format!(
                "rejected_connections:{}\r\n",
                metrics.rejected_connections
            ));
            info.push_str(&format!("expired_keys:{}\r\n", ttl.expired_keys));
            info.push_str(&format!(
                "unexpected_error_replies:{}\r\n",
                metrics.total_command_errors
            ));
            info.push_str("\r\n");
        }

        // Replication section
        if show_replication {
            info.push_str("# Replication\r\n");
            info.push_str("redis_replication_supported:0\r\n");
            info.push_str("onedis_storage_role:standalone\r\n\r\n");
        }

        // CPU section
        if show_cpu {
            info.push_str("# CPU\r\n");
            info.push_str("cpu_measurement_supported:0\r\n\r\n");
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
                "db{}:keys={},expires={},avg_ttl={}\r\n",
                db.db_index(),
                db_size,
                ttl.expires,
                ttl.avg_ttl_millis
            ));
        }

        info
    }
}

fn process_resident_memory_bytes() -> Option<usize> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let rss_kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse::<usize>()
        .ok()?;
    rss_kib.checked_mul(1024)
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
            other => panic!("expected bulk string, got {}", other),
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
        assert!(default_info.contains("onedis_memory_measurement:process_rss"));

        let all_info = bulk_text(command(&["info", "all"]).apply(&db).unwrap());
        assert!(all_info.contains("redis_replication_supported:0"));
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
