use std::{
    collections::HashMap,
    fmt::Write,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

const DURATION_BUCKETS_US: [u64; 10] = [
    100,
    500,
    1_000,
    5_000,
    10_000,
    50_000,
    100_000,
    500_000,
    1_000_000,
    u64::MAX,
];

const ERROR_CLASSES: [&str; 9] = [
    "parse_error",
    "wrong_type",
    "wrong_arity",
    "noauth",
    "noperm",
    "timeout",
    "storage_error",
    "internal_error",
    "unsupported",
];

const REJECTION_REASONS: [&str; 5] = [
    "noauth",
    "noperm",
    "parse_error",
    "maxclients",
    "transaction_state",
];

const COMMAND_NAMES: &[&str] = &[
    "APPEND",
    "AUTH",
    "BGSAVE",
    "BITCOUNT",
    "BITFIELD",
    "BITOP",
    "BITPOS",
    "BLMOVE",
    "BLMPOP",
    "BLPOP",
    "BRPOP",
    "BRPOPLPUSH",
    "BZMPOP",
    "BZPOPMAX",
    "BZPOPMIN",
    "CLIENT",
    "CONFIG",
    "COPY",
    "DBSIZE",
    "DECR",
    "DECRBY",
    "DEL",
    "DISCARD",
    "ECHO",
    "EXEC",
    "EXISTS",
    "EXPIRE",
    "EXPIREAT",
    "EXPIRETIME",
    "FT._LIST",
    "FT.AGGREGATE",
    "FT.ALTER",
    "FT.ALIASADD",
    "FT.ALIASDEL",
    "FT.ALIASUPDATE",
    "FT.CONFIG",
    "FT.CREATE",
    "FT.CURSOR",
    "FT.DICT",
    "FT.DROPINDEX",
    "FT.EXPLAIN",
    "FT.HYBRID",
    "FT.INFO",
    "FT.PROFILE",
    "FT.SEARCH",
    "FT.SPELLCHECK",
    "FT.SUG",
    "FT.SYN",
    "FT.TAGVALS",
    "FT.UNSUPPORTED",
    "GEOADD",
    "GEODIST",
    "GEOHASH",
    "GEOPOS",
    "GEORADIUS",
    "GEORADIUSBYMEMBER",
    "GEOSEARCH",
    "GEOSEARCHSTORE",
    "GET",
    "GETBIT",
    "GETDEL",
    "GETEX",
    "GETRANGE",
    "GETSET",
    "HDEL",
    "HEXISTS",
    "HEXPIRE",
    "HEXPIREAT",
    "HEXPIRETIME",
    "HGET",
    "HGETALL",
    "HGETDEL",
    "HGETEX",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HMSET",
    "HPERSIST",
    "HPEXPIRE",
    "HPEXPIREAT",
    "HPEXPIRETIME",
    "HPTTL",
    "HRANDFIELD",
    "HSCAN",
    "HSET",
    "HSETEX",
    "HSETNX",
    "HSTRLEN",
    "HTTL",
    "HVALS",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "INFO",
    "JSON.DEL",
    "JSON.GET",
    "JSON.SET",
    "JSON.TYPE",
    "KEYS",
    "LCS",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LMOVE",
    "LMPOP",
    "LOLWUT",
    "LPOP",
    "LPOS",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "LUA",
    "MGET",
    "MOVE",
    "MSET",
    "MSETEX",
    "MSETNX",
    "MULTI",
    "PFADD",
    "PFCOUNT",
    "PFMERGE",
    "PEXPIRE",
    "PEXPIREAT",
    "PEXPIRETIME",
    "PING",
    "PSETEX",
    "PTTL",
    "RANDOMKEY",
    "RENAME",
    "RENAMENX",
    "RPOP",
    "RPOPLPUSH",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SAVE",
    "SCAN",
    "SCARD",
    "SDIFF",
    "SDIFFSTORE",
    "SELECT",
    "SET",
    "SETBIT",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "SINTER",
    "SINTERCARD",
    "SINTERSTORE",
    "SISMEMBER",
    "SMEMBERS",
    "SMISMEMBER",
    "SMOVE",
    "SPOP",
    "SRANDMEMBER",
    "SREM",
    "SSCAN",
    "STRLEN",
    "SUNION",
    "SUNIONSTORE",
    "TOUCH",
    "TTL",
    "TYPE",
    "UNKNOWN",
    "UNLINK",
    "UNWATCH",
    "VADD",
    "VCARD",
    "VDIM",
    "VEMB",
    "VGETATTR",
    "VINFO",
    "VLINKS",
    "VRANDMEMBER",
    "VREM",
    "VSETATTR",
    "VSIM",
    "WASM",
    "WATCH",
    "XACK",
    "XACKDEL",
    "XADD",
    "XAUTOCLAIM",
    "XCFGSET",
    "XCLAIM",
    "XDEL",
    "XDELEX",
    "XGROUP",
    "XINFO",
    "XLEN",
    "XPENDING",
    "XRANGE",
    "XREAD",
    "XREADGROUP",
    "XREVRANGE",
    "XSETID",
    "XTRIM",
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZDIFF",
    "ZDIFFSTORE",
    "ZINCRBY",
    "ZINTER",
    "ZINTERCARD",
    "ZINTERSTORE",
    "ZLEXCOUNT",
    "ZMPOP",
    "ZMSCORE",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANDMEMBER",
    "ZRANGE",
    "ZRANGEBYLEX",
    "ZRANGEBYSCORE",
    "ZRANGESTORE",
    "ZRANK",
    "ZREM",
    "ZREMRANGEBYLEX",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    "ZREVRANGE",
    "ZREVRANGEBYLEX",
    "ZREVRANGEBYSCORE",
    "ZREVRANK",
    "ZSCAN",
    "ZSCORE",
    "ZUNION",
    "ZUNIONSTORE",
];

pub fn global_metrics() -> Arc<OnedisMetrics> {
    static METRICS: OnceLock<Arc<OnedisMetrics>> = OnceLock::new();
    METRICS
        .get_or_init(|| Arc::new(OnedisMetrics::new()))
        .clone()
}

pub struct OnedisMetrics {
    enabled: AtomicBool,
    started: Instant,
    config_databases: AtomicU64,
    config_maxclients: AtomicU64,
    connections_current: AtomicU64,
    connections_total: AtomicU64,
    connections_rejected: AtomicU64,
    connections_closed: AtomicU64,
    net_input_bytes: AtomicU64,
    net_output_bytes: AtomicU64,
    resp_parse_errors: AtomicU64,
    protocol_frames: AtomicU64,
    pipeline_commands: AtomicU64,
    pipeline_batch_buckets: Vec<AtomicU64>,
    command_rejections: Vec<AtomicU64>,
    commands: Vec<CommandMetrics>,
    storage_reads: AtomicU64,
    storage_writes: AtomicU64,
    storage_write_errors: AtomicU64,
    storage_read_duration_sum_us: AtomicU64,
    storage_read_duration_count: AtomicU64,
    storage_read_duration_buckets: Vec<AtomicU64>,
    storage_write_duration_sum_us: AtomicU64,
    storage_write_duration_count: AtomicU64,
    storage_write_duration_buckets: Vec<AtomicU64>,
    ttl_sweep_duration_sum_us: AtomicU64,
    ttl_sweep_duration_count: AtomicU64,
    ttl_sweep_duration_buckets: Vec<AtomicU64>,
    fulltext_refresh_requests: AtomicU64,
    fulltext_refresh_errors: AtomicU64,
    fulltext_refresh_duration_sum_us: AtomicU64,
    fulltext_refresh_duration_count: AtomicU64,
    fulltext_refresh_duration_buckets: Vec<AtomicU64>,
    fulltext_search_total: AtomicU64,
    fulltext_search_duration_sum_us: AtomicU64,
    fulltext_search_duration_count: AtomicU64,
    fulltext_search_duration_buckets: Vec<AtomicU64>,
    stream_groups: AtomicU64,
    stream_pending_entries: AtomicU64,
    stream_claims: AtomicU64,
    stream_autoclaims: AtomicU64,
    stream_reads: AtomicU64,
    stream_blocked_clients: AtomicU64,
    vector_indexes: AtomicU64,
    vector_writes: AtomicU64,
    vector_search_total: AtomicU64,
    vector_search_errors: AtomicU64,
    vector_search_duration_sum_us: AtomicU64,
    vector_search_duration_count: AtomicU64,
    vector_search_duration_buckets: Vec<AtomicU64>,
    lua_eval_total: AtomicU64,
    lua_eval_errors: AtomicU64,
    lua_eval_duration_sum_us: AtomicU64,
    lua_eval_duration_count: AtomicU64,
    lua_eval_duration_buckets: Vec<AtomicU64>,
    wasm_calls_total: AtomicU64,
    wasm_errors: AtomicU64,
    wasm_duration_sum_us: AtomicU64,
    wasm_duration_count: AtomicU64,
    wasm_duration_buckets: Vec<AtomicU64>,
}

#[derive(Clone, Debug)]
pub struct MetricsSnapshot {
    pub total_connections_received: u64,
    pub total_commands_processed: u64,
    pub total_command_errors: u64,
    pub total_net_input_bytes: u64,
    pub total_net_output_bytes: u64,
    pub rejected_connections: u64,
    pub command_stats: Vec<CommandStatsSnapshot>,
}

#[derive(Clone, Debug)]
pub struct CommandStatsSnapshot {
    pub name: &'static str,
    pub calls: u64,
    pub errors: u64,
    pub usec: u64,
}

struct CommandMetrics {
    calls: AtomicU64,
    errors: Vec<AtomicU64>,
    slow: AtomicU64,
    duration_sum_us: AtomicU64,
    duration_count: AtomicU64,
    duration_buckets: Vec<AtomicU64>,
}

impl OnedisMetrics {
    fn new() -> Self {
        Self {
            enabled: AtomicBool::new(true),
            started: Instant::now(),
            config_databases: AtomicU64::new(0),
            config_maxclients: AtomicU64::new(0),
            connections_current: AtomicU64::new(0),
            connections_total: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            connections_closed: AtomicU64::new(0),
            net_input_bytes: AtomicU64::new(0),
            net_output_bytes: AtomicU64::new(0),
            resp_parse_errors: AtomicU64::new(0),
            protocol_frames: AtomicU64::new(0),
            pipeline_commands: AtomicU64::new(0),
            pipeline_batch_buckets: zeroed_vec(6),
            command_rejections: zeroed_vec(REJECTION_REASONS.len()),
            commands: (0..COMMAND_NAMES.len())
                .map(|_| CommandMetrics {
                    calls: AtomicU64::new(0),
                    errors: zeroed_vec(ERROR_CLASSES.len()),
                    slow: AtomicU64::new(0),
                    duration_sum_us: AtomicU64::new(0),
                    duration_count: AtomicU64::new(0),
                    duration_buckets: zeroed_vec(DURATION_BUCKETS_US.len()),
                })
                .collect(),
            storage_reads: AtomicU64::new(0),
            storage_writes: AtomicU64::new(0),
            storage_write_errors: AtomicU64::new(0),
            storage_read_duration_sum_us: AtomicU64::new(0),
            storage_read_duration_count: AtomicU64::new(0),
            storage_read_duration_buckets: zeroed_vec(DURATION_BUCKETS_US.len()),
            storage_write_duration_sum_us: AtomicU64::new(0),
            storage_write_duration_count: AtomicU64::new(0),
            storage_write_duration_buckets: zeroed_vec(DURATION_BUCKETS_US.len()),
            ttl_sweep_duration_sum_us: AtomicU64::new(0),
            ttl_sweep_duration_count: AtomicU64::new(0),
            ttl_sweep_duration_buckets: zeroed_vec(DURATION_BUCKETS_US.len()),
            fulltext_refresh_requests: AtomicU64::new(0),
            fulltext_refresh_errors: AtomicU64::new(0),
            fulltext_refresh_duration_sum_us: AtomicU64::new(0),
            fulltext_refresh_duration_count: AtomicU64::new(0),
            fulltext_refresh_duration_buckets: zeroed_vec(DURATION_BUCKETS_US.len()),
            fulltext_search_total: AtomicU64::new(0),
            fulltext_search_duration_sum_us: AtomicU64::new(0),
            fulltext_search_duration_count: AtomicU64::new(0),
            fulltext_search_duration_buckets: zeroed_vec(DURATION_BUCKETS_US.len()),
            stream_groups: AtomicU64::new(0),
            stream_pending_entries: AtomicU64::new(0),
            stream_claims: AtomicU64::new(0),
            stream_autoclaims: AtomicU64::new(0),
            stream_reads: AtomicU64::new(0),
            stream_blocked_clients: AtomicU64::new(0),
            vector_indexes: AtomicU64::new(0),
            vector_writes: AtomicU64::new(0),
            vector_search_total: AtomicU64::new(0),
            vector_search_errors: AtomicU64::new(0),
            vector_search_duration_sum_us: AtomicU64::new(0),
            vector_search_duration_count: AtomicU64::new(0),
            vector_search_duration_buckets: zeroed_vec(DURATION_BUCKETS_US.len()),
            lua_eval_total: AtomicU64::new(0),
            lua_eval_errors: AtomicU64::new(0),
            lua_eval_duration_sum_us: AtomicU64::new(0),
            lua_eval_duration_count: AtomicU64::new(0),
            lua_eval_duration_buckets: zeroed_vec(DURATION_BUCKETS_US.len()),
            wasm_calls_total: AtomicU64::new(0),
            wasm_errors: AtomicU64::new(0),
            wasm_duration_sum_us: AtomicU64::new(0),
            wasm_duration_count: AtomicU64::new(0),
            wasm_duration_buckets: zeroed_vec(DURATION_BUCKETS_US.len()),
        }
    }

    pub fn initialize_command_index(&self) {
        let _ = command_index();
    }

    pub fn configure(&self, databases: usize, maxclients: usize) {
        self.config_databases
            .store(databases as u64, Ordering::Relaxed);
        self.config_maxclients
            .store(maxclients as u64, Ordering::Relaxed);
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Release);
    }

    #[inline]
    fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub fn connection_accepted(&self) {
        if !self.is_enabled() {
            return;
        }
        self.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_opened(&self) {
        if !self.is_enabled() {
            return;
        }
        self.connections_current.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_closed(&self) {
        if !self.is_enabled() {
            return;
        }
        self.connections_closed.fetch_add(1, Ordering::Relaxed);
        self.connections_current
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(1)
            })
            .ok();
    }

    pub fn connection_rejected(&self, reason: &'static str) {
        if !self.is_enabled() {
            return;
        }
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
        self.record_rejection(reason);
    }

    pub fn add_input_bytes(&self, len: usize) {
        if !self.is_enabled() {
            return;
        }
        self.net_input_bytes
            .fetch_add(len as u64, Ordering::Relaxed);
    }

    pub fn add_output_bytes(&self, len: usize) {
        if !self.is_enabled() {
            return;
        }
        self.net_output_bytes
            .fetch_add(len as u64, Ordering::Relaxed);
    }

    pub fn record_parse_error(&self) {
        if !self.is_enabled() {
            return;
        }
        self.resp_parse_errors.fetch_add(1, Ordering::Relaxed);
        self.record_rejection("parse_error");
    }

    pub fn record_protocol_frames(&self, count: usize) {
        if !self.is_enabled() {
            return;
        }
        self.protocol_frames
            .fetch_add(count as u64, Ordering::Relaxed);
        self.pipeline_commands
            .fetch_add(count as u64, Ordering::Relaxed);
        observe_bucket(
            count as u64,
            &PIPELINE_BATCH_BOUNDS,
            &self.pipeline_batch_buckets,
        );
    }

    pub fn record_rejection(&self, reason: &'static str) {
        if !self.is_enabled() {
            return;
        }
        if let Some(index) = REJECTION_REASONS.iter().position(|name| *name == reason) {
            self.command_rejections[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_command(
        &self,
        command: &'static str,
        elapsed_us: u64,
        error_class: Option<&'static str>,
        slow_threshold_us: u64,
    ) {
        self.record_command_batch(command, 1, elapsed_us, error_class, slow_threshold_us);
    }

    pub fn record_command_batch(
        &self,
        command: &'static str,
        count: usize,
        elapsed_us: u64,
        error_class: Option<&'static str>,
        slow_threshold_us: u64,
    ) {
        if !self.is_enabled() {
            return;
        }
        let Some(index) = command_index().get(command).copied() else {
            return;
        };
        let count = count.max(1) as u64;
        let per_command_us = (elapsed_us / count).max(1);
        let stats = &self.commands[index];
        stats.calls.fetch_add(count, Ordering::Relaxed);
        stats
            .duration_sum_us
            .fetch_add(per_command_us.saturating_mul(count), Ordering::Relaxed);
        stats.duration_count.fetch_add(count, Ordering::Relaxed);
        observe_duration(per_command_us, count, &stats.duration_buckets);
        if per_command_us >= slow_threshold_us {
            stats.slow.fetch_add(count, Ordering::Relaxed);
        }
        if let Some(error_class) = error_class
            && let Some(error_index) = ERROR_CLASSES.iter().position(|name| *name == error_class)
        {
            stats.errors[error_index].fetch_add(count, Ordering::Relaxed);
        }
    }

    pub fn record_storage_read(&self, elapsed_us: u64) {
        if !self.is_enabled() {
            return;
        }
        self.storage_reads.fetch_add(1, Ordering::Relaxed);
        self.storage_read_duration_sum_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.storage_read_duration_count
            .fetch_add(1, Ordering::Relaxed);
        observe_duration(elapsed_us, 1, &self.storage_read_duration_buckets);
    }

    pub fn record_storage_write(&self, elapsed_us: u64, failed: bool) {
        if !self.is_enabled() {
            return;
        }
        self.storage_writes.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.storage_write_errors.fetch_add(1, Ordering::Relaxed);
        }
        self.storage_write_duration_sum_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.storage_write_duration_count
            .fetch_add(1, Ordering::Relaxed);
        observe_duration(elapsed_us, 1, &self.storage_write_duration_buckets);
    }

    pub fn record_ttl_sweep_duration(&self, elapsed_us: u64) {
        if !self.is_enabled() {
            return;
        }
        self.ttl_sweep_duration_sum_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.ttl_sweep_duration_count
            .fetch_add(1, Ordering::Relaxed);
        observe_duration(elapsed_us, 1, &self.ttl_sweep_duration_buckets);
    }

    pub fn record_fulltext_refresh(&self, elapsed_us: u64, failed: bool) {
        if !self.is_enabled() {
            return;
        }
        self.fulltext_refresh_requests
            .fetch_add(1, Ordering::Relaxed);
        if failed {
            self.fulltext_refresh_errors.fetch_add(1, Ordering::Relaxed);
        }
        self.fulltext_refresh_duration_sum_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.fulltext_refresh_duration_count
            .fetch_add(1, Ordering::Relaxed);
        observe_duration(elapsed_us, 1, &self.fulltext_refresh_duration_buckets);
    }

    pub fn record_fulltext_search(&self, elapsed_us: u64) {
        if !self.is_enabled() {
            return;
        }
        self.fulltext_search_total.fetch_add(1, Ordering::Relaxed);
        self.fulltext_search_duration_sum_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.fulltext_search_duration_count
            .fetch_add(1, Ordering::Relaxed);
        observe_duration(elapsed_us, 1, &self.fulltext_search_duration_buckets);
    }

    pub fn set_stream_snapshot(&self, groups: u64, pending_entries: u64) {
        if !self.is_enabled() {
            return;
        }
        self.stream_groups.store(groups, Ordering::Relaxed);
        self.stream_pending_entries
            .store(pending_entries, Ordering::Relaxed);
    }

    pub fn record_stream_read(&self) {
        if !self.is_enabled() {
            return;
        }
        self.stream_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_stream_claim(&self) {
        if !self.is_enabled() {
            return;
        }
        self.stream_claims.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_stream_autoclaim(&self) {
        if !self.is_enabled() {
            return;
        }
        self.stream_autoclaims.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stream_blocked_started(&self) {
        if !self.is_enabled() {
            return;
        }
        self.stream_blocked_clients.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stream_blocked_finished(&self) {
        if !self.is_enabled() {
            return;
        }
        self.stream_blocked_clients
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(1)
            })
            .ok();
    }

    pub fn set_vector_indexes(&self, indexes: u64) {
        if !self.is_enabled() {
            return;
        }
        self.vector_indexes.store(indexes, Ordering::Relaxed);
    }

    pub fn record_vector_write(&self) {
        if !self.is_enabled() {
            return;
        }
        self.vector_writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_vector_search(&self, elapsed_us: u64, failed: bool) {
        if !self.is_enabled() {
            return;
        }
        self.vector_search_total.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.vector_search_errors.fetch_add(1, Ordering::Relaxed);
        }
        self.vector_search_duration_sum_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.vector_search_duration_count
            .fetch_add(1, Ordering::Relaxed);
        observe_duration(elapsed_us, 1, &self.vector_search_duration_buckets);
    }

    pub fn record_lua_eval(&self, elapsed_us: u64, failed: bool) {
        if !self.is_enabled() {
            return;
        }
        self.lua_eval_total.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.lua_eval_errors.fetch_add(1, Ordering::Relaxed);
        }
        self.lua_eval_duration_sum_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.lua_eval_duration_count.fetch_add(1, Ordering::Relaxed);
        observe_duration(elapsed_us, 1, &self.lua_eval_duration_buckets);
    }

    pub fn record_wasm_call(&self, elapsed_us: u64, failed: bool) {
        if !self.is_enabled() {
            return;
        }
        self.wasm_calls_total.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.wasm_errors.fetch_add(1, Ordering::Relaxed);
        }
        self.wasm_duration_sum_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.wasm_duration_count.fetch_add(1, Ordering::Relaxed);
        observe_duration(elapsed_us, 1, &self.wasm_duration_buckets);
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(64 * 1024);
        push_gauge(&mut out, "onedis_up", "1");
        push_gauge(
            &mut out,
            "onedis_uptime_seconds",
            &self.started.elapsed().as_secs().to_string(),
        );
        let _ = writeln!(
            out,
            "onedis_build_info{{version=\"{}\",git_sha=\"unknown\"}} 1",
            env!("CARGO_PKG_VERSION")
        );
        push_gauge(
            &mut out,
            "onedis_config_databases",
            &self.config_databases.load(Ordering::Relaxed).to_string(),
        );
        push_gauge(
            &mut out,
            "onedis_config_maxclients",
            &self.config_maxclients.load(Ordering::Relaxed).to_string(),
        );
        push_gauge(
            &mut out,
            "onedis_connections_current",
            &self.connections_current.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            &mut out,
            "onedis_connections_total",
            &self.connections_total.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            &mut out,
            "onedis_connections_rejected_total",
            &self
                .connections_rejected
                .load(Ordering::Relaxed)
                .to_string(),
        );
        push_counter(
            &mut out,
            "onedis_connections_closed_total",
            &self.connections_closed.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            &mut out,
            "onedis_net_input_bytes_total",
            &self.net_input_bytes.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            &mut out,
            "onedis_net_output_bytes_total",
            &self.net_output_bytes.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            &mut out,
            "onedis_resp_parse_errors_total",
            &self.resp_parse_errors.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            &mut out,
            "onedis_protocol_frames_total",
            &self.protocol_frames.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            &mut out,
            "onedis_pipeline_commands_total",
            &self.pipeline_commands.load(Ordering::Relaxed).to_string(),
        );
        render_pipeline_buckets(&mut out, &self.pipeline_batch_buckets);
        render_rejections(&mut out, &self.command_rejections);
        render_commands(&mut out, &self.commands);
        self.render_storage(&mut out);
        self.render_module_metrics(&mut out);
        out
    }

    fn render_storage(&self, out: &mut String) {
        push_counter(
            out,
            "onedis_storage_reads_total",
            &self.storage_reads.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            out,
            "onedis_storage_writes_total",
            &self.storage_writes.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            out,
            "onedis_storage_write_errors_total",
            &self
                .storage_write_errors
                .load(Ordering::Relaxed)
                .to_string(),
        );
        render_duration_histogram(
            out,
            "onedis_storage_read_duration_seconds",
            &self.storage_read_duration_buckets,
            self.storage_read_duration_sum_us.load(Ordering::Relaxed),
            self.storage_read_duration_count.load(Ordering::Relaxed),
        );
        render_duration_histogram(
            out,
            "onedis_storage_write_duration_seconds",
            &self.storage_write_duration_buckets,
            self.storage_write_duration_sum_us.load(Ordering::Relaxed),
            self.storage_write_duration_count.load(Ordering::Relaxed),
        );
        push_gauge(out, "onedis_storage_pending_write_bytes", "0");
        push_gauge(out, "onedis_storage_compaction_running", "0");
    }

    fn render_module_metrics(&self, out: &mut String) {
        render_duration_histogram(
            out,
            "onedis_ttl_sweep_duration_seconds",
            &self.ttl_sweep_duration_buckets,
            self.ttl_sweep_duration_sum_us.load(Ordering::Relaxed),
            self.ttl_sweep_duration_count.load(Ordering::Relaxed),
        );
        push_counter(
            out,
            "onedis_fulltext_refresh_requests_total",
            &self
                .fulltext_refresh_requests
                .load(Ordering::Relaxed)
                .to_string(),
        );
        push_counter(
            out,
            "onedis_fulltext_refresh_errors_total",
            &self
                .fulltext_refresh_errors
                .load(Ordering::Relaxed)
                .to_string(),
        );
        render_duration_histogram(
            out,
            "onedis_fulltext_refresh_duration_seconds",
            &self.fulltext_refresh_duration_buckets,
            self.fulltext_refresh_duration_sum_us
                .load(Ordering::Relaxed),
            self.fulltext_refresh_duration_count.load(Ordering::Relaxed),
        );
        push_counter(
            out,
            "onedis_fulltext_search_total",
            &self
                .fulltext_search_total
                .load(Ordering::Relaxed)
                .to_string(),
        );
        render_duration_histogram(
            out,
            "onedis_fulltext_search_duration_seconds",
            &self.fulltext_search_duration_buckets,
            self.fulltext_search_duration_sum_us.load(Ordering::Relaxed),
            self.fulltext_search_duration_count.load(Ordering::Relaxed),
        );
        push_gauge(
            out,
            "onedis_stream_groups_total",
            &self.stream_groups.load(Ordering::Relaxed).to_string(),
        );
        push_gauge(
            out,
            "onedis_stream_pending_entries",
            &self
                .stream_pending_entries
                .load(Ordering::Relaxed)
                .to_string(),
        );
        push_counter(
            out,
            "onedis_stream_claims_total",
            &self.stream_claims.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            out,
            "onedis_stream_autoclaims_total",
            &self.stream_autoclaims.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            out,
            "onedis_stream_reads_total",
            &self.stream_reads.load(Ordering::Relaxed).to_string(),
        );
        push_gauge(
            out,
            "onedis_stream_blocked_clients",
            &self
                .stream_blocked_clients
                .load(Ordering::Relaxed)
                .to_string(),
        );
        push_gauge(
            out,
            "onedis_vector_indexes_total",
            &self.vector_indexes.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            out,
            "onedis_vector_writes_total",
            &self.vector_writes.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            out,
            "onedis_vector_search_total",
            &self.vector_search_total.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            out,
            "onedis_vector_search_errors_total",
            &self
                .vector_search_errors
                .load(Ordering::Relaxed)
                .to_string(),
        );
        render_duration_histogram(
            out,
            "onedis_vector_search_duration_seconds",
            &self.vector_search_duration_buckets,
            self.vector_search_duration_sum_us.load(Ordering::Relaxed),
            self.vector_search_duration_count.load(Ordering::Relaxed),
        );
        push_counter(
            out,
            "onedis_lua_eval_total",
            &self.lua_eval_total.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            out,
            "onedis_lua_eval_errors_total",
            &self.lua_eval_errors.load(Ordering::Relaxed).to_string(),
        );
        render_duration_histogram(
            out,
            "onedis_lua_eval_duration_seconds",
            &self.lua_eval_duration_buckets,
            self.lua_eval_duration_sum_us.load(Ordering::Relaxed),
            self.lua_eval_duration_count.load(Ordering::Relaxed),
        );
        push_counter(
            out,
            "onedis_wasm_calls_total",
            &self.wasm_calls_total.load(Ordering::Relaxed).to_string(),
        );
        push_counter(
            out,
            "onedis_wasm_errors_total",
            &self.wasm_errors.load(Ordering::Relaxed).to_string(),
        );
        render_duration_histogram(
            out,
            "onedis_wasm_duration_seconds",
            &self.wasm_duration_buckets,
            self.wasm_duration_sum_us.load(Ordering::Relaxed),
            self.wasm_duration_count.load(Ordering::Relaxed),
        );
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut total_commands_processed = 0;
        let mut total_command_errors = 0;
        let mut command_stats = Vec::with_capacity(COMMAND_NAMES.len());
        for (index, name) in COMMAND_NAMES.iter().enumerate() {
            let stats = &self.commands[index];
            let calls = stats.calls.load(Ordering::Relaxed);
            let errors = stats
                .errors
                .iter()
                .map(|error| error.load(Ordering::Relaxed))
                .sum();
            let usec = stats.duration_sum_us.load(Ordering::Relaxed);
            total_commands_processed += calls;
            total_command_errors += errors;
            command_stats.push(CommandStatsSnapshot {
                name,
                calls,
                errors,
                usec,
            });
        }

        MetricsSnapshot {
            total_connections_received: self.connections_total.load(Ordering::Relaxed),
            total_commands_processed,
            total_command_errors,
            total_net_input_bytes: self.net_input_bytes.load(Ordering::Relaxed),
            total_net_output_bytes: self.net_output_bytes.load(Ordering::Relaxed),
            rejected_connections: self.connections_rejected.load(Ordering::Relaxed),
            command_stats,
        }
    }
}

const PIPELINE_BATCH_BOUNDS: [u64; 6] = [1, 2, 4, 8, 16, u64::MAX];

fn zeroed_vec(len: usize) -> Vec<AtomicU64> {
    (0..len).map(|_| AtomicU64::new(0)).collect()
}

fn command_index() -> &'static HashMap<&'static str, usize> {
    static INDEX: OnceLock<HashMap<&'static str, usize>> = OnceLock::new();
    INDEX.get_or_init(|| {
        COMMAND_NAMES
            .iter()
            .enumerate()
            .map(|(index, name)| (*name, index))
            .collect()
    })
}

fn observe_duration(elapsed_us: u64, count: u64, buckets: &[AtomicU64]) {
    let index = DURATION_BUCKETS_US
        .iter()
        .position(|bound| elapsed_us <= *bound)
        .unwrap_or(DURATION_BUCKETS_US.len() - 1);
    buckets[index].fetch_add(count, Ordering::Relaxed);
}

fn observe_bucket(value: u64, bounds: &[u64], buckets: &[AtomicU64]) {
    let index = bounds
        .iter()
        .position(|bound| value <= *bound)
        .unwrap_or(bounds.len() - 1);
    buckets[index].fetch_add(1, Ordering::Relaxed);
}

pub fn elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}

pub fn classify_error_response(bytes: &[u8]) -> Option<&'static str> {
    if !bytes.starts_with(b"-") {
        return None;
    }
    let text = String::from_utf8_lossy(bytes).to_ascii_lowercase();
    if text.contains("noauth") {
        Some("noauth")
    } else if text.contains("noperm") {
        Some("noperm")
    } else if text.contains("wrong number") {
        Some("wrong_arity")
    } else if text.contains("wrongtype") || text.contains("wrong type") {
        Some("wrong_type")
    } else if text.contains("unsupported") || text.contains("unknown command") {
        Some("unsupported")
    } else if text.contains("timeout") {
        Some("timeout")
    } else if text.contains("kv_engine") || text.contains("storage") {
        Some("storage_error")
    } else {
        Some("internal_error")
    }
}

pub fn borrowed_fast_command_name(command: &[u8]) -> Option<&'static str> {
    if command.eq_ignore_ascii_case(b"GET") {
        Some("GET")
    } else if command.eq_ignore_ascii_case(b"MGET") {
        Some("MGET")
    } else if command.eq_ignore_ascii_case(b"EXISTS") {
        Some("EXISTS")
    } else if command.eq_ignore_ascii_case(b"TTL") {
        Some("TTL")
    } else if command.eq_ignore_ascii_case(b"PTTL") {
        Some("PTTL")
    } else if command.eq_ignore_ascii_case(b"STRLEN") {
        Some("STRLEN")
    } else if command.eq_ignore_ascii_case(b"TYPE") {
        Some("TYPE")
    } else if command.eq_ignore_ascii_case(b"SET") {
        Some("SET")
    } else if command.eq_ignore_ascii_case(b"HSET") {
        Some("HSET")
    } else if command.eq_ignore_ascii_case(b"LPUSH") {
        Some("LPUSH")
    } else if command.eq_ignore_ascii_case(b"RPUSH") {
        Some("RPUSH")
    } else if command.eq_ignore_ascii_case(b"LRANGE") {
        Some("LRANGE")
    } else if command.eq_ignore_ascii_case(b"PING") {
        Some("PING")
    } else {
        None
    }
}

fn push_gauge(out: &mut String, name: &str, value: &str) {
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

fn push_counter(out: &mut String, name: &str, value: &str) {
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {value}");
}

fn render_pipeline_buckets(out: &mut String, buckets: &[AtomicU64]) {
    let _ = writeln!(out, "# TYPE onedis_pipeline_batch_size histogram");
    let mut cumulative = 0;
    for (index, bound) in PIPELINE_BATCH_BOUNDS.iter().enumerate() {
        cumulative += buckets[index].load(Ordering::Relaxed);
        let le = if *bound == u64::MAX {
            "+Inf".to_string()
        } else {
            bound.to_string()
        };
        let _ = writeln!(
            out,
            "onedis_pipeline_batch_size_bucket{{le=\"{le}\"}} {cumulative}"
        );
    }
    let _ = writeln!(out, "onedis_pipeline_batch_size_count {cumulative}");
}

fn render_rejections(out: &mut String, rejections: &[AtomicU64]) {
    let _ = writeln!(out, "# TYPE onedis_command_rejected_total counter");
    for (index, reason) in REJECTION_REASONS.iter().enumerate() {
        let value = rejections[index].load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "onedis_command_rejected_total{{reason=\"{reason}\"}} {value}"
        );
    }
}

fn render_commands(out: &mut String, commands: &[CommandMetrics]) {
    let _ = writeln!(out, "# TYPE onedis_commands_total counter");
    let _ = writeln!(out, "# TYPE onedis_command_errors_total counter");
    let _ = writeln!(out, "# TYPE onedis_slow_commands_total counter");
    let _ = writeln!(out, "# TYPE onedis_command_duration_seconds histogram");
    for (index, command) in COMMAND_NAMES.iter().enumerate() {
        let stats = &commands[index];
        let calls = stats.calls.load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "onedis_commands_total{{command=\"{command}\"}} {calls}"
        );
        for (error_index, error_class) in ERROR_CLASSES.iter().enumerate() {
            let value = stats.errors[error_index].load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                "onedis_command_errors_total{{command=\"{command}\",error_class=\"{error_class}\"}} {value}"
            );
        }
        let slow = stats.slow.load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "onedis_slow_commands_total{{command=\"{command}\"}} {slow}"
        );
        let mut cumulative = 0;
        for (bucket_index, bound_us) in DURATION_BUCKETS_US.iter().enumerate() {
            cumulative += stats.duration_buckets[bucket_index].load(Ordering::Relaxed);
            let le = if *bound_us == u64::MAX {
                "+Inf".to_string()
            } else {
                format!("{:.6}", *bound_us as f64 / 1_000_000.0)
            };
            let _ = writeln!(
                out,
                "onedis_command_duration_seconds_bucket{{command=\"{command}\",le=\"{le}\"}} {cumulative}"
            );
        }
        let sum_seconds = stats.duration_sum_us.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let count = stats.duration_count.load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "onedis_command_duration_seconds_sum{{command=\"{command}\"}} {sum_seconds:.6}"
        );
        let _ = writeln!(
            out,
            "onedis_command_duration_seconds_count{{command=\"{command}\"}} {count}"
        );
    }
}

fn render_duration_histogram(
    out: &mut String,
    name: &str,
    buckets: &[AtomicU64],
    sum_us: u64,
    count: u64,
) {
    let _ = writeln!(out, "# TYPE {name} histogram");
    let mut cumulative = 0;
    for (bucket_index, bound_us) in DURATION_BUCKETS_US.iter().enumerate() {
        cumulative += buckets[bucket_index].load(Ordering::Relaxed);
        let le = if *bound_us == u64::MAX {
            "+Inf".to_string()
        } else {
            format!("{:.6}", *bound_us as f64 / 1_000_000.0)
        };
        let _ = writeln!(out, "{name}_bucket{{le=\"{le}\"}} {cumulative}");
    }
    let sum_seconds = sum_us as f64 / 1_000_000.0;
    let _ = writeln!(out, "{name}_sum {sum_seconds:.6}");
    let _ = writeln!(out, "{name}_count {count}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_render_includes_command_and_error_counters() {
        let metrics = OnedisMetrics::new();
        metrics.configure(16, 1000);
        metrics.connection_accepted();
        metrics.connection_opened();
        metrics.record_command("GET", 250, None, 10_000);
        metrics.record_command("SET", 20_000, Some("wrong_type"), 10_000);

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("onedis_config_databases 16"));
        assert!(rendered.contains("onedis_connections_current 1"));
        assert!(rendered.contains("onedis_commands_total{command=\"GET\"} 1"));
        assert!(
            rendered.contains(
                "onedis_command_errors_total{command=\"SET\",error_class=\"wrong_type\"} 1"
            )
        );
        assert!(rendered.contains("onedis_slow_commands_total{command=\"SET\"} 1"));
        assert!(rendered.contains("onedis_storage_reads_total 0"));
        assert!(rendered.contains("onedis_fulltext_search_total 0"));
        assert!(rendered.contains("onedis_stream_blocked_clients 0"));
    }

    #[test]
    fn classify_error_response_uses_stable_classes() {
        assert_eq!(
            classify_error_response(b"-NOAUTH Authentication required.\r\n"),
            Some("noauth")
        );
        assert_eq!(
            classify_error_response(b"-ERR wrong number of arguments\r\n"),
            Some("wrong_arity")
        );
        assert_eq!(classify_error_response(b"+OK\r\n"), None);
    }
}
