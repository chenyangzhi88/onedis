use anyhow::{Context, Error};
use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub struct ResourceLimits {
    pub response_bytes: usize,
    pub collection_items: usize,
    pub keys_items: usize,
    pub transaction_commands: usize,
    pub transaction_bytes: usize,
    pub subscriptions_per_client: usize,
    pub readonly_timeout_ms: u64,
    pub aggregate_cursors_per_db: usize,
    pub aggregate_cursor_idle_ms: u64,
    pub json_document_bytes: usize,
    pub json_nodes: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            response_bytes: 64 * 1024 * 1024,
            collection_items: 1_000_000,
            keys_items: 100_000,
            transaction_commands: 10_000,
            transaction_bytes: 64 * 1024 * 1024,
            subscriptions_per_client: 10_000,
            readonly_timeout_ms: 30_000,
            aggregate_cursors_per_db: 1_024,
            aggregate_cursor_idle_ms: 3_600_000,
            json_document_bytes: 16 * 1024 * 1024,
            json_nodes: 1_000_000,
        }
    }
}

impl ResourceLimits {
    fn from_env() -> Result<Self, Error> {
        let defaults = Self::default();
        let limits = Self {
            response_bytes: positive_usize("ONEDIS_LIMIT_RESPONSE_BYTES", defaults.response_bytes)?,
            collection_items: positive_usize(
                "ONEDIS_LIMIT_COLLECTION_ITEMS",
                defaults.collection_items,
            )?,
            keys_items: positive_usize("ONEDIS_LIMIT_KEYS_ITEMS", defaults.keys_items)?,
            transaction_commands: positive_usize(
                "ONEDIS_LIMIT_TRANSACTION_COMMANDS",
                defaults.transaction_commands,
            )?,
            transaction_bytes: positive_usize(
                "ONEDIS_LIMIT_TRANSACTION_BYTES",
                defaults.transaction_bytes,
            )?,
            subscriptions_per_client: positive_usize(
                "ONEDIS_LIMIT_SUBSCRIPTIONS_PER_CLIENT",
                defaults.subscriptions_per_client,
            )?,
            readonly_timeout_ms: positive_u64(
                "ONEDIS_LIMIT_READONLY_TIMEOUT_MS",
                defaults.readonly_timeout_ms,
            )?,
            aggregate_cursors_per_db: positive_usize(
                "ONEDIS_LIMIT_AGGREGATE_CURSORS_PER_DB",
                defaults.aggregate_cursors_per_db,
            )?,
            aggregate_cursor_idle_ms: positive_u64(
                "ONEDIS_LIMIT_AGGREGATE_CURSOR_IDLE_MS",
                defaults.aggregate_cursor_idle_ms,
            )?,
            json_document_bytes: positive_usize(
                "ONEDIS_LIMIT_JSON_DOCUMENT_BYTES",
                defaults.json_document_bytes,
            )?,
            json_nodes: positive_usize("ONEDIS_LIMIT_JSON_NODES", defaults.json_nodes)?,
        };
        if limits.response_bytes > crate::frame::MAX_FRAME_BYTES {
            return Err(Error::msg(format!(
                "ONEDIS_LIMIT_RESPONSE_BYTES must be <= {}",
                crate::frame::MAX_FRAME_BYTES
            )));
        }
        if limits.collection_items > crate::frame::MAX_ARRAY_ELEMENTS {
            return Err(Error::msg(format!(
                "ONEDIS_LIMIT_COLLECTION_ITEMS must be <= {}",
                crate::frame::MAX_ARRAY_ELEMENTS
            )));
        }
        if limits.keys_items > limits.collection_items {
            return Err(Error::msg(
                "ONEDIS_LIMIT_KEYS_ITEMS must be <= ONEDIS_LIMIT_COLLECTION_ITEMS",
            ));
        }
        if limits.json_document_bytes > crate::frame::MAX_BULK_STRING_BYTES {
            return Err(Error::msg(format!(
                "ONEDIS_LIMIT_JSON_DOCUMENT_BYTES must be <= {}",
                crate::frame::MAX_BULK_STRING_BYTES
            )));
        }
        if limits.json_nodes > crate::frame::MAX_FRAME_NODES {
            return Err(Error::msg(format!(
                "ONEDIS_LIMIT_JSON_NODES must be <= {}",
                crate::frame::MAX_FRAME_NODES
            )));
        }
        Ok(limits)
    }
}

pub fn validate_resource_limit_environment() -> Result<(), Error> {
    resource_limits().map(|_| ())
}

pub fn resource_limits() -> Result<&'static ResourceLimits, Error> {
    static LIMITS: OnceLock<Result<ResourceLimits, String>> = OnceLock::new();
    match LIMITS.get_or_init(|| ResourceLimits::from_env().map_err(|error| error.to_string())) {
        Ok(limits) => Ok(limits),
        Err(message) => Err(Error::msg(message.clone())),
    }
}

/// Returns the validated process limits for hot paths that cannot return a
/// configuration error. `Server::new` rejects an invalid environment before
/// any handler is created; the default is only used by isolated library tests.
pub fn active_resource_limits() -> &'static ResourceLimits {
    static DEFAULTS: OnceLock<ResourceLimits> = OnceLock::new();
    resource_limits().unwrap_or_else(|_| DEFAULTS.get_or_init(ResourceLimits::default))
}

fn positive_usize(name: &str, default: usize) -> Result<usize, Error> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    value
        .to_str()
        .ok_or_else(|| Error::msg(format!("{name} is not valid UTF-8")))?
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))
        .and_then(|value| {
            if value == 0 {
                Err(Error::msg(format!("{name} must be greater than zero")))
            } else {
                Ok(value)
            }
        })
}

fn positive_u64(name: &str, default: u64) -> Result<u64, Error> {
    let value = positive_usize(name, usize::try_from(default).unwrap_or(usize::MAX))?;
    u64::try_from(value).with_context(|| format!("{name} is out of range"))
}
