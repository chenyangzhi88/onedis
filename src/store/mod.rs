pub(crate) const TABLE_LOCAL_INTERNAL_PREFIX: &[u8] = b"\x80";

pub mod db;
pub mod db_manager;
pub mod kv_store;
pub mod ttl;
