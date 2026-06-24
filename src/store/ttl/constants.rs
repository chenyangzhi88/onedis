type ExpireHook = dyn Fn(u16, &str, u8, &mut WriteBatch) -> bool + Send + Sync;

// ============================================================================
// Type Tags — encoded at meta value byte offset 16 (after expire_ms + version)
// ============================================================================

pub const TYPE_STRING: u8 = 1;
pub const TYPE_HASH: u8 = 2;
pub const TYPE_SET: u8 = 3;
pub const TYPE_SORTED_SET: u8 = 4;
pub const TYPE_LIST: u8 = 5;
pub const TYPE_JSON: u8 = 6;
pub const TYPE_VECTOR: u8 = 7;
pub const TYPE_STREAM: u8 = 8;

// ============================================================================
// Namespace byte patterns (must mirror db.rs constants)
// ============================================================================

const HASH_FIELD_NS: [u8; 3] = [0xFF, b'h', 0x00];
const HASH_FIELD_EXPIRE_NS: [u8; 3] = [0xFF, b'H', 0x00];
const LIST_ITEM_NS: [u8; 3] = [0xFF, b'l', 0x00];
const SET_MEMBER_NS: [u8; 3] = [0xFF, b's', 0x00];
const SET_SLOT_NS: [u8; 3] = [0xFF, b'S', 0x00];
const SET_MEMBER_SLOT_NS: [u8; 3] = [0xFF, b't', 0x00];
const ZSET_MEMBER_NS: [u8; 3] = [0xFF, b'z', 0x00];
const ZSET_RANK_NS: [u8; 3] = [0xFF, b'Z', 0x00];
const STREAM_ENTRY_NS: [u8; 3] = [0xFF, b'x', 0x00];
const STREAM_GROUP_NS: [u8; 3] = [0xFF, b'g', 0x00];
const STREAM_PEL_NS: [u8; 3] = [0xFF, b'p', 0x00];
const STREAM_CONSUMER_NS: [u8; 3] = [0xFF, b'c', 0x00];
const JSON_NODE_NS: [u8; 3] = [0xFF, b'j', 0x00];
const VECTOR_META_NS: [u8; 3] = [0xFF, b'v', 0x00];
const VECTOR_DOC_NS: [u8; 3] = [0xFF, b'v', 0x01];
const VECTOR_TAG_NS: [u8; 3] = [0xFF, b'v', 0x02];
const VECTOR_NUMERIC_NS: [u8; 3] = [0xFF, b'v', 0x03];
const VECTOR_SEGMENT_NS: [u8; 3] = [0xFF, b'v', 0x04];
const VECTOR_GRAPH_NS: [u8; 3] = [0xFF, b'v', 0x05];
const LIST_META_MAGIC: [u8; 4] = *b"ULST";
const STREAM_META_MAGIC: [u8; 4] = *b"USTR";
const TTL_INDEX_PREFIX: &[u8] = b"\xFE\xFFonedis:ttl:";
const TTL_INDEX_VALUE: &[u8] = b"\x01";
const VERSION_COUNTER_KEY: &[u8] = b"\xFE\xFFonedis:version";
const VERSION_MARK_PREFIX: &[u8] = b"\xFE\xFFonedis:version:";
const VERSION_RESERVATION_BLOCK: u64 = 4096;
