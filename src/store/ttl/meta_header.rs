// ============================================================================
// Meta Header — fast decode without full bincode deserialization
// ============================================================================

/// Decoded header fields common to every meta value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaHeader {
    pub expire_ms: u64,
    pub version: u64,
    pub type_tag: u8,
}

/// Decode the fixed-size header from a raw meta value.
///
/// **Regular** format: `[expire_ms:8][version:8][type_tag:1][bincode…]`
/// **List** format:    `[ULST:4][expire_ms:8][version:8][head:8][tail:8]` (36 B)
pub fn decode_meta_header(raw: &[u8]) -> Option<MetaHeader> {
    // List meta: 36 bytes, starts with b"ULST"
    if raw.len() == 36 && raw[..4] == LIST_META_MAGIC {
        return Some(MetaHeader {
            expire_ms: u64::from_be_bytes(raw[4..12].try_into().ok()?),
            version: u64::from_be_bytes(raw[12..20].try_into().ok()?),
            type_tag: TYPE_LIST,
        });
    }
    // Stream meta: 52 bytes, starts with b"USTR"
    if raw.len() == 52 && raw[..4] == STREAM_META_MAGIC {
        return Some(MetaHeader {
            expire_ms: u64::from_be_bytes(raw[4..12].try_into().ok()?),
            version: u64::from_be_bytes(raw[12..20].try_into().ok()?),
            type_tag: TYPE_STREAM,
        });
    }
    // Regular meta: at least 17 bytes
    if raw.len() < 17 {
        return None;
    }
    Some(MetaHeader {
        expire_ms: u64::from_be_bytes(raw[0..8].try_into().ok()?),
        version: u64::from_be_bytes(raw[8..16].try_into().ok()?),
        type_tag: raw[16],
    })
}

pub fn patch_meta_expire_ms(raw: &[u8], expire_ms: u64) -> Option<Vec<u8>> {
    let mut patched = raw.to_vec();
    if raw.len() == 36 && raw[..4] == LIST_META_MAGIC {
        patched[4..12].copy_from_slice(&expire_ms.to_be_bytes());
        return Some(patched);
    }
    if raw.len() == 52 && raw[..4] == STREAM_META_MAGIC {
        patched[4..12].copy_from_slice(&expire_ms.to_be_bytes());
        return Some(patched);
    }
    if patched.len() < 8 {
        return None;
    }
    patched[0..8].copy_from_slice(&expire_ms.to_be_bytes());
    Some(patched)
}
