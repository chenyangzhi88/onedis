use super::*;

impl Db {
    /**
     * 获取键值（返回 owned Structure）
     *
     * 自动进行惰性过期检测。
     */
    pub fn get(&self, key: &str) -> Option<Structure> {
        self.expire_if_needed(key);
        let raw = self.store.get_raw(&self.mk(key))?.clone();
        if let Some(meta) = decode_list_meta(&raw) {
            return Some(Structure::List(self.read_list_items(key, meta.version)));
        }
        if let Some(meta) = decode_stream_meta(&raw) {
            return Some(Structure::Stream(
                self.read_stream_entries(key, meta.version),
            ));
        }
        if let Some(meta) = decode_set_meta(&raw) {
            return Some(Structure::Set(self.read_set_members(key, meta.version)));
        }
        if let Some(meta) = decode_hash_meta(&raw) {
            return Some(Structure::Hash(self.read_hash_fields(key, meta.version)));
        }
        let (_, version, structure) = decode_entry(&raw)?;
        match structure {
            Structure::Hash(_) => Some(Structure::Hash(self.read_hash_fields(key, version))),
            Structure::SortedSet(_) => {
                Some(Structure::SortedSet(self.read_zset_members(key, version)))
            }
            Structure::Set(_) => Some(Structure::Set(self.read_set_members(key, version))),
            Structure::List(_) => Some(Structure::List(self.read_list_items(key, version))),
            Structure::Stream(_) => Some(Structure::Stream(self.read_stream_entries(key, version))),
            Structure::Json(json) if json == JSON_INDEXED_MARKER => self
                .read_json_value_at_path(key, version, &[])
                .ok()
                .flatten()
                .and_then(|value| serde_json::to_string(&value).ok())
                .map(Structure::Json),
            other => Some(other),
        }
    }

    pub fn get_string(&self, key: &str) -> Result<Option<String>, Error> {
        match self.get_string_bytes(key)? {
            Some(value) => String::from_utf8(value)
                .map(Some)
                .map_err(|_| Error::msg("Type parsing error")),
            None => Ok(None),
        }
    }

    pub async fn get_string_async(&self, key: &str) -> Result<Option<String>, Error> {
        match self.get_string_bytes_async(key).await? {
            Some(value) => String::from_utf8(value)
                .map(Some)
                .map_err(|_| Error::msg("Type parsing error")),
            None => Ok(None),
        }
    }

    pub fn get_string_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let Some(raw) = self.read_live_raw(key) else {
            return Ok(None);
        };
        if let Some(value) = decode_string_bytes(&raw) {
            Ok(Some(value))
        } else {
            Err(Error::msg(WRONG_TYPE_ERROR))
        }
    }

    pub async fn get_string_bytes_async(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let Some(raw) = self.read_live_raw_async(key).await else {
            return Ok(None);
        };
        if let Some(value) = decode_string_bytes(&raw) {
            Ok(Some(value))
        } else {
            Err(Error::msg(WRONG_TYPE_ERROR))
        }
    }
}
