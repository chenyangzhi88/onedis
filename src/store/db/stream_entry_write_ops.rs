use super::*;

impl Db {
    pub fn stream_add(
        &self,
        key: &str,
        requested_id: Option<StreamId>,
        fields: &[(String, String)],
    ) -> Result<StreamId, Error> {
        if fields.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xadd' command",
            ));
        }

        let mut meta = match self.stream_meta(key)? {
            Some(meta) => meta,
            None => StreamMeta {
                expire_ms: 0,
                version: self.next_persisted_version(),
                last_id: StreamId { ms: 0, seq: 0 },
                length: 0,
                entries_added: 0,
            },
        };

        let id = match requested_id {
            Some(id) => {
                if id.ms == 0 && id.seq == 0 {
                    return Err(Error::msg(
                        "ERR The ID specified in XADD must be greater than 0-0",
                    ));
                }
                if id <= meta.last_id {
                    return Err(Error::msg(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                    ));
                }
                id
            }
            None => self.next_stream_id(meta.last_id),
        };

        meta.last_id = id;
        meta.length += 1;
        meta.entries_added += 1;

        let mut batch = WriteBatch::new();
        batch.put(
            &stream_entry_key(self.db_index, key, meta.version, id),
            &encode_stream_entry(fields),
        );
        batch.put(&self.mk(key), &encode_stream_meta(meta));
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(id)
    }

    pub async fn stream_add_async(
        &self,
        key: &str,
        requested_id: Option<StreamId>,
        fields: &[(String, String)],
    ) -> Result<StreamId, Error> {
        if fields.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xadd' command",
            ));
        }

        let _stream_write_guard = self.set_write_lock(key).lock().await;
        let mut meta = match self.stream_meta_async(key).await? {
            Some(meta) => meta,
            None => StreamMeta {
                expire_ms: 0,
                version: self.next_persisted_version_async().await,
                last_id: StreamId { ms: 0, seq: 0 },
                length: 0,
                entries_added: 0,
            },
        };

        let id = match requested_id {
            Some(id) => {
                if id.ms == 0 && id.seq == 0 {
                    return Err(Error::msg(
                        "ERR The ID specified in XADD must be greater than 0-0",
                    ));
                }
                if id <= meta.last_id {
                    return Err(Error::msg(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                    ));
                }
                id
            }
            None => self.next_stream_id(meta.last_id),
        };

        meta.last_id = id;
        meta.length += 1;
        meta.entries_added += 1;

        let mut batch = WriteBatch::new();
        batch.put(
            &stream_entry_key(self.db_index, key, meta.version, id),
            &encode_stream_entry(fields),
        );
        batch.put(&self.mk(key), &encode_stream_meta(meta));
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(id)
    }
}
