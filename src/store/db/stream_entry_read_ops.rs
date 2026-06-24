impl Db {
    pub fn stream_len(&self, key: &str) -> Result<usize, Error> {
        Ok(self
            .stream_meta(key)?
            .map(|meta| meta.length as usize)
            .unwrap_or(0))
    }

    pub async fn stream_len_async(&self, key: &str) -> Result<usize, Error> {
        Ok(self
            .stream_meta_async(key)
            .await?
            .map(|meta| meta.length as usize)
            .unwrap_or(0))
    }

    pub fn stream_range(
        &self,
        key: &str,
        start: Option<StreamId>,
        end: Option<StreamId>,
        count: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<StreamEntry>, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(Vec::new());
        };
        let lower = start.unwrap_or(StreamId { ms: 0, seq: 0 });
        let upper = end.unwrap_or(StreamId {
            ms: u64::MAX,
            seq: u64::MAX,
        });
        let limit = count.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut entries = self.stream_entries_between(key, meta.version, lower, upper);
        if reverse {
            entries.reverse();
        }
        entries.truncate(limit);
        Ok(entries)
    }

    pub async fn stream_range_async(
        &self,
        key: &str,
        start: Option<StreamId>,
        end: Option<StreamId>,
        count: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<StreamEntry>, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(Vec::new());
        };
        let lower = start.unwrap_or(StreamId { ms: 0, seq: 0 });
        let upper = end.unwrap_or(StreamId {
            ms: u64::MAX,
            seq: u64::MAX,
        });
        let limit = count.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut entries = self
            .stream_entries_between_async(key, meta.version, lower, upper)
            .await;
        if reverse {
            entries.reverse();
        }
        entries.truncate(limit);
        Ok(entries)
    }

    pub fn stream_read(
        &self,
        requests: &[(String, StreamReadStart)],
        count: Option<usize>,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>, Error> {
        let mut result = Vec::new();
        let limit = count.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(result);
        }

        for (key, start) in requests {
            let Some(meta) = self.stream_meta(key)? else {
                continue;
            };
            let lower = match start {
                StreamReadStart::Id(id) => *id,
                StreamReadStart::Latest => meta.last_id,
            };
            let upper = StreamId {
                ms: u64::MAX,
                seq: u64::MAX,
            };
            let mut entries = self
                .stream_entries_between(key, meta.version, lower, upper)
                .into_iter()
                .filter(|entry| parse_stream_id(&entry.id).is_some_and(|id| id > lower))
                .collect::<Vec<_>>();
            entries.truncate(limit);
            if !entries.is_empty() {
                result.push((key.clone(), entries));
            }
        }
        Ok(result)
    }

    pub async fn stream_read_async(
        &self,
        requests: &[(String, StreamReadStart)],
        count: Option<usize>,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>, Error> {
        let mut result = Vec::new();
        let limit = count.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(result);
        }

        for (key, start) in requests {
            let Some(meta) = self.stream_meta_async(key).await? else {
                continue;
            };
            let lower = match start {
                StreamReadStart::Id(id) => *id,
                StreamReadStart::Latest => meta.last_id,
            };
            let upper = StreamId {
                ms: u64::MAX,
                seq: u64::MAX,
            };
            let mut entries = self
                .stream_entries_between_async(key, meta.version, lower, upper)
                .await
                .into_iter()
                .filter(|entry| parse_stream_id(&entry.id).is_some_and(|id| id > lower))
                .collect::<Vec<_>>();
            entries.truncate(limit);
            if !entries.is_empty() {
                result.push((key.clone(), entries));
            }
        }
        Ok(result)
    }
}
