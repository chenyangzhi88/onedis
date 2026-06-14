impl Db {
    /**
     * 创建数据库
     */
    pub fn new(
        db_index: u16,
        store: KvStore,
        version_counter: Arc<VersionCounter>,
        ttl_manager: Arc<TtlManager>,
    ) -> Self {
        Self::new_with_mutation_tracker(
            db_index,
            store,
            version_counter,
            ttl_manager,
            Arc::new(KeyMutationTracker::default()),
        )
    }

    pub fn new_with_mutation_tracker(
        db_index: u16,
        store: KvStore,
        version_counter: Arc<VersionCounter>,
        ttl_manager: Arc<TtlManager>,
        mutation_tracker: Arc<KeyMutationTracker>,
    ) -> Self {
        Db {
            db_index,
            store,
            changes: Arc::new(AtomicU64::new(0)),
            version_counter,
            ttl_manager,
            counter_cache: Arc::new(DashMap::new()),
            counter_cache_epoch: Arc::new(AtomicU64::new(0)),
            list_meta_cache: Arc::new(DashMap::new()),
            vector_runtimes: Arc::new(VectorRuntimeRegistry::default()),
            fulltext_runtimes: Arc::new(FullTextRuntimeRegistry::default()),
            set_write_locks: Arc::new(std::array::from_fn(|_| tokio::sync::Mutex::new(()))),
            mutation_tracker,
            pending_mutations: Arc::new(Mutex::new(PendingMutations::default())),
        }
    }

    pub fn transactional_view(&self) -> Result<Self, Error> {
        Ok(Db {
            db_index: self.db_index,
            store: self.store.begin_transaction()?,
            changes: self.changes.clone(),
            version_counter: self.version_counter.clone(),
            ttl_manager: self.ttl_manager.clone(),
            counter_cache: self.counter_cache.clone(),
            counter_cache_epoch: self.counter_cache_epoch.clone(),
            list_meta_cache: self.list_meta_cache.clone(),
            vector_runtimes: self.vector_runtimes.clone(),
            fulltext_runtimes: self.fulltext_runtimes.clone(),
            set_write_locks: self.set_write_locks.clone(),
            mutation_tracker: self.mutation_tracker.clone(),
            pending_mutations: Arc::new(Mutex::new(PendingMutations::default())),
        })
    }

    fn set_write_lock(&self, key: &str) -> &tokio::sync::Mutex<()> {
        &self.set_write_locks[set_write_lock_shard(self.db_index, key)]
    }

    fn next_persisted_version(&self) -> u64 {
        Self::next_persisted_version_for_store(&self.store, &self.version_counter)
    }

    async fn next_persisted_version_async(&self) -> u64 {
        Self::next_persisted_version_for_store_async(&self.store, &self.version_counter).await
    }

    fn next_persisted_version_for_store(store: &KvStore, version_counter: &VersionCounter) -> u64 {
        version_counter.next_reserved(|high_water| {
            let mut batch = WriteBatch::new();
            reserve_version_high_water_to_batch(&mut batch, high_water);
            store.write_batch_direct(&batch);
        })
    }

    async fn next_persisted_version_for_store_async(
        store: &KvStore,
        version_counter: &VersionCounter,
    ) -> u64 {
        version_counter
            .next_reserved_async(|high_water| async move {
                let mut batch = WriteBatch::new();
                reserve_version_high_water_to_batch(&mut batch, high_water);
                store.write_batch_direct_async(batch).await;
            })
            .await
    }

    pub fn commit_transaction(&self) -> Result<(), Error> {
        let (keys, dbs) = self.take_pending_mutations();
        if keys.is_empty() && dbs.is_empty() {
            self.store.discard_transaction();
            return Ok(());
        }
        self.store.commit_transaction()?;
        let direct_db = self.non_transactional_view();
        direct_db.fulltext_reconcile_committed_keys(&keys)?;
        self.publish_mutations(keys, dbs);
        Ok(())
    }

    pub fn discard_transaction(&self) {
        self.store.discard_transaction();
    }

    pub async fn commit_transaction_async(&self) -> Result<(), Error> {
        let (keys, dbs) = self.take_pending_mutations();
        if keys.is_empty() && dbs.is_empty() {
            self.store.discard_transaction();
            return Ok(());
        }
        self.store.commit_transaction_async().await?;
        let direct_db = self.non_transactional_view();
        direct_db.fulltext_reconcile_committed_keys(&keys)?;
        self.publish_mutations(keys, dbs);
        Ok(())
    }

    fn non_transactional_view(&self) -> Self {
        Db {
            db_index: self.db_index,
            store: self.store.non_transactional_view(),
            version_counter: self.version_counter.clone(),
            ttl_manager: self.ttl_manager.clone(),
            changes: self.changes.clone(),
            fulltext_runtimes: self.fulltext_runtimes.clone(),
            vector_runtimes: self.vector_runtimes.clone(),
            mutation_tracker: self.mutation_tracker.clone(),
            pending_mutations: Arc::new(Mutex::new(PendingMutations::default())),
            list_meta_cache: self.list_meta_cache.clone(),
            counter_cache: self.counter_cache.clone(),
            counter_cache_epoch: self.counter_cache_epoch.clone(),
            set_write_locks: self.set_write_locks.clone(),
        }
    }

    fn write_batch_if_not_empty(&self, batch: &WriteBatch) {
        if batch.count() == 0 {
            return;
        }
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        self.store.write_batch(batch);
        self.record_or_publish_mutations(batch);
    }

    async fn write_batch_if_not_empty_async(&self, batch: &WriteBatch) {
        if batch.count() == 0 {
            return;
        }
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        self.store.write_batch_async(batch).await;
        self.record_or_publish_mutations(batch);
    }

    async fn write_batch_if_not_empty_without_watch_publish_async(&self, batch: &WriteBatch) {
        if batch.count() == 0 {
            return;
        }
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        self.store.write_batch_async(batch).await;
    }

    async fn compare_and_write_batch_if_not_empty_async(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> Result<bool, Error> {
        if batch.count() == 0 {
            return Ok(true);
        }
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        match self
            .store
            .compare_and_write_batch_async(conditions, batch)
            .await
        {
            Ok(()) => {
                self.record_or_publish_mutations(batch);
                Ok(true)
            }
            Err(Status::Conflict(_)) => Ok(false),
            Err(err) => Err(Error::msg(err.to_string())),
        }
    }

    fn record_or_publish_mutations(&self, batch: &WriteBatch) {
        let (keys, dbs) = collect_logical_mutations(batch);
        if keys.is_empty() && dbs.is_empty() {
            return;
        }

        if self.store.is_transactional() {
            let mut pending = self
                .pending_mutations
                .lock()
                .expect("pending mutation mutex poisoned");
            pending.keys.extend(keys);
            pending.dbs.extend(dbs);
            return;
        }

        self.publish_mutations(keys, dbs);
    }

    fn take_pending_mutations(&self) -> (Vec<Vec<u8>>, Vec<u16>) {
        let mut pending = self
            .pending_mutations
            .lock()
            .expect("pending mutation mutex poisoned");
        let keys = std::mem::take(&mut pending.keys);
        let dbs = std::mem::take(&mut pending.dbs);
        (keys, dbs)
    }

    fn publish_mutations(&self, keys: Vec<Vec<u8>>, dbs: Vec<u16>) {
        let mut seen_keys = HashSet::new();
        for key in keys {
            if seen_keys.insert(key.clone()) {
                self.mutation_tracker.bump_key(key);
            }
        }

        let mut seen_dbs = HashSet::new();
        for db_index in dbs {
            if seen_dbs.insert(db_index) {
                self.mutation_tracker.bump_db(db_index);
            }
        }
    }

    fn invalidate_counter_cache_for_batch(&self, batch: &WriteBatch) {
        let mut clear_all = false;
        let mut keys = Vec::new();
        for (write_type, key, _) in batch.iter() {
            match write_type {
                common::types::write_batch::WriteType::Put
                | common::types::write_batch::WriteType::PutBlobMedium
                | common::types::write_batch::WriteType::PutBlobExternal
                | common::types::write_batch::WriteType::Delete
                | common::types::write_batch::WriteType::Merge => {
                    if let Some(key) = logical_main_key_from_raw_key(key) {
                        keys.push(key);
                    }
                }
                common::types::write_batch::WriteType::RangeDelete => {
                    clear_all = true;
                    break;
                }
            }
        }

        if clear_all {
            self.counter_cache.clear();
            self.counter_cache_epoch.fetch_add(1, Ordering::Release);
            return;
        }
        if !keys.is_empty() {
            for key in keys {
                self.counter_cache.remove(&key);
            }
            self.counter_cache_epoch.fetch_add(1, Ordering::Release);
        }
    }

    fn invalidate_list_meta_cache_for_batch(&self, batch: &WriteBatch) {
        if self.store.is_transactional() {
            return;
        }
        let mut clear_all = false;
        let mut keys = Vec::new();
        for (write_type, key, _) in batch.iter() {
            match write_type {
                WriteType::Put
                | WriteType::PutBlobMedium
                | WriteType::PutBlobExternal
                | WriteType::Delete
                | WriteType::Merge => {
                    if let Some(key) = logical_main_key_from_raw_key(key) {
                        keys.push(key);
                    }
                }
                WriteType::RangeDelete => {
                    clear_all = true;
                    break;
                }
            }
        }
        if clear_all {
            self.list_meta_cache.clear();
            return;
        }
        for key in keys {
            self.list_meta_cache.remove(&key);
        }
    }

    fn cache_list_meta_if_non_transactional(&self, key: &str, meta: ListMeta) {
        if !self.store.is_transactional() {
            self.list_meta_cache.insert(self.mk(key), meta);
        }
    }

    fn remove_list_meta_cache_if_non_transactional(&self, key: &str) {
        if !self.store.is_transactional() {
            self.list_meta_cache.remove(&self.mk(key));
        }
    }

    pub fn handle_command_autocommit(&self, command: Command) -> Result<Frame, Error> {
        let txn_db = self.transactional_view()?;
        let frame = txn_db.handle_command(command)?;
        txn_db.commit_transaction()?;
        Ok(frame)
    }

    pub async fn handle_command_autocommit_async(&self, command: Command) -> Result<Frame, Error> {
        let txn_db = self.transactional_view()?;
        let frame = txn_db.handle_command_async(command).await?;
        txn_db.commit_transaction_async().await?;
        Ok(frame)
    }

    /// 生成带数据库前缀的主键。
    fn mk(&self, key: &str) -> Vec<u8> {
        main_key(self.db_index, key)
    }

    /// Read the encoded top-level key value used by Redis WATCH snapshots.
    ///
    /// The command watches logical keys, so a changed structure header, TTL, or
    /// type version must invalidate the transaction even if nested data lives in
    /// secondary namespaces.
    pub fn raw_main_value_for_watch(&self, key: &str) -> Option<Vec<u8>> {
        self.expire_if_needed(key);
        self.store.get_raw(&self.mk(key))
    }

    pub fn watch_version_snapshot(&self, key: &str) -> (u64, u64) {
        self.expire_if_needed(key);
        let key_version = self.mutation_tracker.key_version(&self.mk(key));
        let db_version = self.mutation_tracker.db_version(self.db_index);
        (key_version, db_version)
    }

    pub fn watch_version_changed(&self, key: &str, key_version: u64, db_version: u64) -> bool {
        self.expire_if_needed(key);
        self.mutation_tracker.key_version(&self.mk(key)) != key_version
            || self.mutation_tracker.db_version(self.db_index) != db_version
    }

    pub fn handle_command(&self, command: Command) -> Result<Frame, Error> {
        match command {
            Command::Bitcount(bitcount) => bitcount.apply(self),
            Command::Bitfield(bitfield) => bitfield.apply(self),
            Command::Bitop(bitop) => bitop.apply(self),
            Command::Bitpos(bitpos) => bitpos.apply(self),
            Command::Set(set) => set.apply(self),
            Command::Setex(setex) => setex.apply(self),
            Command::Setnx(setnx) => setnx.apply(self),
            Command::Psetex(psetex) => psetex.apply(self),
            Command::Get(get) => get.apply(self),
            Command::Lcs(lcs) => lcs.apply(self),
            Command::Geoadd(geoadd) => geoadd.apply(self),
            Command::Geodist(geodist) => geodist.apply(self),
            Command::Geohash(geohash) => geohash.apply(self),
            Command::Geopos(geopos) => geopos.apply(self),
            Command::Georadius(georadius) => georadius.apply(self),
            Command::Georadiusbymember(georadiusbymember) => georadiusbymember.apply(self),
            Command::Geosearch(geosearch) => geosearch.apply(self),
            Command::Geosearchstore(geosearchstore) => geosearchstore.apply(self),
            Command::Getbit(getbit) => getbit.apply(self),
            Command::Pfadd(pfadd) => pfadd.apply(self),
            Command::Pfcount(pfcount) => pfcount.apply(self),
            Command::Pfmerge(pfmerge) => pfmerge.apply(self),
            Command::GetDel(getdel) => getdel.apply(self),
            Command::GetEx(getex) => getex.apply(self),
            Command::Del(del) => del.apply(self),
            Command::Unlink(unlink) => unlink.apply(self),
            Command::GetRange(getrange) => getrange.apply(self),
            Command::Flushdb(flushdb) => flushdb.apply(self),
            Command::RandomKey(randomkey) => randomkey.apply(self),
            Command::Renamenx(renamenx) => renamenx.apply(self),
            Command::Rename(rename) => rename.apply(self),
            Command::Exists(exists) => exists.apply(self),
            Command::Expire(expire) => expire.apply(self),
            Command::ExpireTime(expiretime) => expiretime.apply(self),
            Command::Ttl(ttl) => ttl.apply(self),
            Command::Type(r#type) => r#type.apply(self),
            Command::Pttl(pttl) => pttl.apply(self),
            Command::PexpireTime(pexpiretime) => pexpiretime.apply(self),
            Command::Mset(mset) => mset.apply(self),
            Command::Msetex(msetex) => msetex.apply(self),
            Command::Msetnx(msetnx) => msetnx.apply(self),
            Command::Mget(mget) => mget.apply(self),
            Command::Strlen(strlen) => strlen.apply(self),
            Command::SetRange(setrange) => setrange.apply(self),
            Command::Append(append) => append.apply(self),
            Command::Setbit(setbit) => setbit.apply(self),
            Command::Touch(touch) => touch.apply(self),
            Command::Dbsize(dbsize) => dbsize.apply(self),
            Command::Persist(persist) => persist.apply(self),
            Command::Hexists(hexists) => hexists.apply(self),
            Command::Hstrlen(hstrlen) => hstrlen.apply(self),
            Command::Hgetall(hgetall) => hgetall.apply(self),
            Command::Hsetnx(hsetnx) => hsetnx.apply(self),
            Command::Hscan(hscan) => hscan.apply(self),
            Command::Hrandfield(hrandfield) => hrandfield.apply(self),
            Command::Hexpire(hexpire) => hexpire.apply(self),
            Command::HexpireAt(hexpireat) => hexpireat.apply(self),
            Command::HexpireTime(hexpiretime) => hexpiretime.apply(self),
            Command::Hgetdel(hgetdel) => hgetdel.apply(self),
            Command::Hgetex(hgetex) => hgetex.apply(self),
            Command::Hmget(hmget) => hmget.apply(self),
            Command::Hmset(hmset) => hmset.apply(self),
            Command::Hpersist(hpersist) => hpersist.apply(self),
            Command::Hpexpire(hpexpire) => hpexpire.apply(self),
            Command::HpexpireAt(hpexpireat) => hpexpireat.apply(self),
            Command::HpexpireTime(hpexpiretime) => hpexpiretime.apply(self),
            Command::Hpttl(hpttl) => hpttl.apply(self),
            Command::Hset(hset) => hset.apply(self),
            Command::Hsetex(hsetex) => hsetex.apply(self),
            Command::Hincrby(hincrby) => hincrby.apply(self),
            Command::HincrbyFloat(hincrbyfloat) => hincrbyfloat.apply(self),
            Command::Hget(hget) => hget.apply(self),
            Command::Hdel(hdel) => hdel.apply(self),
            Command::Keys(keys) => keys.apply(self),
            Command::Hlen(hlen) => hlen.apply(self),
            Command::Hkeys(hkeys) => hkeys.apply(self),
            Command::Httl(httl) => httl.apply(self),
            Command::Hvals(hvals) => hvals.apply(self),
            Command::Blmove(blmove) => blmove.apply(self),
            Command::Blmpop(blmpop) => blmpop.apply(self),
            Command::Blpop(blpop) => blpop.apply(self),
            Command::Brpop(brpop) => brpop.apply(self),
            Command::Brpoplpush(brpoplpush) => brpoplpush.apply(self),
            Command::Linsert(linsert) => linsert.apply(self),
            Command::Lmove(lmove) => lmove.apply(self),
            Command::Lmpop(lmpop) => lmpop.apply(self),
            Command::Lpush(lpush) => lpush.apply(self),
            Command::Rpush(rpush) => rpush.apply(self),
            Command::Lindex(lindex) => lindex.apply(self),
            Command::Lpop(lpop) => lpop.apply(self),
            Command::Lpos(lpos) => lpos.apply(self),
            Command::Lrem(lrem) => lrem.apply(self),
            Command::Rpop(rpop) => rpop.apply(self),
            Command::Rpoplpush(rpoplpush) => rpoplpush.apply(self),
            Command::Llen(llen) => llen.apply(self),
            Command::Sadd(sadd) => sadd.apply(self),
            Command::Scard(scard) => scard.apply(self),
            Command::Sdiff(sdiff) => sdiff.apply(self),
            Command::Sdiffstore(sdiffstore) => sdiffstore.apply(self),
            Command::Spop(spop) => spop.apply(self),
            Command::Srem(srem) => srem.apply(self),
            Command::Sinter(sinter) => sinter.apply(self),
            Command::Sintercard(sintercard) => sintercard.apply(self),
            Command::Sinterstore(sinterstore) => sinterstore.apply(self),
            Command::Sismember(sismember) => sismember.apply(self),
            Command::Smismember(smismember) => smismember.apply(self),
            Command::Srandmember(srandmember) => srandmember.apply(self),
            Command::Smove(smove) => smove.apply(self),
            Command::Sunionstore(sunionstore) => sunionstore.apply(self),
            Command::Smembers(smembers) => smembers.apply(self),
            Command::Sunion(sunion) => sunion.apply(self),
            Command::Rpushx(rpushx) => rpushx.apply(self),
            Command::Lpushx(lpushx) => lpushx.apply(self),
            Command::IncrbyFloat(incrbyfloat) => incrbyfloat.apply(self),
            Command::Incr(incr) => incr.apply(self),
            Command::Decr(decr) => decr.apply(self),
            Command::Lset(lset) => lset.apply(self),
            Command::Ltrim(ltrim) => ltrim.apply(self),
            Command::Bzmpop(bzmpop) => bzmpop.apply(self),
            Command::Bzpopmax(bzpopmax) => bzpopmax.apply(self),
            Command::Bzpopmin(bzpopmin) => bzpopmin.apply(self),
            Command::Zadd(zadd) => zadd.apply(self),
            Command::Zdiff(zdiff) => zdiff.apply(self),
            Command::Zdiffstore(zdiffstore) => zdiffstore.apply(self),
            Command::Zincrby(zincrby) => zincrby.apply(self),
            Command::Zinter(zinter) => zinter.apply(self),
            Command::Zintercard(zintercard) => zintercard.apply(self),
            Command::Zinterstore(zinterstore) => zinterstore.apply(self),
            Command::Zlexcount(zlexcount) => zlexcount.apply(self),
            Command::Zmpop(zmpop) => zmpop.apply(self),
            Command::Zcount(zcount) => zcount.apply(self),
            Command::Zscore(zscore) => zscore.apply(self),
            Command::Zmscore(zmscore) => zmscore.apply(self),
            Command::Zcard(zcard) => zcard.apply(self),
            Command::Zpopmax(zpopmax) => zpopmax.apply(self),
            Command::Zpopmin(zpopmin) => zpopmin.apply(self),
            Command::Zrandmember(zrandmember) => zrandmember.apply(self),
            Command::Zrank(zrank) => zrank.apply(self),
            Command::Zrevrank(zrevrank) => zrevrank.apply(self),
            Command::Zrem(zrem) => zrem.apply(self),
            Command::Zremrangebylex(zremrangebylex) => zremrangebylex.apply(self),
            Command::Zremrangebyrank(zremrangebyrank) => zremrangebyrank.apply(self),
            Command::Zremrangebyscore(zremrangebyscore) => zremrangebyscore.apply(self),
            Command::Zrange(zrange) => zrange.apply(self),
            Command::Zrangebylex(zrangebylex) => zrangebylex.apply(self),
            Command::Zrangestore(zrangestore) => zrangestore.apply(self),
            Command::Zrevrange(zrevrange) => zrevrange.apply(self),
            Command::Zrevrangebylex(zrevrangebylex) => zrevrangebylex.apply(self),
            Command::Zrevrangebyscore(zrevrangebyscore) => zrevrangebyscore.apply(self),
            Command::Zrangebyscore(zrangebyscore) => zrangebyscore.apply(self),
            Command::Zscan(zscan) => zscan.apply(self),
            Command::Zunion(zunion) => zunion.apply(self),
            Command::Zunionstore(zunionstore) => zunionstore.apply(self),
            Command::Xack(xack) => xack.apply(self),
            Command::Xackdel(xackdel) => xackdel.apply(self),
            Command::Xadd(xadd) => xadd.apply(self),
            Command::Xautoclaim(xautoclaim) => xautoclaim.apply(self),
            Command::Xcfgset(xcfgset) => xcfgset.apply(self),
            Command::Xclaim(xclaim) => xclaim.apply(self),
            Command::Xdel(xdel) => xdel.apply(self),
            Command::Xdelex(xdelex) => xdelex.apply(self),
            Command::Xgroup(xgroup) => xgroup.apply(self),
            Command::Xinfo(xinfo) => xinfo.apply(self),
            Command::Xlen(xlen) => xlen.apply(self),
            Command::Xpending(xpending) => xpending.apply(self),
            Command::Xrange(xrange) => xrange.apply(self),
            Command::Xread(xread) => xread.apply(self),
            Command::Xreadgroup(xreadgroup) => xreadgroup.apply(self),
            Command::Xrevrange(xrevrange) => xrevrange.apply(self),
            Command::Xsetid(xsetid) => xsetid.apply(self),
            Command::Xtrim(xtrim) => xtrim.apply(self),
            Command::Incrby(incrby) => incrby.apply(self),
            Command::Decrby(decrby) => decrby.apply(self),
            Command::ExpireAt(expireat) => expireat.apply(self),
            Command::PexpireAt(pexpireat) => pexpireat.apply(self),
            Command::Pexpire(pexpire) => pexpire.apply(self),
            Command::Lrange(lrange) => lrange.apply(self),
            Command::GetSet(getset) => getset.apply(self),
            Command::Info(info) => info.apply(self),
            Command::Scan(scan) => scan.apply(self),
            Command::Sscan(sscan) => sscan.apply(self),
            Command::JsonSet(json_set) => json_set.apply(self),
            Command::JsonGet(json_get) => json_get.apply(self),
            Command::JsonDel(json_del) => json_del.apply(self),
            Command::JsonType(json_type) => json_type.apply(self),
            Command::FtCreate(ft_create) => ft_create.apply(self),
            Command::FtList(ft_list) => ft_list.apply(self),
            Command::FtDropIndex(ft_drop_index) => ft_drop_index.apply(self),
            Command::FtAlter(ft_alter) => ft_alter.apply(self),
            Command::FtAliasAdd(ft_alias_add) => ft_alias_add.apply(self),
            Command::FtAliasUpdate(ft_alias_update) => ft_alias_update.apply(self),
            Command::FtAliasDel(ft_alias_del) => ft_alias_del.apply(self),
            Command::FtConfig(ft_config) => ft_config.apply(self),
            Command::FtInfo(ft_info) => ft_info.apply(self),
            Command::FtSearch(ft_search) => ft_search.apply(self),
            Command::FtHybrid(ft_hybrid) => ft_hybrid.apply(self),
            Command::FtAggregate(ft_aggregate) => ft_aggregate.apply(self),
            Command::FtCursor(ft_cursor) => ft_cursor.apply(self),
            Command::FtProfile(ft_profile) => ft_profile.apply(self),
            Command::FtExplain(ft_explain) => ft_explain.apply(self),
            Command::FtTagVals(ft_tagvals) => ft_tagvals.apply(self),
            Command::FtDict(ft_dict) => ft_dict.apply(self),
            Command::FtSpellCheck(ft_spellcheck) => ft_spellcheck.apply(self),
            Command::FtSug(ft_sug) => ft_sug.apply(self),
            Command::FtSyn(ft_syn) => ft_syn.apply(self),
            Command::FtUnsupported(ft_unsupported) => ft_unsupported.apply(),
            Command::Lua(lua) => lua.apply(self),
            Command::VAdd(vadd) => vadd.apply(self),
            Command::VSim(vsim) => vsim.apply(self),
            Command::VRem(vrem) => vrem.apply(self),
            Command::VCard(vcard) => vcard.apply(self),
            Command::VDim(vdim) => vdim.apply(self),
            Command::VEmb(vemb) => vemb.apply(self),
            Command::VGetAttr(vgetattr) => vgetattr.apply(self),
            Command::VSetAttr(vsetattr) => vsetattr.apply(self),
            Command::VInfo(vinfo) => vinfo.apply(self),
            Command::VRandMember(vrandmember) => vrandmember.apply(self),
            Command::VLinks(vlinks) => vlinks.apply(self),
            Command::Copy(copy) => {
                let copied = self.copy_key_to_db(
                    copy.db_index().unwrap_or(self.db_index as usize) as u16,
                    copy.source(),
                    copy.destination(),
                    copy.replace(),
                )?;
                Ok(Frame::Integer(if copied { 1 } else { 0 }))
            }
            Command::Move(r#move) => {
                let moved = self.move_key_to_db(r#move.get_db_index() as u16, r#move.get_key())?;
                Ok(Frame::Integer(if moved { 1 } else { 0 }))
            }
            Command::Flushall(_) => {
                self.clear();
                Ok(Frame::Ok)
            }
            _ => Err(Error::msg("Unknown command")),
        }
    }

    pub async fn handle_command_async(&self, command: Command) -> Result<Frame, Error> {
        match command {
            Command::Bitcount(bitcount) => bitcount.apply_async(self).await,
            Command::Set(set) => set.apply_async(self).await,
            Command::Bitfield(bitfield) => bitfield.apply_async(self).await,
            Command::Bitop(bitop) => bitop.apply_async(self).await,
            Command::Bitpos(bitpos) => bitpos.apply_async(self).await,
            Command::Get(get) => get.apply_async(self).await,
            Command::Getbit(getbit) => getbit.apply_async(self).await,
            Command::GetRange(getrange) => getrange.apply_async(self).await,
            Command::Lcs(lcs) => lcs.apply_async(self).await,
            Command::Setex(setex) => setex.apply_async(self).await,
            Command::Setnx(setnx) => setnx.apply_async(self).await,
            Command::Psetex(psetex) => psetex.apply_async(self).await,
            Command::Mset(mset) => mset.apply_async(self).await,
            Command::Mget(mget) => mget.apply_async(self).await,
            Command::Msetnx(msetnx) => msetnx.apply_async(self).await,
            Command::Incr(incr) => incr.apply_async(self).await,
            Command::Incrby(incrby) => incrby.apply_async(self).await,
            Command::Decr(decr) => decr.apply_async(self).await,
            Command::Decrby(decrby) => decrby.apply_async(self).await,
            Command::Append(append) => append.apply_async(self).await,
            Command::SetRange(setrange) => setrange.apply_async(self).await,
            Command::Setbit(setbit) => setbit.apply_async(self).await,
            Command::GetSet(getset) => getset.apply_async(self).await,
            Command::GetDel(getdel) => getdel.apply_async(self).await,
            Command::GetEx(getex) => getex.apply_async(self).await,
            Command::Msetex(msetex) => msetex.apply_async(self).await,
            Command::Strlen(strlen) => strlen.apply_async(self).await,
            Command::IncrbyFloat(incrbyfloat) => incrbyfloat.apply_async(self).await,
            Command::Hset(hset) => hset.apply_async(self).await,
            Command::Hdel(hdel) => hdel.apply_async(self).await,
            Command::Hexists(hexists) => hexists.apply_async(self).await,
            Command::Hget(hget) => hget.apply_async(self).await,
            Command::Hgetall(hgetall) => hgetall.apply_async(self).await,
            Command::Hkeys(hkeys) => hkeys.apply_async(self).await,
            Command::Hlen(hlen) => hlen.apply_async(self).await,
            Command::Hmget(hmget) => hmget.apply_async(self).await,
            Command::Hrandfield(hrandfield) => hrandfield.apply_async(self).await,
            Command::Hscan(hscan) => hscan.apply_async(self).await,
            Command::Hstrlen(hstrlen) => hstrlen.apply_async(self).await,
            Command::Httl(httl) => httl.apply_async(self).await,
            Command::Hpttl(hpttl) => hpttl.apply_async(self).await,
            Command::HexpireTime(hexpiretime) => hexpiretime.apply_async(self).await,
            Command::HpexpireTime(hpexpiretime) => hpexpiretime.apply_async(self).await,
            Command::Hvals(hvals) => hvals.apply_async(self).await,
            Command::Hmset(hmset) => hmset.apply_async(self).await,
            Command::Hsetnx(hsetnx) => hsetnx.apply_async(self).await,
            Command::Hincrby(hincrby) => hincrby.apply_async(self).await,
            Command::HincrbyFloat(hincrbyfloat) => hincrbyfloat.apply_async(self).await,
            Command::Hgetdel(hgetdel) => hgetdel.apply_async(self).await,
            Command::Hgetex(hgetex) => hgetex.apply_async(self).await,
            Command::Hsetex(hsetex) => hsetex.apply_async(self).await,
            Command::Hexpire(hexpire) => hexpire.apply_async(self).await,
            Command::HexpireAt(hexpireat) => hexpireat.apply_async(self).await,
            Command::Hpexpire(hpexpire) => hpexpire.apply_async(self).await,
            Command::HpexpireAt(hpexpireat) => hpexpireat.apply_async(self).await,
            Command::Hpersist(hpersist) => hpersist.apply_async(self).await,
            Command::Del(del) => del.apply_async(self).await,
            Command::Unlink(unlink) => unlink.apply_async(self).await,
            Command::Expire(expire) => expire.apply_async(self).await,
            Command::ExpireAt(expireat) => expireat.apply_async(self).await,
            Command::Pexpire(pexpire) => pexpire.apply_async(self).await,
            Command::PexpireAt(pexpireat) => pexpireat.apply_async(self).await,
            Command::Persist(persist) => persist.apply_async(self).await,
            Command::Rename(rename) => rename.apply_async(self).await,
            Command::Renamenx(renamenx) => renamenx.apply_async(self).await,
            Command::Flushdb(flushdb) => flushdb.apply_async(self).await,
            Command::Exists(exists) => exists.apply_async(self).await,
            Command::ExpireTime(expiretime) => expiretime.apply_async(self).await,
            Command::PexpireTime(pexpiretime) => pexpiretime.apply_async(self).await,
            Command::RandomKey(randomkey) => randomkey.apply_async(self).await,
            Command::Touch(touch) => touch.apply_async(self).await,
            Command::Ttl(ttl) => ttl.apply_async(self).await,
            Command::Pttl(pttl) => pttl.apply_async(self).await,
            Command::Type(r#type) => r#type.apply_async(self).await,
            Command::Lrange(lrange) => lrange.apply_async(self).await,
            Command::Dbsize(dbsize) => dbsize.apply_async(self).await,
            Command::Keys(keys) => keys.apply_async(self).await,
            Command::Scan(scan) => scan.apply_async(self).await,
            Command::Sdiff(sdiff) => sdiff.apply_async(self).await,
            Command::Sdiffstore(sdiffstore) => sdiffstore.apply_async(self).await,
            Command::Sadd(sadd) => sadd.apply_async(self).await,
            Command::Scard(scard) => scard.apply_async(self).await,
            Command::Sismember(sismember) => sismember.apply_async(self).await,
            Command::Sintercard(sintercard) => sintercard.apply_async(self).await,
            Command::Smismember(smismember) => smismember.apply_async(self).await,
            Command::Srem(srem) => srem.apply_async(self).await,
            Command::Sinter(sinter) => sinter.apply_async(self).await,
            Command::Sinterstore(sinterstore) => sinterstore.apply_async(self).await,
            Command::Smembers(smembers) => smembers.apply_async(self).await,
            Command::Spop(spop) => spop.apply_async(self).await,
            Command::Srandmember(srandmember) => srandmember.apply_async(self).await,
            Command::Sscan(sscan) => sscan.apply_async(self).await,
            Command::Sunion(sunion) => sunion.apply_async(self).await,
            Command::Sunionstore(sunionstore) => sunionstore.apply_async(self).await,
            Command::Zcard(zcard) => zcard.apply_async(self).await,
            Command::Zadd(zadd) => zadd.apply_async(self).await,
            Command::Zincrby(zincrby) => zincrby.apply_async(self).await,
            Command::Zcount(zcount) => zcount.apply_async(self).await,
            Command::Zdiff(zdiff) => zdiff.apply_async(self).await,
            Command::Zrange(zrange) => zrange.apply_async(self).await,
            Command::Zrangebylex(zrangebylex) => zrangebylex.apply_async(self).await,
            Command::Zrank(zrank) => zrank.apply_async(self).await,
            Command::Zrem(zrem) => zrem.apply_async(self).await,
            Command::Zremrangebyrank(zremrangebyrank) => zremrangebyrank.apply_async(self).await,
            Command::Zremrangebyscore(zremrangebyscore) => zremrangebyscore.apply_async(self).await,
            Command::Zdiffstore(zdiffstore) => zdiffstore.apply_async(self).await,
            Command::Zinter(zinter) => zinter.apply_async(self).await,
            Command::Zintercard(zintercard) => zintercard.apply_async(self).await,
            Command::Zinterstore(zinterstore) => zinterstore.apply_async(self).await,
            Command::Zlexcount(zlexcount) => zlexcount.apply_async(self).await,
            Command::Zmscore(zmscore) => zmscore.apply_async(self).await,
            Command::Zrandmember(zrandmember) => zrandmember.apply_async(self).await,
            Command::Zunionstore(zunionstore) => zunionstore.apply_async(self).await,
            Command::Zpopmax(zpopmax) => zpopmax.apply_async(self).await,
            Command::Zpopmin(zpopmin) => zpopmin.apply_async(self).await,
            Command::Zremrangebylex(zremrangebylex) => zremrangebylex.apply_async(self).await,
            Command::Zrevrange(zrevrange) => zrevrange.apply_async(self).await,
            Command::Zrevrangebylex(zrevrangebylex) => zrevrangebylex.apply_async(self).await,
            Command::Zrevrangebyscore(zrevrangebyscore) => zrevrangebyscore.apply_async(self).await,
            Command::Zrevrank(zrevrank) => zrevrank.apply_async(self).await,
            Command::Zrangebyscore(zrangebyscore) => zrangebyscore.apply_async(self).await,
            Command::Zrangestore(zrangestore) => zrangestore.apply_async(self).await,
            Command::Zscan(zscan) => zscan.apply_async(self).await,
            Command::Zscore(zscore) => zscore.apply_async(self).await,
            Command::Zunion(zunion) => zunion.apply_async(self).await,
            Command::Lindex(lindex) => lindex.apply_async(self).await,
            Command::Llen(llen) => llen.apply_async(self).await,
            Command::Lpos(lpos) => lpos.apply_async(self).await,
            Command::Lpush(lpush) => lpush.apply_async(self).await,
            Command::Lpushx(lpushx) => lpushx.apply_async(self).await,
            Command::Rpush(rpush) => rpush.apply_async(self).await,
            Command::Rpushx(rpushx) => rpushx.apply_async(self).await,
            Command::Lpop(lpop) => lpop.apply_async(self).await,
            Command::Rpop(rpop) => rpop.apply_async(self).await,
            Command::Lset(lset) => lset.apply_async(self).await,
            Command::Ltrim(ltrim) => ltrim.apply_async(self).await,
            Command::Linsert(linsert) => linsert.apply_async(self).await,
            Command::Lrem(lrem) => lrem.apply_async(self).await,
            Command::Lmpop(lmpop) => lmpop.apply_async(self).await,
            Command::Blpop(blpop) => blpop.apply_async(self).await,
            Command::Brpop(brpop) => brpop.apply_async(self).await,
            Command::Brpoplpush(brpoplpush) => brpoplpush.apply_async(self).await,
            Command::Blmove(blmove) => blmove.apply_async(self).await,
            Command::Blmpop(blmpop) => blmpop.apply_async(self).await,
            Command::Rpoplpush(rpoplpush) => rpoplpush.apply_async(self).await,
            Command::Lmove(lmove) => lmove.apply_async(self).await,
            Command::Smove(smove) => smove.apply_async(self).await,
            Command::Geoadd(geoadd) => geoadd.apply_async(self).await,
            Command::Geodist(geodist) => geodist.apply_async(self).await,
            Command::Geohash(geohash) => geohash.apply_async(self).await,
            Command::Geopos(geopos) => geopos.apply_async(self).await,
            Command::Geosearch(geosearch) => geosearch.apply_async(self).await,
            Command::Georadius(georadius) => georadius.apply_async(self).await,
            Command::Georadiusbymember(georadiusbymember) => {
                georadiusbymember.apply_async(self).await
            }
            Command::Geosearchstore(geosearchstore) => geosearchstore.apply_async(self).await,
            Command::Pfadd(pfadd) => pfadd.apply_async(self).await,
            Command::Pfcount(pfcount) => pfcount.apply_async(self).await,
            Command::Pfmerge(pfmerge) => pfmerge.apply_async(self).await,
            Command::Xadd(xadd) => xadd.apply_async(self).await,
            Command::Xdel(xdel) => xdel.apply_async(self).await,
            Command::Xtrim(xtrim) => xtrim.apply_async(self).await,
            Command::Xack(xack) => xack.apply_async(self).await,
            Command::Xackdel(xackdel) => xackdel.apply_async(self).await,
            Command::Xautoclaim(xautoclaim) => xautoclaim.apply_async(self).await,
            Command::Xcfgset(xcfgset) => xcfgset.apply_async(self).await,
            Command::Xclaim(xclaim) => xclaim.apply_async(self).await,
            Command::Xdelex(xdelex) => xdelex.apply_async(self).await,
            Command::Xgroup(xgroup) => xgroup.apply_async(self).await,
            Command::Xinfo(xinfo) => xinfo.apply_async(self).await,
            Command::Xlen(xlen) => xlen.apply_async(self).await,
            Command::Xpending(xpending) => xpending.apply_async(self).await,
            Command::Xrange(xrange) => xrange.apply_async(self).await,
            Command::Xread(xread) => xread.apply_async(self).await,
            Command::Xsetid(xsetid) => xsetid.apply_async(self).await,
            Command::Xrevrange(xrevrange) => xrevrange.apply_async(self).await,
            Command::Xreadgroup(xreadgroup) => xreadgroup.apply_async(self).await,
            Command::Zmpop(zmpop) => zmpop.apply_async(self).await,
            Command::Bzpopmin(bzpopmin) => bzpopmin.apply_async(self).await,
            Command::Bzpopmax(bzpopmax) => bzpopmax.apply_async(self).await,
            Command::Bzmpop(bzmpop) => bzmpop.apply_async(self).await,
            Command::JsonSet(json_set) => json_set.apply_async(self).await,
            Command::JsonGet(json_get) => json_get.apply_async(self).await,
            Command::JsonDel(json_del) => json_del.apply_async(self).await,
            Command::JsonType(json_type) => json_type.apply_async(self).await,
            Command::FtCreate(ft_create) => ft_create.apply_async(self).await,
            Command::FtList(ft_list) => ft_list.apply_async(self).await,
            Command::FtDropIndex(ft_drop_index) => ft_drop_index.apply_async(self).await,
            Command::FtAlter(ft_alter) => ft_alter.apply_async(self).await,
            Command::FtAliasAdd(ft_alias_add) => ft_alias_add.apply_async(self).await,
            Command::FtAliasUpdate(ft_alias_update) => ft_alias_update.apply_async(self).await,
            Command::FtAliasDel(ft_alias_del) => ft_alias_del.apply_async(self).await,
            Command::FtConfig(ft_config) => ft_config.apply_async(self).await,
            Command::FtInfo(ft_info) => ft_info.apply_async(self).await,
            Command::FtSearch(ft_search) => ft_search.apply_async(self).await,
            Command::FtHybrid(ft_hybrid) => ft_hybrid.apply_async(self).await,
            Command::FtAggregate(ft_aggregate) => ft_aggregate.apply_async(self).await,
            Command::FtCursor(ft_cursor) => ft_cursor.apply_async(self).await,
            Command::FtProfile(ft_profile) => ft_profile.apply_async(self).await,
            Command::FtExplain(ft_explain) => ft_explain.apply_async(self).await,
            Command::FtTagVals(ft_tagvals) => ft_tagvals.apply_async(self).await,
            Command::FtDict(ft_dict) => ft_dict.apply_async(self).await,
            Command::FtSpellCheck(ft_spellcheck) => ft_spellcheck.apply_async(self).await,
            Command::FtSug(ft_sug) => ft_sug.apply_async(self).await,
            Command::FtSyn(ft_syn) => ft_syn.apply_async(self).await,
            Command::FtUnsupported(ft_unsupported) => ft_unsupported.apply_async().await,
            Command::Lua(lua) => lua.apply_async(self).await,
            Command::VAdd(vadd) => vadd.apply_async(self).await,
            Command::VSim(vsim) => vsim.apply_async(self).await,
            Command::VRem(vrem) => vrem.apply_async(self).await,
            Command::VCard(vcard) => vcard.apply_async(self).await,
            Command::VDim(vdim) => vdim.apply_async(self).await,
            Command::VEmb(vemb) => vemb.apply_async(self).await,
            Command::VGetAttr(vgetattr) => vgetattr.apply_async(self).await,
            Command::VSetAttr(vsetattr) => vsetattr.apply_async(self).await,
            Command::VInfo(vinfo) => vinfo.apply_async(self).await,
            Command::VRandMember(vrandmember) => vrandmember.apply_async(self).await,
            Command::VLinks(vlinks) => vlinks.apply_async(self).await,
            Command::Copy(copy) => {
                let copied = self
                    .copy_key_to_db_async(
                        copy.db_index().unwrap_or(self.db_index as usize) as u16,
                        copy.source(),
                        copy.destination(),
                        copy.replace(),
                    )
                    .await?;
                Ok(Frame::Integer(if copied { 1 } else { 0 }))
            }
            Command::Move(r#move) => {
                let moved = self
                    .move_key_to_db_async(r#move.get_db_index() as u16, r#move.get_key())
                    .await?;
                Ok(Frame::Integer(if moved { 1 } else { 0 }))
            }
            Command::Flushall(_) => {
                self.clear_async().await;
                Ok(Frame::Ok)
            }
            Command::Info(info) => info.apply_async(self).await,
            Command::Save(_) => Ok(Frame::Ok),
            Command::Bgsave(_) => Ok(Frame::Ok),
            other => self.handle_command(other),
        }
    }

}
