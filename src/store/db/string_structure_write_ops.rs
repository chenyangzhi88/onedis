use super::*;

impl Db {
    // ========================================================================
    // 核心 KV 操作方法
    // ========================================================================

    /**
     * 插入键值（重置过期时间）
     *
     * 用于 SET 等命令，插入后过期时间清零。
     */
    pub fn insert(&self, key: String, value: Structure) {
        self.changes.fetch_add(1, Ordering::Relaxed);
        if let Structure::String(value) = value {
            self.write_string(&key, value.as_bytes(), 0);
            return;
        }
        let version = self.next_persisted_version();
        self.write_structure(&key, &value, 0, version);
    }

    /**
     * 更新键值（保留原有过期时间）
     *
     * 用于 INCR、LPUSH 等修改数据但不改变 TTL 的命令。
     * 替代原来的 get_mut() 就地修改模式。
     */
    pub fn update(&self, key: String, value: Structure) {
        self.changes.fetch_add(1, Ordering::Relaxed);
        let (expire_ms, version) = self.get_expire_and_version(&key);
        self.write_structure(&key, &value, expire_ms, version);
    }
}
