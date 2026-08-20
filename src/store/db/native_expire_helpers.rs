use super::*;

impl Db {
    pub(in crate::store::db) fn get_expire_and_version(
        &self,
        key: &str,
    ) -> Result<(u64, u64), Error> {
        if let Some(raw) = self.store.get_raw(&self.mk(key))?
            && let Some(header) = decode_meta_header(&raw)
        {
            return Ok((header.expire_ms, header.version));
        }
        Ok((0, 0))
    }
}
