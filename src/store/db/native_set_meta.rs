use super::*;

impl Db {
    pub(in crate::store::db) fn set_meta(&self, key: &str) -> Result<Option<SetMeta>, Error> {
        self.expire_if_needed(key);

        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return Ok(None);
        };

        if let Some(header) = decode_meta_header(&raw)
            && header.type_tag != TYPE_SET
        {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }

        let Some(meta) = decode_set_meta(&raw) else {
            return Err(Error::msg("Failed to decode set metadata"));
        };

        Ok(Some(meta))
    }

    pub(in crate::store::db) async fn set_meta_async(
        &self,
        key: &str,
    ) -> Result<Option<SetMeta>, Error> {
        self.expire_if_needed_async(key).await;

        let Some(raw) = self.store.get_raw_async(&self.mk(key)).await else {
            return Ok(None);
        };

        if let Some(header) = decode_meta_header(&raw)
            && header.type_tag != TYPE_SET
        {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }

        let Some(meta) = decode_set_meta(&raw) else {
            return Err(Error::msg("Failed to decode set metadata"));
        };

        Ok(Some(meta))
    }
}
