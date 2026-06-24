#[derive(Debug)]
struct OnedisIntegerMergeOperator;

impl OnedisIntegerMergeOperator {
    const TYPE_STRING: u8 = 1;

    fn decode_operand(bytes: &[u8], context: &str) -> KvResult<i64> {
        let array: [u8; 8] = bytes.try_into().map_err(|_| {
            Status::InvalidArgument(format!("{context} must be an 8-byte big-endian i64"))
        })?;
        Ok(i64::from_be_bytes(array))
    }

    fn decode_existing(bytes: &[u8]) -> KvResult<(u64, i64)> {
        if bytes.len() < 17 || bytes[16] != Self::TYPE_STRING {
            return Err(Status::InvalidArgument(
                "existing value is not an onedis string".to_string(),
            ));
        }
        let expire_ms = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let text = std::str::from_utf8(&bytes[17..]).map_err(|_| {
            Status::InvalidArgument("existing string value is not valid UTF-8".to_string())
        })?;
        let value = text.parse::<i64>().map_err(|_| {
            Status::InvalidArgument("existing string value is not an integer".to_string())
        })?;
        Ok((expire_ms, value))
    }

    fn encode_string(value: i64, expire_ms: u64) -> Vec<u8> {
        let value = value.to_string();
        let mut encoded = Vec::with_capacity(17 + value.len());
        encoded.extend_from_slice(&expire_ms.to_be_bytes());
        encoded.extend_from_slice(&0u64.to_be_bytes());
        encoded.push(Self::TYPE_STRING);
        encoded.extend_from_slice(value.as_bytes());
        encoded
    }
}

impl MergeOperate for OnedisIntegerMergeOperator {
    fn name(&self) -> &str {
        "onedis_integer"
    }

    fn full_merge(
        &self,
        _key: &[u8],
        existing_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> KvResult<Option<Vec<u8>>> {
        let (expire_ms, mut value) = match existing_value {
            Some(existing) => Self::decode_existing(existing)?,
            None => (0, 0),
        };
        for operand in operands {
            let delta = Self::decode_operand(operand, "merge operand")?;
            value = value.checked_add(delta).ok_or_else(|| {
                Status::InvalidArgument("integer merge would overflow".to_string())
            })?;
        }
        Ok(Some(Self::encode_string(value, expire_ms)))
    }

    fn partial_merge(&self, _key: &[u8], left: &[u8], right: &[u8]) -> KvResult<Vec<u8>> {
        let left = Self::decode_operand(left, "left merge operand")?;
        let right = Self::decode_operand(right, "right merge operand")?;
        let merged = left.checked_add(right).ok_or_else(|| {
            Status::InvalidArgument("integer merge operand would overflow".to_string())
        })?;
        Ok(merged.to_be_bytes().to_vec())
    }
}
