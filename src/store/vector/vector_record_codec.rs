fn encode_record<T: Encode>(value: &T) -> Result<Vec<u8>, Error> {
    bincode::encode_to_vec(value, bincode::config::standard())
        .map_err(|_| Error::msg("ERR failed to encode vector record"))
}

fn decode_record<T: Decode<()>>(raw: &[u8]) -> Result<T, Error> {
    bincode::decode_from_slice::<T, _>(raw, bincode::config::standard())
        .and_then(|(value, consumed)| {
            if consumed == raw.len() {
                Ok(value)
            } else {
                Err(bincode::error::DecodeError::Other(
                    "trailing bytes in vector record",
                ))
            }
        })
        .map_err(|_| Error::msg("ERR failed to decode vector record"))
}

fn decode_vector_meta(raw: &[u8]) -> Result<VectorIndexMeta, Error> {
    decode_record::<VectorIndexMeta>(raw).or_else(|current_error| {
        decode_record::<LegacyVectorIndexMetaV1>(raw)
            .map(VectorIndexMeta::from)
            .map_err(|_| current_error)
    })
}

fn decode_vector_hnsw_index(raw: &[u8]) -> Result<VectorHnswIndexBlob, Error> {
    decode_record::<VectorHnswIndexBlob>(raw).or_else(|current_error| {
        decode_record::<LegacyVectorHnswIndexBlobV1>(raw)
            .map(VectorHnswIndexBlob::from_legacy)
            .map_err(|_| current_error)
    })
}
