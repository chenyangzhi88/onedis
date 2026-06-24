fn encode_record<T: Encode>(value: &T) -> Result<Vec<u8>, Error> {
    bincode::encode_to_vec(value, bincode::config::standard())
        .map_err(|_| Error::msg("ERR failed to encode vector record"))
}

fn decode_record<T: Decode<()>>(raw: &[u8]) -> Result<T, Error> {
    bincode::decode_from_slice::<T, _>(raw, bincode::config::standard())
        .map(|(value, _)| value)
        .map_err(|_| Error::msg("ERR failed to decode vector record"))
}
