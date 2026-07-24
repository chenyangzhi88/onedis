use super::*;

const HLL_MAGIC: &[u8; 4] = b"HYLL";
const HLL_DENSE: u8 = 0;
const HLL_SPARSE: u8 = 1;
const HLL_HEADER_SIZE: usize = 16;
const HLL_PRECISION: usize = 14;
const HLL_REGISTERS: usize = 1 << HLL_PRECISION;
const HLL_REGISTER_BITS: usize = 6;
const HLL_DENSE_BYTES: usize = HLL_REGISTERS * HLL_REGISTER_BITS / 8;
const HLL_DENSE_SIZE: usize = HLL_HEADER_SIZE + HLL_DENSE_BYTES;
const HLL_SPARSE_MAX_BYTES: usize = 3_000;
const HLL_HASH_SEED: u64 = 0xadc8_3b19;
const HLL_ALPHA_INF: f64 = 0.721_347_520_444_481_7;
const INVALID_HLL_ERROR: &str = "WRONGTYPE Key is not a valid HyperLogLog string value.";

impl Db {
    pub fn hll_add(&self, key: &str, elements: &[Vec<u8>]) -> Result<bool, Error> {
        let existing = self.get_string_bytes(key)?;
        let created = existing.is_none();
        let mut registers = match existing {
            Some(value) => decode_hll_registers(&value)?,
            None => vec![0u8; HLL_REGISTERS],
        };
        let mut changed = created;
        for element in elements {
            changed |= register_add(&mut registers, element);
        }
        if changed {
            self.set_string_bytes(
                key.to_string(),
                encode_hll(&registers),
                SetExpiration::KeepTtl,
                SetCondition::Always,
                false,
            )?;
        }
        Ok(changed)
    }

    pub async fn hll_add_async(&self, key: &str, elements: &[Vec<u8>]) -> Result<bool, Error> {
        self.mutate_string_bytes_if_changed_async(key, |value, exists| {
            let mut registers = if exists {
                decode_hll_registers(value)?
            } else {
                vec![0u8; HLL_REGISTERS]
            };
            let mut changed = !exists;
            for element in elements {
                changed |= register_add(&mut registers, element);
            }
            if changed {
                *value = encode_hll(&registers);
            }
            Ok((changed, changed))
        })
        .await
    }

    pub fn hll_count(&self, keys: &[String]) -> Result<u64, Error> {
        let mut registers = vec![0u8; HLL_REGISTERS];
        for key in keys {
            if let Some(value) = self.get_string_bytes(key)? {
                merge_registers(&mut registers, &decode_hll_registers(&value)?)?;
            }
        }
        Ok(estimate_cardinality(&registers))
    }

    pub async fn hll_count_async(&self, keys: &[String]) -> Result<u64, Error> {
        let mut registers = vec![0u8; HLL_REGISTERS];
        for key in keys {
            if let Some(value) = self.get_string_bytes_async(key).await? {
                merge_registers(&mut registers, &decode_hll_registers(&value)?)?;
            }
        }
        Ok(estimate_cardinality(&registers))
    }

    pub fn hll_merge(&self, destination: &str, sources: &[String]) -> Result<(), Error> {
        let mut registers = vec![0u8; HLL_REGISTERS];
        if let Some(value) = self.get_string_bytes(destination)? {
            merge_registers(&mut registers, &decode_hll_registers(&value)?)?;
        }
        for key in sources {
            if key == destination {
                continue;
            }
            if let Some(value) = self.get_string_bytes(key)? {
                merge_registers(&mut registers, &decode_hll_registers(&value)?)?;
            }
        }
        self.set_string_bytes(
            destination.to_string(),
            encode_hll(&registers),
            SetExpiration::KeepTtl,
            SetCondition::Always,
            false,
        )?;
        Ok(())
    }

    pub async fn hll_merge_async(
        &self,
        destination: &str,
        sources: &[String],
    ) -> Result<(), Error> {
        let mut source_registers = vec![0u8; HLL_REGISTERS];
        for key in sources {
            if key == destination {
                continue;
            }
            if let Some(value) = self.get_string_bytes_async(key).await? {
                merge_registers(&mut source_registers, &decode_hll_registers(&value)?)?;
            }
        }
        self.mutate_string_bytes_if_changed_async(destination, |value, exists| {
            let mut registers = source_registers.clone();
            if exists {
                merge_registers(&mut registers, &decode_hll_registers(value)?)?;
            }
            *value = encode_hll(&registers);
            Ok(((), true))
        })
        .await
    }
}

fn empty_dense_hll() -> Vec<u8> {
    let mut dense = vec![0u8; HLL_DENSE_SIZE];
    dense[..HLL_MAGIC.len()].copy_from_slice(HLL_MAGIC);
    dense[4] = HLL_DENSE;
    invalidate_cardinality(&mut dense);
    dense
}

fn encode_hll(registers: &[u8]) -> Vec<u8> {
    sparse_from_registers(registers).unwrap_or_else(|| dense_from_registers(registers))
}

fn dense_from_registers(registers: &[u8]) -> Vec<u8> {
    let mut dense = empty_dense_hll();
    for (index, value) in registers.iter().copied().enumerate() {
        dense_set_register(&mut dense, index, value);
    }
    dense
}

fn sparse_from_registers(registers: &[u8]) -> Option<Vec<u8>> {
    if registers.len() != HLL_REGISTERS {
        return None;
    }
    let mut sparse = vec![0u8; HLL_HEADER_SIZE];
    sparse[..4].copy_from_slice(HLL_MAGIC);
    sparse[4] = HLL_SPARSE;
    invalidate_cardinality(&mut sparse);
    let mut index = 0;
    while index < registers.len() {
        let value = registers[index];
        let mut run = 1;
        while index + run < registers.len() && registers[index + run] == value {
            run += 1;
        }
        let mut remaining = run;
        if value == 0 {
            while remaining > 0 {
                if remaining > 64 {
                    let chunk = remaining.min(HLL_REGISTERS);
                    let encoded = chunk - 1;
                    sparse.push(0x40 | ((encoded >> 8) as u8 & 0x3f));
                    sparse.push(encoded as u8);
                    remaining -= chunk;
                } else {
                    let chunk = remaining.min(64);
                    sparse.push((chunk - 1) as u8);
                    remaining -= chunk;
                }
            }
        } else {
            if value > 32 {
                return None;
            }
            while remaining > 0 {
                let chunk = remaining.min(4);
                sparse.push(0x80 | ((value - 1) << 2) | (chunk - 1) as u8);
                remaining -= chunk;
            }
        }
        if sparse.len() > HLL_SPARSE_MAX_BYTES {
            return None;
        }
        index += run;
    }
    Some(sparse)
}

fn decode_hll_registers(value: &[u8]) -> Result<Vec<u8>, Error> {
    if value.len() < HLL_HEADER_SIZE || value.get(..4) != Some(HLL_MAGIC) {
        return Err(Error::msg(INVALID_HLL_ERROR));
    }
    match value[4] {
        HLL_DENSE if value.len() == HLL_DENSE_SIZE => Ok((0..HLL_REGISTERS)
            .map(|index| dense_get_register(value, index))
            .collect()),
        HLL_SPARSE => decode_sparse_registers(&value[HLL_HEADER_SIZE..]),
        _ => Err(Error::msg(INVALID_HLL_ERROR)),
    }
}

fn decode_sparse_registers(encoded: &[u8]) -> Result<Vec<u8>, Error> {
    let mut registers = Vec::with_capacity(HLL_REGISTERS);
    let mut index = 0;
    while index < encoded.len() && registers.len() < HLL_REGISTERS {
        let opcode = encoded[index];
        if opcode & 0xc0 == 0 {
            registers.resize(registers.len() + usize::from(opcode & 0x3f) + 1, 0);
            index += 1;
        } else if opcode & 0xc0 == 0x40 {
            let Some(next) = encoded.get(index + 1).copied() else {
                return Err(Error::msg(INVALID_HLL_ERROR));
            };
            let run = ((usize::from(opcode & 0x3f) << 8) | usize::from(next)) + 1;
            registers.resize(registers.len() + run, 0);
            index += 2;
        } else {
            let value = ((opcode >> 2) & 0x1f) + 1;
            let run = usize::from(opcode & 0x03) + 1;
            registers.resize(registers.len() + run, value);
            index += 1;
        }
        if registers.len() > HLL_REGISTERS {
            return Err(Error::msg(INVALID_HLL_ERROR));
        }
    }
    if index != encoded.len() || registers.len() != HLL_REGISTERS {
        return Err(Error::msg(INVALID_HLL_ERROR));
    }
    Ok(registers)
}

fn register_add(registers: &mut [u8], element: &[u8]) -> bool {
    let hash = murmur_hash64a(element, HLL_HASH_SEED);
    let register = (hash as usize) & (HLL_REGISTERS - 1);
    let remaining = (hash >> HLL_PRECISION) | (1u64 << (64 - HLL_PRECISION));
    let rank = remaining.trailing_zeros() as u8 + 1;
    if rank > registers[register] {
        registers[register] = rank;
        true
    } else {
        false
    }
}

fn dense_get_register(dense: &[u8], register: usize) -> u8 {
    let bit = register * HLL_REGISTER_BITS;
    let byte = HLL_HEADER_SIZE + bit / 8;
    let shift = bit & 7;
    let low = u16::from(dense[byte]);
    let high = dense.get(byte + 1).copied().map(u16::from).unwrap_or(0);
    ((low | (high << 8)) >> shift) as u8 & 0x3f
}

fn dense_set_register(dense: &mut [u8], register: usize, value: u8) {
    let bit = register * HLL_REGISTER_BITS;
    let byte = HLL_HEADER_SIZE + bit / 8;
    let shift = bit & 7;
    let mask = 0x3fu16 << shift;
    let high = dense.get(byte + 1).copied().map(u16::from).unwrap_or(0);
    let mut word = u16::from(dense[byte]) | (high << 8);
    word = (word & !mask) | (u16::from(value & 0x3f) << shift);
    dense[byte] = word as u8;
    if let Some(next) = dense.get_mut(byte + 1) {
        *next = (word >> 8) as u8;
    }
}

fn invalidate_cardinality(dense: &mut [u8]) {
    dense[15] |= 0x80;
}

fn merge_registers(destination: &mut [u8], source: &[u8]) -> Result<(), Error> {
    if destination.len() != HLL_REGISTERS || source.len() != HLL_REGISTERS {
        return Err(Error::msg(INVALID_HLL_ERROR));
    }
    for (destination, source) in destination.iter_mut().zip(source) {
        *destination = (*destination).max(*source);
    }
    Ok(())
}

fn estimate_cardinality(registers: &[u8]) -> u64 {
    let mut histogram = [0u64; 64];
    for register in registers {
        histogram[usize::from(*register)] += 1;
    }
    let m = HLL_REGISTERS as f64;
    let q = 64 - HLL_PRECISION;
    let mut z = m * hll_tau((m - histogram[q + 1] as f64) / m);
    for count in histogram[1..=q].iter().rev() {
        z += *count as f64;
        z *= 0.5;
    }
    z += m * hll_sigma(histogram[0] as f64 / m);
    (HLL_ALPHA_INF * m * m / z).round() as u64
}

fn hll_sigma(mut value: f64) -> f64 {
    if value == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = value;
    loop {
        value *= value;
        let previous = z;
        z += value * y;
        y += y;
        if previous == z {
            return z;
        }
    }
}

fn hll_tau(mut value: f64) -> f64 {
    if value == 0.0 || value == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - value;
    loop {
        value = value.sqrt();
        let previous = z;
        y *= 0.5;
        z -= (1.0 - value).powi(2) * y;
        if previous == z {
            return z / 3.0;
        }
    }
}

fn murmur_hash64a(bytes: &[u8], seed: u64) -> u64 {
    const MULTIPLIER: u64 = 0xc6a4_a793_5bd1_e995;
    const ROTATION: u32 = 47;
    let mut hash = seed ^ (bytes.len() as u64).wrapping_mul(MULTIPLIER);
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let mut data = [0u8; 8];
        data.copy_from_slice(chunk);
        let mut value = u64::from_le_bytes(data);
        value = value.wrapping_mul(MULTIPLIER);
        value ^= value >> ROTATION;
        value = value.wrapping_mul(MULTIPLIER);
        hash ^= value;
        hash = hash.wrapping_mul(MULTIPLIER);
    }
    let remainder = chunks.remainder();
    for (index, byte) in remainder.iter().copied().enumerate() {
        hash ^= u64::from(byte) << (index * 8);
    }
    if !remainder.is_empty() {
        hash = hash.wrapping_mul(MULTIPLIER);
    }
    hash ^= hash >> ROTATION;
    hash = hash.wrapping_mul(MULTIPLIER);
    hash ^ (hash >> ROTATION)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_and_sparse_redis_encodings_round_trip() {
        let mut registers = vec![0u8; HLL_REGISTERS];
        assert!(register_add(&mut registers, b"alpha"));
        assert!(!register_add(&mut registers, b"alpha"));
        let dense = dense_from_registers(&registers);
        assert_eq!(decode_hll_registers(&dense).unwrap().len(), HLL_REGISTERS);

        let sparse = encode_hll(&vec![0; HLL_REGISTERS]);
        assert_eq!(
            decode_hll_registers(&sparse).unwrap(),
            vec![0; HLL_REGISTERS]
        );
    }

    #[test]
    fn cardinality_is_zero_for_empty_and_exact_for_tiny_inputs() {
        let empty = vec![0u8; HLL_REGISTERS];
        assert_eq!(estimate_cardinality(&empty), 0);
        let mut registers = vec![0u8; HLL_REGISTERS];
        for value in [b"a".as_slice(), b"b", b"c"] {
            register_add(&mut registers, value);
        }
        assert_eq!(estimate_cardinality(&registers), 3);
        assert_eq!(
            registers
                .iter()
                .copied()
                .enumerate()
                .filter(|(_, value)| *value != 0)
                .collect::<Vec<_>>(),
            vec![(8436, 1), (12711, 2), (15780, 1)]
        );

        let mut redis_sparse = vec![0u8; HLL_HEADER_SIZE];
        redis_sparse[..4].copy_from_slice(HLL_MAGIC);
        redis_sparse[4] = HLL_SPARSE;
        redis_sparse[8] = 3;
        redis_sparse.extend_from_slice(&[
            0x60, 0xf3, 0x80, 0x50, 0xb1, 0x84, 0x4b, 0xfb, 0x80, 0x42, 0x5a,
        ]);
        assert_eq!(
            estimate_cardinality(&decode_hll_registers(&redis_sparse).unwrap()),
            3
        );
    }
}
