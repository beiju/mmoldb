use std::num::NonZero;
use num::Integer;
use crate::IngestFatalError;

pub struct Partitioner {
    num_partitions: NonZero<usize>,
    trailing_hexits_for_modulus: usize,
}

impl Partitioner {
    pub fn new(num_partitions: NonZero<usize>) -> Self {
        Self {
            num_partitions,
            // Compute how many least significant hexits we need to accurately compute
            // the modulus between an arbitrary hex number and the given # of partitions
            trailing_hexits_for_modulus: num_partitions.get().lcm(&16),
        }
    }
    
    pub fn partition_for(&self, id: &str) -> Result<usize, IngestFatalError> {
        let ascii_id = ascii::AsciiStr::from_ascii(id.as_bytes())
            .map_err(IngestFatalError::NonAsciiEntityId)?;
        let start_idx = ascii_id.len() - self.trailing_hexits_for_modulus;
        let hex_for_modulus = if start_idx > 0 {
            usize::from_str_radix(ascii_id[start_idx..].as_str(), 16)
                .map_err(IngestFatalError::NonHexEntityId)?
        } else {
            usize::from_str_radix(ascii_id.as_str(), 16)
                .map_err(IngestFatalError::NonHexEntityId)?
        };
        Ok(hex_for_modulus % self.num_partitions.get())
    }
}