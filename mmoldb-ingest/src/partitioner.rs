use crate::IngestFatalError;
use num::Integer;
use std::num::NonZero;

enum PartitionerType {
    Numbered {
        num_partitions: NonZero<usize>,
        trailing_hexits_for_modulus: usize,
    },
    SinglePartition,
}

pub struct Partitioner(PartitionerType);

impl Partitioner {
    pub fn with_partitions(num_partitions: NonZero<usize>) -> Self {
        Self(PartitionerType::Numbered {
            num_partitions,
            // Compute how many least significant hexits we need to accurately compute
            // the modulus between an arbitrary hex number and the given # of partitions
            trailing_hexits_for_modulus: num_partitions.get().lcm(&16),
        })
    }

    pub fn with_single_partition() -> Self {
        Self(PartitionerType::SinglePartition)
    }

    pub fn num_partitions(&self) -> NonZero<usize> {
        match self.0 {
            PartitionerType::Numbered { num_partitions, .. } => num_partitions,
            PartitionerType::SinglePartition => nonzero_lit::usize!(1),
        }
    }

    pub fn partition_for(&self, id: &str) -> Result<usize, IngestFatalError> {
        match self.0 {
            PartitionerType::Numbered { num_partitions, trailing_hexits_for_modulus } => {
                let ascii_id = ascii::AsciiStr::from_ascii(id.as_bytes())
                    .map_err(IngestFatalError::NonAsciiEntityId)?;
                let start_idx = ascii_id.len().checked_sub(trailing_hexits_for_modulus)
                    .ok_or_else(|| {
                        IngestFatalError::TooShortEntityId {
                            actual_len: ascii_id.len(),
                            expected_minimum_len: trailing_hexits_for_modulus,
                        }
                    } )?;
                let hex_for_modulus = if start_idx > 0 {
                    usize::from_str_radix(ascii_id[start_idx..].as_str(), 16)
                        .map_err(IngestFatalError::NonHexEntityId)?
                } else {
                    usize::from_str_radix(ascii_id.as_str(), 16)
                        .map_err(IngestFatalError::NonHexEntityId)?
                };
                Ok(hex_for_modulus % num_partitions.get())
            }
            PartitionerType::SinglePartition => {
                Ok(0)
            }
        }
    }
}
