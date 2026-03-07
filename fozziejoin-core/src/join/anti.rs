use crate::join::FuzzyJoin;
use anyhow::{anyhow, Result};
use polars::prelude::*;
use rustc_hash::FxHashSet;

impl FuzzyJoin {
    pub fn anti(left: &DataFrame, left_idxs: Vec<u32>) -> Result<DataFrame> {
        // Use a set for faster lookups
        let left_idx_set: FxHashSet<u32> = left_idxs.into_iter().collect();

        // Generate all indices and filter out those present in left_idx_set
        let idx_complement: Vec<u32> = (0..left.height() as u32) // Using the number of rows in the left DataFrame
            .filter(|&i| !left_idx_set.contains(&i))
            .collect();

        // Create a slice with the filtered indices
        let keep_idxs = UInt32Chunked::from_slice("left_idx".into(), &idx_complement);

        let left_out = left
            .take(&keep_idxs)
            .map_err(|e| anyhow!("Failed to take left rows: {e}"))?;

        Ok(left_out)
    }
}

