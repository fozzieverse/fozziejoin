use crate::join::FuzzyJoin;
use anyhow::{anyhow, Result};
use polars::prelude::*;

impl FuzzyJoin {
    pub fn semi(left: &DataFrame, _right: &DataFrame, left_idxs: Vec<u32>) -> Result<DataFrame> {
        // Create a slice with the filtered left indices
        let left_idxs2 = UInt32Chunked::from_slice("left_idx".into(), &left_idxs);

        let left_out = left
            .take(&left_idxs2)
            .map_err(|e| anyhow!("Failed to take left rows: {e}"))?;

        Ok(left_out)
    }
}
