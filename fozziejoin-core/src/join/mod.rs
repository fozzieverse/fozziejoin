use anyhow::{anyhow, Result};
use polars::prelude::*;
use rustc_hash::FxHashSet;

pub struct Join;

impl Join {
    pub fn inner(
        left: &DataFrame,
        right: &DataFrame,
        left_idxs: &Vec<u32>,
        right_idxs: &Vec<u32>,
        dists: &Vec<f64>,
        distance_col: Option<String>,
        suffix: &str,
    ) -> Result<DataFrame> {
        // Use fuzzy indices to subset each component df
        let left_idxs2 = UInt32Chunked::from_slice("left_idx".into(), &left_idxs);
        let right_idxs2 = UInt32Chunked::from_slice("right_idx".into(), &right_idxs);

        let left_out = left
            .take(&left_idxs2)
            .map_err(|e| anyhow!("Failed to take left rows: {e}"))?;
        let mut right_out = right
            .take(&right_idxs2)
            .map_err(|e| anyhow!("Failed to take right rows: {e}"))?;

        // Detect shared column names
        let left_names: FxHashSet<&str> = left_out.get_column_names_str().iter().cloned().collect();
        for col in right_out.get_column_names_owned() {
            // If shared, add suffix to right hand side
            if left_names.contains(col.as_str()) {
                right_out
                    .rename(col.as_str(), format!("{}{}", col, suffix).into())
                    .expect("Ruhroh");
            }
        }

        // Merge df subsets
        let mut joined = left_out
            .hstack(&right_out.get_columns())
            .map_err(|e| anyhow!("Failed to hstack: {e}"))?;

        // Add distance column, if desired
        if let Some(dist_colname) = distance_col {
            let dist_col = Float64Chunked::from_slice(dist_colname.into(), &dists).into_column();
            joined = joined
                .hstack(&[dist_col])
                .map_err(|e| anyhow!("Failed to add distance column: {e}"))?;
        }

        Ok(joined)
    }
}
