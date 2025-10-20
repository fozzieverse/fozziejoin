use crate::join::{utils::add_null_rows, DistanceData, FuzzyJoin};
use anyhow::{anyhow, Result};
use polars::prelude::*;
use rustc_hash::FxHashSet;

impl FuzzyJoin {
    pub fn left(
        left: &DataFrame,
        right: &DataFrame,
        mut left_idxs: Vec<u32>,
        right_idxs: Vec<u32>,
        dists: DistanceData,
        distance_cols: Option<Vec<String>>,
        suffix: &str,
    ) -> Result<DataFrame> {
        let lhs_len = left.height();
        let lhs_complement: Vec<u32> = (0..=(lhs_len - 1) as u32)
            .filter(|i| !left_idxs.contains(i))
            .collect();

        let comp_len = lhs_complement.iter().len();

        // Add complement to LHS and subset df
        left_idxs.extend(lhs_complement);
        let left_idxs2 = UInt32Chunked::from_slice("left_idx".into(), &left_idxs);
        let left_out = left
            .take(&left_idxs2)
            .map_err(|e| anyhow!("Failed to take left rows: {e}"))?;

        // Now take the right matches
        let right_idxs2 = UInt32Chunked::from_slice("right_idx".into(), &right_idxs);

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

        // Add distance column, if desired
        if let Some(dist_colnames) = distance_cols {
            match dists {
                // Single case: no need to make unique distance column names
                DistanceData::Single(vec) => {
                    let dist_col =
                        Float64Chunked::from_slice(dist_colnames[0].clone().into(), &vec)
                            .into_column();
                    right_out = right_out
                        .hstack(&[dist_col])
                        .map_err(|e| anyhow!("Failed to add distance column: {e}"))?;
                }
                // Many case: need to make sure each distance column is uniquely named
                DistanceData::Many(vecs) => {
                    for (vec, name) in vecs.iter().zip(dist_colnames.iter()) {
                        let dist_col = Float64Chunked::from_slice(name.into(), &vec).into_column();
                        right_out = right_out
                            .hstack(&[dist_col])
                            .map_err(|e| anyhow!("Failed to add distance column: {e}"))?;
                    }
                }
            }
        }

        // Pad with null values
        right_out = add_null_rows(&right_out, comp_len).expect("hi!");

        // Merge df subsets
        let joined = left_out
            .hstack(&right_out.get_columns())
            .map_err(|e| anyhow!("Failed to hstack: {e}"))?;

        Ok(joined)
    }
}
