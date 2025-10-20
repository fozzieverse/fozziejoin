mod jaccard;
mod string_dist_method;

use crate::join::{DistanceData, FuzzyJoin};
use crate::utils::{format_distance_labels, get_pool, Unzip3};
pub use string_dist_method::StringDistMethod;

use anyhow::{anyhow, Result};
use polars::prelude::*;

pub fn string_distance_join_polars(
    left: DataFrame,
    right: DataFrame,
    left_on: Vec<String>,
    right_on: Vec<String>,
    how: String,
    method: String,
    max_distance: f64,
    q: Option<usize>,
    prefix_weight: Option<f64>,
    max_prefix: Option<usize>,
    distance_col: Option<String>,
    suffix: String,
    nthread: Option<usize>,
) -> Result<DataFrame> {
    // Set up thread pool for use throughout the function
    let pool = get_pool(nthread).map_err(|e| anyhow!("Failed to get thread pool: {e}"))?;

    // Parse string distance method
    let method = StringDistMethod::new(&method)
        .map_err(|e| anyhow!("Invalid string distance method '{}': {}", method, e))?;

    // Get number of columns we are joining on for downstream logic
    let num_join_cols = left_on.iter().len();

    // Process final output distance column names (could be none)
    let distance_cols: Option<Vec<String>> = match distance_col {
        Some(x) => Some(format_distance_labels(x.as_str(), &left_on, &right_on)),
        None => None,
    };

    // Begin processing first iteration of join on columns
    // Left
    let mut left_on_iter = left_on.iter();
    let first_left_key = match left_on_iter.next() {
        Some(x) => x.as_str(),
        None => return Err(anyhow!("You gotta add at least one join pair, bro")),
    };
    // Right
    let mut right_on_iter = right_on.iter();
    let first_right_key: &str = match right_on_iter.next() {
        Some(x) => x.as_str(),
        None => return Err(anyhow!("You gotta add at least one join pair, bro")),
    };

    // Extract string vec from left hand side
    let left_utf8 = left
        .column(first_left_key)
        .map_err(|_| anyhow!("Column '{}' not found in left DataFrame", first_left_key))?
        .str()
        .map_err(|_| anyhow!("Column '{}' is not a string column", first_left_key))?;
    let left_vec: Vec<Option<String>> = left_utf8
        .into_iter()
        .map(|opt| opt.map(|s| s.to_string()))
        .collect();

    // Extract string vec from right hand side
    let right_utf8 = right
        .column(first_right_key)
        .map_err(|_| anyhow!("Column '{}' not found in right DataFrame", first_right_key))?
        .str()
        .map_err(|_| anyhow!("Column '{}' is not a string column", first_right_key))?;
    let right_vec: Vec<Option<String>> = right_utf8
        .into_iter()
        .map(|opt| opt.map(|s| s.to_string()))
        .collect();

    // Determine which of the cartesian pairs survived the first join-on comparison
    let idxs = method.fuzzy_indices(
        &left_vec,
        &right_vec,
        &max_distance,
        &q,
        prefix_weight,
        max_prefix,
        &pool,
    )?;

    // Split into left, right, and distance vecs
    let (mut left_idxs, mut right_idxs, dists): (Vec<u32>, Vec<u32>, Vec<f64>) = idxs
        .into_iter()
        .map(|(a, b, c)| (a as u32, b as u32, c))
        .unzip3();

    if num_join_cols == 1 {
        // If only one column pair in join, create a merged df and return
        let dists = DistanceData::Single(&dists);
        let joined = FuzzyJoin::dispatch_join(
            &left,
            &right,
            left_idxs,
            right_idxs,
            how.as_str(),
            dists,
            distance_cols,
            &suffix,
        );
        return joined;
    } else {
        // Otherwise, we must evaluate candidates pairs for each subsequent
        // join column and keep track of distances

        // Start a vec of distance vecs
        let mut dists: Vec<Vec<f64>> = vec![dists];

        // Start comparing pairs for subsequent columns
        for (l_on, r_on) in left_on_iter.zip(right_on_iter) {
            // Extract string vec from left hand side
            let left_idxs2 = UInt32Chunked::from_slice("left_idx".into(), &left_idxs);
            let left_strs = left
                .column(l_on)
                .map_err(|_| anyhow!("Column '{}' not found in left DataFrame", l_on))?
                .take(&left_idxs2)?
                .str()
                .map_err(|_| anyhow!("Column '{}' is not a string column", l_on))?
                .clone();
            let left_vec: Vec<Option<String>> = left_strs
                .into_iter()
                .map(|opt| opt.map(|s| s.to_string()))
                .collect();

            // Extract string vec from right hand side
            let right_idxs2 = UInt32Chunked::from_slice("right_idx".into(), &right_idxs);
            let right_strs = right
                .column(r_on)
                .map_err(|_| anyhow!("Column '{}' not found in left DataFrame", l_on))?
                .take(&right_idxs2)?
                .str()
                .map_err(|_| anyhow!("Column '{}' is not a string column", l_on))?
                .clone();
            let right_vec: Vec<Option<String>> = right_strs
                .into_iter()
                .map(|opt| opt.map(|s| s.to_string()))
                .collect();

            // Run pairwise comparison function
            let (new_idxs, new_dist) = method.compare_pairs(
                &left_vec,
                &right_vec,
                &max_distance,
                &q,
                prefix_weight,
                max_prefix,
                &pool,
            )?;

            // Keep only the idxs that survived this round of comparison
            left_idxs = new_idxs.iter().map(|&i| left_idxs[i]).collect();
            right_idxs = new_idxs.iter().map(|&i| right_idxs[i]).collect();

            // Update distance vector
            let mut new_distvec: Vec<Vec<f64>> = vec![];
            for distvec in &dists {
                // Subset to distances for surviving pairs
                let tmp: Vec<f64> = new_idxs.iter().map(|&i| distvec[i]).collect();
                new_distvec.push(tmp);
            }
            // Add new distances vector from current iteration
            new_distvec.push(new_dist);

            // Update main distance object
            dists = new_distvec;
        }

        // Perform the dataframe join for the case of many distances
        let dists = DistanceData::Many(&dists);
        let joined = FuzzyJoin::dispatch_join(
            &left,
            &right,
            left_idxs,
            right_idxs,
            how.as_str(),
            dists,
            distance_cols,
            &suffix,
        );
        return joined;
    }
}
