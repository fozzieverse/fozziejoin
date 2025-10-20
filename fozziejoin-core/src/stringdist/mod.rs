mod jaccard;
mod string_dist_method;

use crate::join::Join;
use crate::utils::{get_pool, Unzip3};
pub use string_dist_method::StringDistMethod;

use anyhow::{anyhow, Result};
use polars::prelude::*;

pub fn string_distance_join_polars(
    left: DataFrame,
    right: DataFrame,
    left_on: Vec<String>,
    right_on: Vec<String>,
    _how: String,
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

    // Pull user-specified string distance method
    let method = StringDistMethod::new(&method).expect("ohno");

    let num_join_cols = left_on.iter().len();
    let left_key: &str = match left_on.iter().next() {
        Some(x) => x.as_str(),
        None => return Err(anyhow!("You gotta add at least one join pair, bro")),
    };

    let right_key: &str = match right_on.iter().next() {
        Some(x) => x.as_str(),
        None => return Err(anyhow!("You gotta add at least one join pair, bro")),
    };

    let first_dist_col: Option<String> = match distance_col {
        Some(x) => {
            match num_join_cols {
                // Single distance: use user input directly
                1 => Some(x),
                // Many distances: append left and right keys to differentiate
                2.. => Some(format!("{}_{}_{}", x, left_key, right_key)),
                // There is always at least one join column pair
                _ => return Err(anyhow!("This should not be happening!")),
            }
        }
        None => None,
    };

    // Extract string vec from left hand side
    let left_utf8 = left
        .column(left_key)
        .map_err(|_| anyhow!("Column '{}' not found in left DataFrame", left_key))?
        .str()
        .map_err(|_| anyhow!("Column '{}' is not a string column", left_key))?;
    let left_vec: Vec<Option<String>> = left_utf8
        .into_iter()
        .map(|opt| opt.map(|s| s.to_string()))
        .collect();

    // Extract string vec from right hand side
    let right_utf8 = right
        .column(right_key)
        .map_err(|_| anyhow!("Column '{}' not found in right DataFrame", right_key))?
        .str()
        .map_err(|_| anyhow!("Column '{}' is not a string column", right_key))?;
    let right_vec: Vec<Option<String>> = right_utf8
        .into_iter()
        .map(|opt| opt.map(|s| s.to_string()))
        .collect();

    // First pass: run fuzzy indices function
    let idxs = method.fuzzy_indices(
        &left_vec,
        &right_vec,
        &max_distance,
        &q,
        prefix_weight,
        max_prefix,
        &pool,
    )?;
    let (left_idxs, right_idxs, dists): (Vec<u32>, Vec<u32>, Vec<f64>) = idxs
        .into_iter()
        .map(|(a, b, c)| (a as u32, b as u32, c))
        .unzip3();

    let joined = Join::inner(
        &left,
        &right,
        &left_idxs,
        &right_idxs,
        &dists,
        first_dist_col,
        &suffix,
    );
    joined
}
