mod inner;
mod left;
mod utils;

use anyhow::{anyhow, Result};
use polars::prelude::*;

pub struct FuzzyJoin;

impl FuzzyJoin {
    pub fn dispatch_join(
        left: &DataFrame,
        right: &DataFrame,
        left_idxs: Vec<u32>,
        right_idxs: Vec<u32>,
        how: &str,
        dists: DistanceData,
        distance_cols: Option<Vec<String>>,
        suffix: &str,
    ) -> Result<DataFrame> {
        match how {
            "inner" => FuzzyJoin::inner(
                left,
                right,
                left_idxs,
                right_idxs,
                dists,
                distance_cols,
                suffix,
            ),
            "left" => FuzzyJoin::left(
                left,
                right,
                left_idxs,
                right_idxs,
                dists,
                distance_cols,
                suffix,
            ),
            _ => Err(anyhow!("Join type not supported")),
        }
    }
}

pub enum DistanceData<'a> {
    Single(&'a Vec<f64>),
    Many(&'a Vec<Vec<f64>>),
}
