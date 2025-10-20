use crate::stringdist::jaccard::Jaccard;
use anyhow::Result;

pub enum StringDistMethod {
    Jaccard {},
}

impl StringDistMethod {
    pub fn new(method: &str) -> Result<StringDistMethod, anyhow::Error> {
        match method {
            "jaccard" => Ok(StringDistMethod::Jaccard {}),
            _ => Err(anyhow::anyhow!("Unsupported method `{}`", method)),
        }
    }

    pub fn fuzzy_indices(
        &self,
        left: &Vec<Option<String>>,
        right: &Vec<Option<String>>,
        max_distance: &f64,
        q: &Option<usize>,
        prefix_weight: Option<f64>,
        max_prefix: Option<usize>,
        pool: &rayon::ThreadPool,
    ) -> Result<Vec<(usize, usize, f64)>> {
        let result = match self {
            StringDistMethod::Jaccard {} => Jaccard.fuzzy_indices(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
        }?;

        Ok(result)
    }

    pub fn compare_pairs(
        &self,
        left: &Vec<Option<String>>,
        right: &Vec<Option<String>>,
        max_distance: &f64,
        q: &Option<usize>,
        prefix_weight: Option<f64>,
        max_prefix: Option<usize>,
        pool: &rayon::ThreadPool,
    ) -> Result<(Vec<usize>, Vec<f64>)> {
        let result = match self {
            StringDistMethod::Jaccard {} => Jaccard.compare_pairs(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
        }?;

        Ok(result)
    }
}
