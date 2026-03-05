use crate::stringdist::hamming::Hamming;
use crate::stringdist::jaccard::Jaccard;
use anyhow::Result;

pub trait StringDistance {
    fn fuzzy_indices(
        &self,
        left: &Vec<Option<String>>,
        right: &Vec<Option<String>>,
        max_distance: &f64,
        q: &Option<usize>,
        prefix_weight: Option<f64>,
        max_prefix: Option<usize>,
        pool: &rayon::ThreadPool,
    ) -> Result<Vec<(usize, usize, f64)>>;

    fn compare_pairs(
        &self,
        left: &Vec<Option<String>>,
        right: &Vec<Option<String>>,
        max_distance: &f64,
        q: &Option<usize>,
        prefix_weight: Option<f64>,
        max_prefix: Option<usize>,
        pool: &rayon::ThreadPool,
    ) -> Result<(Vec<usize>, Vec<f64>)>;
}

pub enum StringDistMethod {
    Jaccard(Jaccard),
    Hamming(Hamming),
}

impl StringDistMethod {
    pub fn new(method: &str) -> Result<StringDistMethod> {
        match method {
            "jaccard" => Ok(StringDistMethod::Jaccard(Jaccard)),
            "hamming" => Ok(StringDistMethod::Hamming(Hamming)),
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
        match self {
            StringDistMethod::Jaccard(distance) => distance.fuzzy_indices(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
            StringDistMethod::Hamming(distance) => distance.fuzzy_indices(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
        }
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
        match self {
            StringDistMethod::Jaccard(distance) => distance.compare_pairs(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
            StringDistMethod::Hamming(distance) => distance.compare_pairs(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
        }
    }
}
