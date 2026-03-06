use crate::stringdist::damerau_levenshtein::DamerauLevenshtein;
use crate::stringdist::hamming::Hamming;
use crate::stringdist::jaccard::Jaccard;
use crate::stringdist::lcs::LCS;
use crate::stringdist::levenshtein::Levenshtein;
use crate::stringdist::osa::OSA;
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
    Levenshtein(Levenshtein),
    OSA(OSA),
    DamerauLevenshtein(DamerauLevenshtein),
    LCS(LCS),
}

impl StringDistMethod {
    pub fn new(method: &str) -> Result<StringDistMethod> {
        match method {
            "jaccard" => Ok(StringDistMethod::Jaccard(Jaccard)),
            "hamming" => Ok(StringDistMethod::Hamming(Hamming)),
            "levenshtein" | "lv" => Ok(StringDistMethod::Levenshtein(Levenshtein)),
            "osa" => Ok(StringDistMethod::OSA(OSA)),
            "damerau_levenshtein" | "dl" => {
                Ok(StringDistMethod::DamerauLevenshtein(DamerauLevenshtein))
            }
            "lcs" => Ok(StringDistMethod::LCS(LCS)),
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
            StringDistMethod::Levenshtein(distance) => distance.fuzzy_indices(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
            StringDistMethod::OSA(distance) => distance.fuzzy_indices(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
            StringDistMethod::DamerauLevenshtein(distance) => distance.fuzzy_indices(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
            StringDistMethod::LCS(distance) => distance.fuzzy_indices(
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
            StringDistMethod::Levenshtein(distance) => distance.compare_pairs(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
            StringDistMethod::OSA(distance) => distance.compare_pairs(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
            StringDistMethod::DamerauLevenshtein(distance) => distance.compare_pairs(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
            StringDistMethod::LCS(distance) => distance.compare_pairs(
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
