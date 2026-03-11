// This text distance is adapted from the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use crate::stringdist::string_dist_method::StringDistance;
use crate::stringdist::utils::{get_qgrams, strvec_to_qgram_map};
use anyhow::Result;
use itertools::iproduct;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

// Q-Gram Distance Implementation
pub struct QGram;

impl QGram {
    fn compute(
        &self,
        qgrams_s1: &FxHashMap<&str, usize>,
        qgrams_s2: &FxHashMap<&str, usize>,
    ) -> f64 {
        let mut mismatch_count = 0;

        for (qgram, &count1) in qgrams_s1 {
            let count2 = qgrams_s2.get(qgram).unwrap_or(&0);
            mismatch_count += (count1 as i32 - *count2 as i32).abs();
        }

        for (qgram, &count2) in qgrams_s2 {
            if !qgrams_s1.contains_key(qgram) {
                mismatch_count += count2 as i32;
            }
        }

        mismatch_count as f64
    }

    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        map2_qgrams: &FxHashMap<&str, (FxHashMap<&str, usize>, Vec<usize>)>,
        q: usize,
        max_distance: f64,
    ) -> Option<Vec<(usize, usize, f64)>> {
        let mut idxs: Vec<(usize, usize, f64)> = Vec::new();
        let qg1 = get_qgrams(k1, q);

        for (k2, (qg2, v2)) in map2_qgrams.iter() {
            if &k1 == k2 {
                iproduct!(v1, v2).for_each(|(v1, v2)| {
                    idxs.push((*v1, *v2, 0.));
                });
                continue;
            }

            let dist = self.compute(&qg1, &qg2) as f64;
            if dist <= max_distance {
                iproduct!(v1, v2).for_each(|(a, b)| {
                    idxs.push((*a, *b, dist));
                });
            }
        }

        if idxs.is_empty() {
            None
        } else {
            Some(idxs)
        }
    }
}

impl StringDistance for QGram {
    fn compare_pairs(
        &self,
        left: &Vec<Option<String>>,
        right: &Vec<Option<String>>,
        max_distance: &f64,
        q: &Option<usize>,
        _prefix_weight: Option<f64>,
        _max_prefix: Option<usize>,
        pool: &rayon::ThreadPool,
    ) -> Result<(Vec<usize>, Vec<f64>)> {
        // Extract q
        let q = match q {
            Some(x) => *x as usize,
            None => {
                return Err(anyhow::anyhow!(
                    "Argument 'q' must be supplied for QGram distance"
                ))
            }
        };

        let (keep, dists): (Vec<usize>, Vec<f64>) = pool.install(|| {
            left.par_iter()
                .zip(right)
                .enumerate()
                .filter_map(|(i, (l, r))| {
                    let l = match l {
                        Some(x) => x,
                        None => return None,
                    };
                    let r = match r {
                        Some(x) => x,
                        None => return None,
                    };

                    let l_qgrams = get_qgrams(l, q);
                    let r_qgrams = get_qgrams(r, q);
                    let dist = self.compute(&l_qgrams, &r_qgrams);
                    if dist <= *max_distance {
                        Some((i, dist))
                    } else {
                        None
                    }
                })
                .unzip()
        });
        Ok((keep, dists))
    }

    fn fuzzy_indices(
        &self,
        left: &Vec<Option<String>>,
        right: &Vec<Option<String>>,
        max_distance: &f64,
        q: &Option<usize>,
        _prefix_weight: Option<f64>,
        _max_prefix: Option<usize>,
        pool: &rayon::ThreadPool,
    ) -> anyhow::Result<Vec<(usize, usize, f64)>> {
        // Extract q
        let q = match q {
            Some(x) => *x as usize,
            None => {
                return Err(anyhow::anyhow!(
                    "Argument 'q' must be supplied for QGram distance"
                ))
            }
        };

        let mut map1: FxHashMap<&str, Vec<usize>> = FxHashMap::default();
        left.iter().enumerate().for_each(|(index, val)| match val {
            Some(x) => map1.entry(x).or_default().push(index),
            None => (),
        });

        let mut map2: FxHashMap<&str, Vec<usize>> = FxHashMap::default();
        right.iter().enumerate().for_each(|(index, val)| match val {
            Some(x) => map2.entry(x).or_default().push(index),
            None => (),
        });

        // This map uses qgrams as keys and keeps track of both frequencies
        // and the number of occurrences of each qgram
        let map2_qgrams = strvec_to_qgram_map(right, q)?;

        let idxs: Vec<(usize, usize, f64)> = pool.install(|| {
            map1.par_iter()
                .filter_map(|(k1, v1)| {
                    let out = self.compare_one_to_many(k1, v1, &map2_qgrams, q, *max_distance);
                    out
                })
                .flatten()
                .collect()
        });
        Ok(idxs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_strings() -> Vec<Option<String>> {
        vec![
            Some("apple".to_string()),
            Some("banana".to_string()),
            Some("grape".to_string()),
            None,
            Some("grapefruit".to_string()),
        ]
    }

    #[test]
    fn test_compute_distances() {
        let qgram = QGram;

        let qgrams_s1 = get_qgrams("apple", 2);
        let qgrams_s2 = get_qgrams("grape", 2);
        let distance = qgram.compute(&qgrams_s1, &qgrams_s2);

        assert!(distance >= 0.0, "Distance should be non-negative");

        let qgrams_s3 = get_qgrams("apple", 2);
        let identical_distance = qgram.compute(&qgrams_s1, &qgrams_s3);

        assert_eq!(
            identical_distance, 0.0,
            "Identical strings should have distance 0"
        );
    }

    #[test]
    fn test_compare_pairs() {
        let qgram = QGram;
        let left = test_strings();
        let right = left.clone(); // Comparing with itself

        let max_distance = &1.0; // Example max distance
        let q = &Some(2); // Set q for 2-grams

        // Prepare a thread pool
        let pool = crate::utils::get_pool(None).unwrap();

        let (matches, dists) = qgram
            .compare_pairs(&left, &right, max_distance, q, None, None, &pool)
            .unwrap();

        assert_eq!(
            matches.len(),
            dists.len(),
            "Matches and distances should have the same length"
        );
        assert!(!matches.is_empty(), "Should find at least one match");
    }

    #[test]
    fn test_fuzzy_indices() {
        let qgram = QGram;
        let left = test_strings();
        let right = vec![
            Some("appl".to_string()), // Slightly off from "apple"
            Some("banana".to_string()),
            None,
            Some("grapefruit".to_string()),
        ];

        let max_distance = &1.0; // Example max distance
        let q = &Some(2); // Set q for 2-grams

        // Prepare a thread pool
        let pool = crate::utils::get_pool(None).unwrap();

        let indices = qgram
            .fuzzy_indices(&left, &right, max_distance, q, None, None, &pool)
            .unwrap();

        assert!(!indices.is_empty(), "Indices should not be empty");

        // Further assertions can be made on the content of `indices`
    }

    #[test]
    fn test_invalid_q_value() {
        let qgram = QGram;
        let left = test_strings();
        let right = test_strings();

        let max_distance = &1.0; // Example max distance
        let q = &None; // No q value provided
        let pool = crate::utils::get_pool(None).unwrap();

        let result = qgram.compare_pairs(&left, &right, max_distance, q, None, None, &pool);
        assert!(
            result.is_err(),
            "Should return an error when q is not supplied"
        );
    }
}
