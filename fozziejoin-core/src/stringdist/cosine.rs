// This text distance is adapted from the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use crate::stringdist::get_qgrams;
use crate::stringdist::string_dist_method::StringDistance;
use crate::stringdist::strvec_to_qgram_map;
use anyhow::Result;
use itertools::iproduct;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

// Cosine Distance Implementation
pub struct Cosine;

impl Cosine {
    fn compute(
        &self,
        qgrams_s1: &FxHashMap<&str, usize>,
        qgrams_s2: &FxHashMap<&str, usize>,
    ) -> f64 {
        let mut dot_product = 0;
        let mut norm_s1 = 0;
        let mut norm_s2 = 0;

        // Compute dot product and vector norms
        for (qgram, &count1) in qgrams_s1 {
            if let Some(&count2) = qgrams_s2.get(qgram) {
                dot_product += count1 * count2;
            }
            norm_s1 += count1 * count1;
        }

        for &count2 in qgrams_s2.values() {
            norm_s2 += count2 * count2;
        }

        if norm_s1 == 0 || norm_s2 == 0 {
            return 1.0; // Maximum distance if no similarity
        }

        let similarity = dot_product as f64 / (norm_s1 as f64).sqrt() / (norm_s2 as f64).sqrt();
        1.0 - similarity // Convert similarity to edit distance
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

// Define a trait for string distance calculations
impl StringDistance for Cosine {
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
                    "Argument 'q' must be supplied for Cosine distance"
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
                    "Argument 'q' must be supplied for Cosine distance"
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

    fn get_test_qgrams() -> Vec<Option<String>> {
        vec![
            Some("apple".to_string()),
            Some("banana".to_string()),
            Some("grape".to_string()),
            None,
            Some("grapefruit".to_string()),
        ]
    }

    #[test]
    fn test_strvec_to_qgram_map() {
        let strvec = get_test_qgrams();
        let q = 2; // Using 2-grams for this test
        let map = strvec_to_qgram_map(&strvec, q).unwrap();

        assert_eq!(map.len(), 4); // 4 valid strings

        assert!(map.contains_key("apple"));
        assert!(map.contains_key("banana"));
        assert!(map.contains_key("grape"));
        assert!(map.contains_key("grapefruit"));

        // Check q-gram frequencies for "apple"
        let (freq_map, indices) = map.get("apple").unwrap();
        assert_eq!(indices, &[1]); // Apple is at index 1
        assert!(freq_map.len() > 0); // Should have some q-grams
    }

    #[test]
    fn test_cosine_compute() {
        let cosine = Cosine;
        let qgrams_s1 = get_qgrams("apple", 2);
        let qgrams_s2 = get_qgrams("applf", 2);

        let distance = cosine.compute(&qgrams_s1, &qgrams_s2);
        assert!(distance >= 0.0 && distance <= 1.0); // Distance should be normalized
    }

    #[test]
    fn test_compare_pairs() {
        let cosine = Cosine;
        let left = get_test_qgrams();
        let right = get_test_qgrams();

        let max_distance = &0.5; // Example distance
        let q = &Some(2); // Set q for 2-grams

        // Prepare a thread pool
        let pool = crate::utils::get_pool(None).unwrap();

        let (matches, dists) = cosine
            .compare_pairs(&left, &right, max_distance, q, None, None, &pool)
            .unwrap();

        assert!(!matches.is_empty()); // Expect matches to not be empty
        assert_eq!(matches.len(), dists.len()); // Matches and distances should have the same length
    }

    #[test]
    fn test_fuzzy_indices() {
        let cosine = Cosine;
        let left = get_test_qgrams();
        let right = get_test_qgrams();

        let max_distance = &0.5; // Example distance
        let q = &Some(2); // Set q for 2-grams

        // Prepare a thread pool
        let pool = crate::utils::get_pool(None).unwrap();

        let indices = cosine
            .fuzzy_indices(&left, &right, max_distance, q, None, None, &pool)
            .unwrap();

        assert!(!indices.is_empty());
    }
}
