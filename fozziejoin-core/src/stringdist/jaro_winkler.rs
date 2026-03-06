use crate::stringdist::string_dist_method::StringDistance;
use anyhow::Result;
use itertools::iproduct;
use rapidfuzz::distance::jaro as jaro_rf;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

pub struct JaroWinkler;
impl StringDistance for JaroWinkler {
    fn fuzzy_indices(
        &self,
        left: &Vec<Option<String>>,
        right: &Vec<Option<String>>,
        max_distance: &f64,
        _q: &Option<usize>,
        prefix_weight: Option<f64>,
        max_prefix: Option<usize>,
        pool: &rayon::ThreadPool,
    ) -> Result<Vec<(usize, usize, f64)>> {
        // Extract prefix weight
        let prefix_weight = match prefix_weight {
            Some(x) => x,
            None => {
                return Err(anyhow::anyhow!(
                    "Argument 'prefix weight' must be supplied for jaro-winkler distance"
                ))
            }
        };

        // Extract max prefix size
        let max_prefix = match max_prefix {
            Some(x) => x,
            None => {
                return Err(anyhow::anyhow!(
                    "Argument 'prefix weight' must be supplied for jaro-winkler distance"
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

        let idxs: Vec<(usize, usize, f64)> = pool.install(|| {
            map1.par_iter()
                .filter_map(|(k1, v1)| {
                    self.compare_one_to_many(
                        k1,
                        v1,
                        &map2,
                        *max_distance,
                        prefix_weight,
                        max_prefix,
                    )
                })
                .flatten()
                .collect()
        });
        Ok(idxs)
    }

    fn compare_pairs(
        &self,
        left: &Vec<Option<String>>,
        right: &Vec<Option<String>>,
        max_distance: &f64,
        _q: &Option<usize>,
        prefix_weight: Option<f64>,
        max_prefix: Option<usize>,
        pool: &rayon::ThreadPool,
    ) -> Result<(Vec<usize>, Vec<f64>)> {
        // Extract prefix weight
        let prefix_weight = match prefix_weight {
            Some(x) => x,
            None => {
                return Err(anyhow::anyhow!(
                    "Argument 'prefix weight' must be supplied for jaro-winkler distance"
                ))
            }
        };

        // Extract max prefix size
        let max_prefix = match max_prefix {
            Some(x) => x,
            None => {
                return Err(anyhow::anyhow!(
                    "Argument 'prefix weight' must be supplied for jaro-winkler distance"
                ))
            }
        };

        let args = jaro_rf::Args::default().score_cutoff(*max_distance);
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

                    let dist: Option<f64> =
                        jaro_rf::distance_with_args(l.chars(), r.chars(), &args);

                    // Compute capped common prefix length
                    let capped_prefix_len = l
                        .chars()
                        .zip(r.chars())
                        .take_while(|(c1, c2)| c1 == c2)
                        .count()
                        .min(max_prefix);

                    match dist {
                        Some(x) => {
                            let x2 =
                                x + (capped_prefix_len as f64 * prefix_weight * (1.0 - x)) as f64;
                            if x2 <= *max_distance {
                                Some((i, x2))
                            } else {
                                None
                            }
                        }
                        None => None,
                    }
                })
                .unzip()
        });
        Ok((keep, dists))
    }
}

impl JaroWinkler {
    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        idx_map: &FxHashMap<&str, Vec<usize>>,
        max_distance: f64,
        prefix_weight: f64,
        max_prefix: usize,
    ) -> Option<Vec<(usize, usize, f64)>> {
        let mut idxs: Vec<(usize, usize, f64)> = Vec::new();

        for (k2, v2) in idx_map.iter() {
            if &k1 == k2 {
                iproduct!(v1, v2).for_each(|(v1, v2)| {
                    idxs.push((*v1, *v2, 0.));
                });
                continue;
            }

            // Compute capped common prefix length
            let capped_prefix_len = k1
                .chars()
                .zip(k2.chars())
                .take_while(|(c1, c2)| c1 == c2)
                .count()
                .min(max_prefix);

            let scorer = jaro_rf::BatchComparator::new(k1.chars());
            let args = jaro_rf::Args::default().score_cutoff(max_distance);

            let dist = scorer.distance_with_args(k2.chars(), &args);
            match dist {
                Some(x) => {
                    let x2 = x + (capped_prefix_len as f64 * prefix_weight * (1.0 - x)) as f64;
                    if x2 <= max_distance {
                        iproduct!(v1, v2).for_each(|(a, b)| {
                            idxs.push((*a, *b, x2));
                        });
                    }
                }
                None => (),
            }
        }

        if idxs.is_empty() {
            None
        } else {
            Some(idxs)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_strings() -> Vec<Option<String>> {
        vec![
            Some("apple".to_string()),
            Some("applp".to_string()), // Similar, 1 character different
            Some("banana".to_string()),
            None,
            Some("grapefruit".to_string()),
            Some("grapefruit".to_string()), // Duplicate entry for matches
        ]
    }

    #[test]
    fn test_fuzzy_indices() {
        let jaro_winkler = JaroWinkler;

        let left = test_strings();
        let right = test_strings();

        let max_distance = &0.4; // Example max distance
        let prefix_weight = Some(0.1); // Example prefix weight
        let max_prefix = Some(2); // Example max prefix length

        let pool = crate::utils::get_pool(None).unwrap();

        let indices = jaro_winkler
            .fuzzy_indices(
                &left,
                &right,
                max_distance,
                &None, // Not used
                prefix_weight,
                max_prefix,
                &pool,
            )
            .unwrap();

        assert_eq!(indices.len(), 13, "Indices should not be empty");

        // Further assertions can be made based on expected output and correctness
        for (_, _, dist) in indices {
            assert!(dist <= *max_distance, "Distance exceeds maximum allowed");
        }
    }

    #[test]
    fn test_compare_pairs() {
        let jaro_winkler = JaroWinkler;

        let left = test_strings();
        let right = test_strings();

        let max_distance = &0.5; // Example max distance
        let prefix_weight = Some(0.1); // Example prefix weight
        let max_prefix = Some(2); // Example max prefix length
        let pool = crate::utils::get_pool(None).unwrap();

        let (matches, dists) = jaro_winkler
            .compare_pairs(
                &left,
                &right,
                max_distance,
                &None, // Not used
                prefix_weight,
                max_prefix,
                &pool,
            )
            .unwrap();

        assert_eq!(
            matches.len(),
            dists.len(),
            "Matches and distances should have same length"
        );
        assert!(!matches.is_empty(), "There should be at least one match");
    }

    #[test]
    fn test_invalid_weights() {
        let jaro_winkler = JaroWinkler;

        let left = test_strings();
        let right = test_strings();

        let max_distance = &0.5; // Example max distance
        let prefix_weight = None; // No weight given
        let max_prefix = Some(2); // Example max prefix length
        let pool = crate::utils::get_pool(None).unwrap();

        // Test fuzzy indices with missing prefix weight
        let result = jaro_winkler.fuzzy_indices(
            &left,
            &right,
            max_distance,
            &None, // Not used
            prefix_weight,
            max_prefix,
            &pool,
        );

        assert!(
            result.is_err(),
            "Should return an error when prefix weight is not supplied"
        );

        // Test compare pairs with missing prefix weight
        let result = jaro_winkler.compare_pairs(
            &left,
            &right,
            max_distance,
            &None, // Not used
            prefix_weight,
            max_prefix,
            &pool,
        );

        assert!(
            result.is_err(),
            "Should return an error when prefix weight is not supplied"
        );
    }
}
