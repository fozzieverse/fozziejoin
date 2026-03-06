use crate::stringdist::string_dist_method::StringDistance;
use anyhow::Result;
use itertools::iproduct;
use rapidfuzz::distance::damerau_levenshtein as dl_rf;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

pub struct DamerauLevenshtein;
impl StringDistance for DamerauLevenshtein {
    fn compare_pairs(
        &self,
        left: &Vec<Option<String>>,
        right: &Vec<Option<String>>,
        max_distance: &f64,
        _q: &Option<usize>,
        _prefix_weight: Option<f64>,
        _max_prefix: Option<usize>,
        pool: &rayon::ThreadPool,
    ) -> Result<(Vec<usize>, Vec<f64>)> {
        let args = dl_rf::Args::default().score_cutoff(*max_distance as usize);
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

                    let out = dl_rf::distance_with_args(l.chars(), r.chars(), &args)
                        .map(|x| x as f64)
                        .filter(|&x| x <= *max_distance)
                        .map(|x| (i, x));
                    out
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
        _q: &Option<usize>,
        _prefix_weight: Option<f64>,
        _max_prefix: Option<usize>,
        pool: &rayon::ThreadPool,
    ) -> anyhow::Result<Vec<(usize, usize, f64)>> {
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

        let mut length_map: FxHashMap<usize, Vec<&str>> = FxHashMap::default();
        for key in map2.keys() {
            let key_len = key.len();
            length_map.entry(key_len).or_default().push(key);
        }

        let idxs: Vec<(usize, usize, f64)> = pool.install(|| {
            map1.par_iter()
                .filter_map(|(k1, v1)| {
                    self.compare_one_to_many(k1, v1, &length_map, &map2, &max_distance)
                })
                .flatten()
                .collect()
        });

        Ok(idxs)
    }
}

impl DamerauLevenshtein {
    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        length_map: &FxHashMap<usize, Vec<&str>>,
        idx_map: &FxHashMap<&str, Vec<usize>>,
        max_distance: &f64,
    ) -> Option<Vec<(usize, usize, f64)>> {
        let scorer = dl_rf::BatchComparator::new(k1.chars());
        let args = dl_rf::Args::default().score_cutoff(*max_distance as usize);

        // Get range of lengths within max distance of current
        let k1_len = k1.chars().count();
        let start_len = k1_len.saturating_sub(*max_distance as usize);
        let end_len = k1_len.saturating_add(*max_distance as usize + 1);

        // Start a list to collect results
        let mut idxs: Vec<(usize, usize, f64)> = Vec::new();

        // Begin making string comparisons
        for i in start_len..end_len {
            if let Some(lookup) = length_map.get(&i) {
                lookup.iter().for_each(|k2| {
                    // No need to run distance functions if exactly the same
                    if &k1 == k2 {
                        let v2 = idx_map.get(k2).unwrap();
                        iproduct!(v1, v2).for_each(|(v1, v2)| {
                            idxs.push((*v1, *v2, 0.));
                        });
                        return;
                    }

                    // Run distance calculation
                    let dist: Option<usize> = scorer.distance_with_args(k2.chars(), &args);

                    match dist {
                        Some(x) => {
                            let x = x as f64;
                            // Check vs. threshold
                            if x <= *max_distance {
                                let v2 = idx_map.get(k2).unwrap();
                                iproduct!(v1, v2).for_each(|(v1, v2)| {
                                    idxs.push((*v1, *v2, x as f64));
                                });
                                return;
                            }
                        }
                        None => (),
                    }
                });
            }
        }

        // Return all matches, if any
        if idxs.is_empty() {
            return None;
        } else {
            return Some(idxs);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_pairs_basic() {
        let damerau_levenshtein = DamerauLevenshtein;
        let left = vec![Some("test".to_string()), Some("rust".to_string()), None];
        let right = vec![
            Some("test".to_string()),
            Some("rusty".to_string()),
            Some("wrong".to_string()),
        ];
        let max_distance = 1.0;
        let pool = crate::utils::get_pool(None).unwrap();

        let (indices, distances) = damerau_levenshtein
            .compare_pairs(&left, &right, &max_distance, &None, None, None, &pool)
            .unwrap();

        assert_eq!(indices, vec![0, 1]); // Expecting matches at indices 0 and 1
        assert_eq!(distances.len(), 2); // Expecting two distances
    }

    #[test]
    fn test_compare_pairs_empty_strings() {
        let damerau_levenshtein = DamerauLevenshtein;
        let left = vec![Some("".to_string())];
        let right = vec![Some("".to_string())];
        let max_distance = 0.0;
        let pool = crate::utils::get_pool(None).unwrap();

        let (indices, distances) = damerau_levenshtein
            .compare_pairs(&left, &right, &max_distance, &None, None, None, &pool)
            .unwrap();

        assert_eq!(indices, vec![0]); // Expecting match
        assert_eq!(distances, vec![0.0]); // Distance should be zero
    }

    #[test]
    fn test_fuzzy_indices_basic() {
        let damerau_levenshtein = DamerauLevenshtein;
        let left = vec![Some("test".to_string()), Some("rust".to_string())];
        let right = vec![
            Some("test".to_string()),
            Some("rusty".to_string()),
            Some("wrong".to_string()),
        ];
        let max_distance = 1.0;
        let pool = crate::utils::get_pool(None).unwrap();

        let indices = damerau_levenshtein
            .fuzzy_indices(&left, &right, &max_distance, &None, None, None, &pool)
            .unwrap();

        assert_eq!(indices.len(), 2); // Expecting to find two pairs
    }

    #[test]
    fn test_fuzzy_indices_with_none() {
        let damerau_levenshtein = DamerauLevenshtein;
        let left = vec![Some("test".to_string()), None];
        let right = vec![Some("test".to_string()), Some("rusty".to_string())];
        let max_distance = 1.0;
        let pool = crate::utils::get_pool(None).unwrap();

        let indices = damerau_levenshtein
            .fuzzy_indices(&left, &right, &max_distance, &None, None, None, &pool)
            .unwrap();

        assert_eq!(indices.len(), 1); // Should only find one match ignoring None
    }

    #[test]
    fn test_compare_one_to_many() {
        let damerau_levenshtein = DamerauLevenshtein;
        let k1 = "test";
        let v1 = vec![0];
        let length_map: FxHashMap<usize, Vec<&str>> =
            [(4, vec!["test", "rest"])].iter().cloned().collect();
        let idx_map: FxHashMap<&str, Vec<usize>> = [("test", vec![0]), ("rest", vec![1])]
            .iter()
            .cloned()
            .collect();
        let max_distance = 1.0;

        let result = damerau_levenshtein
            .compare_one_to_many(k1, &v1, &length_map, &idx_map, &max_distance)
            .unwrap();

        assert_eq!(result, vec![(0, 0, 0.0), (0, 1, 1.0)]);
    }
}
