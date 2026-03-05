use crate::stringdist::string_dist_method::StringDistance;
use anyhow::Result;
use itertools::iproduct;
use rapidfuzz::distance::hamming as ham_rf;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

pub struct Hamming;
impl StringDistance for Hamming {
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
        let args = ham_rf::Args::default().score_cutoff(*max_distance as usize);
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

                    let out = ham_rf::distance_with_args(l.chars(), r.chars(), &args)
                        .ok()
                        .flatten()
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

impl Hamming {
    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        length_map: &FxHashMap<usize, Vec<&str>>,
        idx_map: &FxHashMap<&str, Vec<usize>>,
        max_distance: &f64,
    ) -> Option<Vec<(usize, usize, f64)>> {
        let scorer = ham_rf::BatchComparator::new(k1.chars());
        let args = ham_rf::Args::default().score_cutoff(*max_distance as usize);

        // Get range of lengths within max distance of current
        let k1_len = k1.len();
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
                    let dist = scorer.distance_with_args(k2.chars(), &args);

                    let dist = match dist {
                        Ok(x) => x,
                        Err(_) => None,
                    };

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
