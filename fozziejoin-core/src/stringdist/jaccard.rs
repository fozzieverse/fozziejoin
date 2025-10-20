// This text distance is adapted from the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use anyhow::Result;
use log::warn;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::VecDeque;

pub struct Jaccard;

fn get_qgram_set(s: &str, q: usize) -> FxHashSet<&str> {
    let mut grams = FxHashSet::default();
    let mut ring = VecDeque::with_capacity(q + 1);

    for (i, _) in s.char_indices() {
        ring.push_back(i);
        if ring.len() == q + 1 {
            let start = ring[0];
            let end = ring[q];
            grams.insert(&s[start..end]);
            ring.pop_front();
        }
    }

    if ring.len() == q {
        let start = ring[0];
        let end = s.len();
        grams.insert(&s[start..end]);
    }

    grams
}

impl Jaccard {
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
        // Check for invalid arguments supplied by user
        if max_prefix.is_some() || prefix_weight.is_some() {
            warn!(
                "Warning: `max_prefix` and/or `prefix_weight` are set but ignored by this method."
            );
        }

        // Extract q
        let q = match q {
            Some(x) => x,
            None => {
                return Err(anyhow::anyhow!(
                    "Argument 'q' must be supplied for Jaccard distance"
                ))
            }
        };

        // Build RHS q-gram reverse index
        let mut rhs_qgram_index: FxHashMap<&str, Vec<usize>> = FxHashMap::default();
        let mut rhs_qgrams: FxHashMap<usize, FxHashSet<&str>> = FxHashMap::default();

        for (r_idx, val) in right.iter().enumerate() {
            let val2 = match val {
                Some(x) => x,
                None => continue,
            };
            let idx = r_idx;
            let grams = get_qgram_set(val2, *q);
            rhs_qgrams.insert(idx, grams.clone());
            for gram in grams {
                rhs_qgram_index.entry(gram).or_default().push(idx);
            }
        }

        let results = pool.install(|| {
            left.par_iter()
                .enumerate()
                .filter_map(|(l_idx, val)| {
                    let val2 = match val {
                        Some(x) => x,
                        None => return None,
                    };
                    let lhs_idx = l_idx;
                    let lhs_grams = get_qgram_set(val2, *q);

                    // Collect RHS candidates that share at least one q-gram
                    let mut candidates = FxHashSet::default();
                    for gram in &lhs_grams {
                        if let Some(rhs_idxs) = rhs_qgram_index.get(gram) {
                            candidates.extend(rhs_idxs);
                        }
                    }

                    // If no candidates, stop early
                    if candidates.is_empty() {
                        return None;
                    }

                    // Compare Jaccard distance for each candidate
                    let mut matches = Vec::new();
                    for &rhs_idx in &candidates {
                        let rhs_grams = &rhs_qgrams[&rhs_idx];

                        // Predict best-case similarity
                        let max_intersection = lhs_grams.len().min(rhs_grams.len());
                        let min_union = lhs_grams.len().max(rhs_grams.len());
                        let min_possible_distance =
                            1.0 - (max_intersection as f64 / min_union as f64);

                        if min_possible_distance > *max_distance {
                            continue; // Skip: can't possibly be close enough
                        }

                        // Proceed with actual Jaccard distance
                        let intersection = lhs_grams.intersection(rhs_grams).count();
                        let union = lhs_grams.union(rhs_grams).count();
                        let dist = 1.0 - (intersection as f64 / union as f64);

                        if dist <= *max_distance {
                            matches.push((lhs_idx, rhs_idx, dist));
                        }
                    }
                    Some(matches)
                })
                .flatten()
                .collect()
        });

        Ok(results)
    }

    pub fn compare_pairs(
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
            Some(x) => x,
            None => {
                return Err(anyhow::anyhow!(
                    "Argument 'q' must be supplied for Jaccard distance"
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

                    let hs1 = get_qgram_set(l, *q);
                    let hs2 = get_qgram_set(r, *q);

                    let dist = if hs1.is_empty() && hs2.is_empty() {
                        0.0
                    } else {
                        let intersection = hs1.intersection(&hs2).count();
                        let union = hs1.union(&hs2).count();
                        1.0 - (intersection as f64 / union as f64)
                    };

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
}

#[cfg(test)]
mod tests {
    use crate::stringdist::StringDistMethod;
    use crate::utils::get_pool;
    use anyhow::Result;

    #[test]
    fn test_basic_match() -> Result<()> {
        let pool = get_pool(Some(1)).expect("error setting up thread pool");
        let method = StringDistMethod::new("jaccard").expect("ohno");
        let left = vec![Some("apple".to_string())];
        let right = vec![Some("apples".to_string())];

        let matches = method.fuzzy_indices(&left, &right, &0.5, &Some(2), None, None, &pool)?;
        assert_eq!(matches.len(), 1);

        let (l, r, dist) = matches[0];
        assert_eq!(l, 0);
        assert_eq!(r, 0);
        assert!(dist <= 0.5);
        Ok(())
    }

    #[test]
    fn test_no_match_due_to_distance() -> Result<()> {
        let pool = get_pool(Some(1)).expect("error setting up thread pool");
        let method = StringDistMethod::new("jaccard").expect("ohno");

        let left = vec![Some("apple".to_string())];
        let right = vec![Some("banana".to_string())];
        let matches = method.fuzzy_indices(&left, &right, &0.2, &Some(2), None, None, &pool)?;

        assert!(matches.is_empty());
        Ok(())
    }

    #[test]
    fn test_multiple_matches() -> Result<()> {
        let pool = get_pool(Some(1)).expect("error setting up thread pool");
        let method = StringDistMethod::new("jaccard").expect("ohno");

        let left = vec![Some("apple".to_string()), Some("banana".to_string())];
        let right = vec![Some("apples".to_string()), Some("bananas".to_string())];
        let matches = method.fuzzy_indices(&left, &right, &0.5, &Some(2), None, None, &pool)?;

        assert_eq!(matches.len(), 2);
        Ok(())
    }

    #[test]
    fn test_qgram_effect() -> Result<()> {
        let pool = get_pool(Some(1)).expect("error setting up thread pool");
        let method = StringDistMethod::new("jaccard").expect("ohno");

        let left = vec![Some("abcdef".to_string())];
        let right = vec![Some("abcxyz".to_string())];

        let matches_q2 = method.fuzzy_indices(&left, &right, &0.8, &Some(2), None, None, &pool)?;
        assert_eq!(matches_q2.len(), 1);

        let matches_q3 = method.fuzzy_indices(&left, &right, &0.8, &Some(3), None, None, &pool)?;
        assert_eq!(matches_q3.len(), 0);
        Ok(())
    }

    #[test]
    fn test_small_str() -> Result<()> {
        let pool = get_pool(Some(1)).expect("error setting up thread pool");
        let method = StringDistMethod::new("jaccard").expect("ohno");

        let left = vec![Some("ab".to_string())];
        let right = vec![Some("ab".to_string())];
        let matches_q3 = method.fuzzy_indices(&left, &right, &0.8, &Some(3), None, None, &pool)?;
        assert_eq!(matches_q3.len(), 0);
        Ok(())
    }
}
