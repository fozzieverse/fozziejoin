// This text distance is adapted from the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use crate::stringdist::string_dist_method::StringDistance;
use anyhow::Result;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::VecDeque;

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

pub struct Jaccard;

impl StringDistance for Jaccard {
    fn fuzzy_indices(
        &self,
        left: &Vec<Option<String>>,
        right: &Vec<Option<String>>,
        max_distance: &f64,
        q: &Option<usize>,
        _prefix_weight: Option<f64>,
        _max_prefix: Option<usize>,
        pool: &rayon::ThreadPool,
    ) -> Result<Vec<(usize, usize, f64)>> {
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
        let mut rhs_max_qgrams: usize = 0;
        let mut rhs_index: FxHashMap<usize, FxHashMap<&str, Vec<usize>>> = FxHashMap::default();
        right.iter().enumerate().for_each(|(i, x)| {
            if let Some(ref x_str) = x {
                let grams = get_qgram_set(x_str, *q);
                let grams_len = grams.len();
                rhs_max_qgrams = rhs_max_qgrams.max(grams_len);
                let map = rhs_index
                    .entry(grams_len)
                    .or_insert_with(FxHashMap::default);

                for &gram in &grams {
                    map.entry(gram).or_insert_with(Vec::new).push(i);
                }
            }
        });

        let out_vals: Vec<(usize, usize, f64)> = pool.install(|| {
            left.par_iter()
                .enumerate()
                .filter_map(|(l_idx, val)| {
                    let left_grams = match val {
                        Some(x) => get_qgram_set(x, *q),
                        None => return None,
                    };
                    let lhs_len = left_grams.len();

                    if (lhs_len == 0) & (max_distance.lt(&1.0)) {
                        return None; // Skip empty q-grams
                    }

                    let mut max_q = ((lhs_len as f64) / (1.0 - max_distance)).ceil() as usize;
                    max_q = max_q.min(rhs_max_qgrams);
                    let min_q = std::cmp::max(
                        1,
                        lhs_len.saturating_sub((lhs_len as f64 * max_distance).floor() as usize),
                    );

                    let mut out: Vec<(usize, usize, f64)> = Vec::new();

                    for rhs_len in min_q..=max_q {
                        let rhs_qgram_idxs = match rhs_index.get(&rhs_len) {
                            Some(x) => x,
                            None => continue,
                        };

                        let mut candidates: FxHashMap<usize, usize> = FxHashMap::default();

                        if max_distance.lt(&1.0) {
                            // Can only include those with 1+ matches
                            for qgram in &left_grams {
                                if let Some(matches) = rhs_qgram_idxs.get(qgram) {
                                    for &matched_idx in matches {
                                        *candidates.entry(matched_idx).or_insert(0) += 1;
                                    }
                                }
                            }
                        } else {
                            // Must return all matches, because max possible distance specified
                            for matches in rhs_qgram_idxs.values() {
                                for &matched_idx in matches {
                                    *candidates.entry(matched_idx).or_insert(0) += 1;
                                }
                            }
                        }

                        // Evaluate distance for each candidate
                        for (r_idx, nmatch) in candidates {
                            let denom = lhs_len + rhs_len - nmatch;
                            let dist = 1.0 - (nmatch as f64 / denom as f64);
                            if dist <= *max_distance {
                                out.push((l_idx, r_idx, dist));
                            }
                        }
                    }
                    Some(out)
                })
                .flatten()
                .collect()
        });

        Ok(out_vals)
    }

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
