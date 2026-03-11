// This text distance is adapted from the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use anyhow::{anyhow, Result};
use extendr_api::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPool;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::VecDeque;

use crate::string::ngram::QGramDistance;

// Jaccard Distance Implementation
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

impl QGramDistance for Jaccard {
    fn compute(
        &self,
        qgrams_s1: &FxHashMap<&str, usize>,
        qgrams_s2: &FxHashMap<&str, usize>,
    ) -> f64 {
        let mut intersection = 0;
        let mut union = 0;

        let mut all_keys: FxHashSet<_> = qgrams_s1.keys().cloned().collect();
        all_keys.extend(qgrams_s2.keys().cloned());

        for key in all_keys {
            let count1 = qgrams_s1.get(&key).copied().unwrap_or(0);
            let count2 = qgrams_s2.get(&key).copied().unwrap_or(0);

            intersection += count1.min(count2);
            union += count1.max(count2);
        }

        if union == 0 {
            1.0
        } else {
            1.0 - (intersection as f64 / union as f64)
        }
    }

    fn compare_pairs(
        &self,
        left: &Vec<&str>,
        right: &Vec<&str>,
        q: &usize,
        max_distance: &f64,
        pool: &rayon::ThreadPool,
    ) -> (Vec<usize>, Vec<f64>) {
        let (keep, dists): (Vec<usize>, Vec<f64>) = pool.install(|| {
            left.par_iter()
                .zip(right)
                .enumerate()
                .filter_map(|(i, (l, r))| {
                    if l.is_na() || r.is_na() {
                        return None;
                    }

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

        (keep, dists)
    }

    fn fuzzy_indices(
        &self,
        left: &List,
        left_key: &str,
        right: &List,
        right_key: &str,
        max_distance: f64,
        q: usize,
        pool: &ThreadPool,
    ) -> Result<Vec<(usize, usize, f64)>> {
        let right_iter = right
            .dollar(right_key)
            .map_err(|_| anyhow!("Column '{}' not found in right dataframe", right_key))?
            .as_str_iter()
            .ok_or_else(|| anyhow!("Column '{}' is not a string vector", right_key))?;

        // Build RHS q-gram reverse index
        let mut rhs_max_qgrams: usize = 0;
        let mut rhs_index: FxHashMap<usize, FxHashMap<&str, Vec<usize>>> = FxHashMap::default();
        right_iter.enumerate().for_each(|(i, x)| {
            if !x.is_na() {
                let grams = get_qgram_set(x, q);
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

        let left_vals = left
            .dollar(left_key)
            .map_err(|_| anyhow!("Column '{}' not found in right dataframe", right_key))?;
        let left_vals = left_vals
            .as_str_vector()
            .ok_or_else(|| anyhow!("Column '{}' is not a string vector", right_key))?;

        let out_vals: Vec<(usize, usize, f64)> = pool.install(|| {
            left_vals
                .par_iter()
                .enumerate()
                .filter_map(|(l_idx, val)| {
                    let left_grams = if val.is_na() {
                        return None;
                    } else {
                        get_qgram_set(val, q)
                    };
                    let lhs_len = left_grams.len();

                    if lhs_len == 0 {
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

                        for qgram in &left_grams {
                            if let Some(matches) = rhs_qgram_idxs.get(qgram) {
                                for &matched_idx in matches {
                                    *candidates.entry(matched_idx).or_insert(0) += 1;
                                }
                            }
                        }

                        // Evaluate distance for each candidate
                        for (r_idx, nmatch) in candidates {
                            let denom = lhs_len + rhs_len - nmatch;
                            let dist = 1.0 - (nmatch as f64 / denom as f64);
                            if dist <= max_distance {
                                out.push((l_idx + 1, r_idx + 1, dist));
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
}
