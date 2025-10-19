// This text distance is adapted from the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use anyhow::Result;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::VecDeque;

// Cosine Distance Implementation
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
    pub fn new() -> Self {
        Jaccard {}
    }

    pub fn fuzzy_indices(
        &self,
        left: &Vec<Option<String>>,
        right: &Vec<Option<String>>,
        max_distance: f64,
        q: usize,
    ) -> Result<Vec<(usize, usize, f64)>> {
        // Build RHS q-gram reverse index
        let mut rhs_qgram_index: FxHashMap<&str, Vec<usize>> = FxHashMap::default();
        let mut rhs_qgrams: FxHashMap<usize, FxHashSet<&str>> = FxHashMap::default();

        for (r_idx, val) in right.iter().enumerate() {
            let val = match val {
                Some(x) => x,
                None => continue,
            };
            let idx = r_idx;
            let grams = get_qgram_set(val, q);
            rhs_qgrams.insert(idx, grams.clone());
            for gram in grams {
                rhs_qgram_index.entry(gram).or_default().push(idx);
            }
        }

        // Match LHS records to RHS candidates via shared q-grams
        let results = left
            .par_iter()
            .enumerate()
            .filter_map(|(l_idx, val)| {
                let val = match val {
                    Some(x) => x,
                    None => return None,
                };
                let lhs_idx = l_idx;
                let lhs_grams = get_qgram_set(val, q);

                // Collect RHS candidates that share at least one q-gram
                let mut candidates = FxHashSet::default();
                for gram in &lhs_grams {
                    if let Some(rhs_idxs) = rhs_qgram_index.get(gram) {
                        candidates.extend(rhs_idxs);
                    }
                }

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
                    let min_possible_distance = 1.0 - (max_intersection as f64 / min_union as f64);

                    if min_possible_distance > max_distance {
                        continue; // Skip: can't possibly be close enough
                    }

                    // Proceed with actual Jaccard distance
                    let intersection = lhs_grams.intersection(rhs_grams).count();
                    let union = lhs_grams.union(rhs_grams).count();
                    let dist = 1.0 - (intersection as f64 / union as f64);

                    if dist <= max_distance {
                        matches.push((lhs_idx, rhs_idx, dist));
                    }
                }
                Some(matches)
            })
            .flatten()
            .collect();

        Ok(results)
    }
}
