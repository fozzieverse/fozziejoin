use anyhow::Result;
use rustc_hash::FxHashMap;

pub fn get_qgrams(s: &str, q: usize) -> FxHashMap<&str, usize> {
    let mut qgram_map = FxHashMap::default();

    if s.len() < q {
        return qgram_map;
    }

    let mut char_indices = s.char_indices().collect::<Vec<_>>();
    char_indices.push((s.len(), '\0'));

    for i in 0..=char_indices.len().saturating_sub(q + 1) {
        let start = char_indices[i].0;
        let end = char_indices[i + q].0;
        let qgram = &s[start..end];
        *qgram_map.entry(qgram).or_insert(0) += 1;
    }

    qgram_map
}

pub fn strvec_to_qgram_map<'a>(
    strvec: &'a Vec<Option<String>>,
    q: usize,
) -> Result<FxHashMap<&'a str, (FxHashMap<&'a str, usize>, Vec<usize>)>> {
    let mut qgram_map: FxHashMap<&'a str, (FxHashMap<&'a str, usize>, Vec<usize>)> =
        FxHashMap::default();

    for (index, val) in strvec.iter().enumerate() {
        let val = match val {
            Some(x) => x,
            None => continue,
        };

        let hm: FxHashMap<&str, usize> = get_qgrams(val, q);
        qgram_map
            .entry(val)
            .and_modify(|v| v.1.push(index + 1))
            .or_insert((hm, vec![index + 1]));
    }

    Ok(qgram_map)
}
