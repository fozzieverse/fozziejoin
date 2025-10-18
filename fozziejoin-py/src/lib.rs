use fozziejoin_core::stringdist::Jaccard;
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

/// Joins two DataFrames using a list of (left_idx, right_idx) pairs
#[pyfunction]
fn fozzie_join(
    left: PyDataFrame,
    right: PyDataFrame,
    left_on: &str,
    right_on: &str,
    max_distance: f64,
    q: usize,
) -> PyResult<PyDataFrame> {
    let left_df: DataFrame = left.into();
    let right_df: DataFrame = right.into();

    let left: Vec<String> = left_df
        .column(left_on)
        .expect("hi!")
        .as_series()
        .expect("ruhroh")
        .clone()
        .iter()
        .map(|x| x.to_string())
        .collect();

    let right: Vec<String> = right_df
        .column(right_on)
        .expect("hi!")
        .as_series()
        .expect("ruhroh")
        .clone()
        .iter()
        .map(|x| x.to_string())
        .collect();

    let idxs = Jaccard
        .fuzzy_indices(&left, &right, max_distance, q)
        .expect("hi!");
    let idxlen = idxs.iter().len();

    let mut left_idxs: Vec<u32> = Vec::with_capacity(idxlen);
    let mut right_idxs: Vec<u32> = Vec::with_capacity(idxlen);

    idxs.iter().for_each(|(a, b, _)| {
        left_idxs.push(*a as u32);
        right_idxs.push(*b as u32);
    });
    let left_idxs2 = UInt32Chunked::from_slice("idx".into(), &left_idxs);
    let right_idxs2 = UInt32Chunked::from_slice("idx".into(), &right_idxs);

    let left_out = left_df.take(&left_idxs2).expect("lul");
    let right_out = right_df.take(&right_idxs2).expect("lul2");

    let joined = left_out.hstack(&right_out.get_columns()).expect("hi!");
    Ok(PyDataFrame(joined))
}

#[pymodule]
fn fozzie(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(fozzie_join, m)?)?;
    Ok(())
}
