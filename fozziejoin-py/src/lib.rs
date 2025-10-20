use fozziejoin_core::stringdist::string_distance_join_polars;
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

#[pyfunction]
fn string_distance_join_rs(
    left: PyDataFrame,
    right: PyDataFrame,
    left_on: Vec<String>,
    right_on: Vec<String>,
    how: String,
    method: String,
    max_distance: f64,
    q: Option<usize>,
    prefix_weight: Option<f64>,
    max_prefix: Option<usize>,
    distance_col: Option<String>,
    suffix: String,
    nthread: Option<usize>,
) -> PyResult<PyDataFrame> {
    let left_df: DataFrame = left.into();
    let right_df: DataFrame = right.into();

    let out = string_distance_join_polars(
        left_df,
        right_df,
        left_on,
        right_on,
        how,
        method,
        max_distance,
        q,
        prefix_weight,
        max_prefix,
        distance_col,
        suffix,
        nthread,
    )
    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    Ok(PyDataFrame(out))
}

#[pymodule]
fn fozziejoin(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(string_distance_join_rs, m)?)?;
    Ok(())
}
