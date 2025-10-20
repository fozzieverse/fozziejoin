use polars::prelude::*;

pub fn add_null_rows(df: &DataFrame, x: usize) -> PolarsResult<DataFrame> {
    // Create null-filled columns matching the schema
    let null_columns: Vec<Column> = df
        .get_columns()
        .iter()
        .map(|col| {
            let dtype = col.dtype();
            Column::full_null(col.name().clone(), x, dtype)
        })
        .collect();

    // Create a null row DataFrame
    let null_df = DataFrame::new(null_columns)?;

    // Concatenate vertically
    df.vstack(&null_df)
}
