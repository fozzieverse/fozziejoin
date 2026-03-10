if __name__ == '__main__':
    import polars as pl
    import pathlib
    import fozziejoin
    import time
    import polars_distance as pld
    import polars_ds as pds

    # Load dataset
    path = pathlib.Path("../../datasets/misspellings.csv")
    orig = pl.read_csv(path)
    df1 = orig[['misspelling']]
    df2 = orig.clone()[['correct']]

    # Benchmark loop
    runs = 10
    records = []

    for i in range(runs):
        # Fozziejoin
        start = time.perf_counter()
        fozzie = fozziejoin.string_distance_join(
            df1, df2,
            left_on=['misspelling'], right_on=['correct'],
            max_distance=1, method='levenshtein'
        )
        fozzie_time = time.perf_counter() - start

        # Cross-join with polars-distance
        start = time.perf_counter()
        pld_cj = df1.join_where(df2,
            pld.col('misspelling').dist_str.levenshtein('correct').le(1)
        )
        pld_time = time.perf_counter() - start

        # Assert results identical
        only_cj = pld_cj.join(fozzie, how='anti', on=fozzie.columns)
        assert only_cj.shape[0] == 0
        only_fozzie = fozzie.join(pld_cj, how='anti', on=fozzie.columns)
        assert only_fozzie.shape[0] == 0

        # Cross-join with polars data science extension
        start = time.perf_counter()
        pds_cj = df1.join_where(
            df2,
            pds.filter_by_levenshtein(
                pl.col('misspelling'), pl.col('correct'), bound=1, parallel=True
            )
        )
        pds_time = time.perf_counter() - start

        # Assert results identical
        only_cj = pds_cj.join(fozzie, how='anti', on=fozzie.columns)
        assert only_cj.shape[0] == 0
        only_fozzie = fozzie.join(pds_cj, how='anti', on=fozzie.columns)
        assert only_fozzie.shape[0] == 0

        # Update the dict that records each runtime
        records.append({
            "run": i + 1,
            "polars_distance": pld_time,
            "polars_ds": pds_time,
            "fozziejoin": fozzie_time,
        })

    # Convert to polars DF
    results_df = pl.DataFrame(records)

    # Summarize
    pld_mean = results_df["polars_distance"].mean()
    pds_mean = results_df["polars_ds"].mean()
    fozzie_mean = results_df["fozziejoin"].mean()
    pld_to_fozzie = pld_mean / fozzie_mean
    pds_to_fozzie = pds_mean / fozzie_mean

    # Print
    print(f"Average pld join_where time: {pld_mean:.2f}")
    print(f"Average pld join_where time: {pds_mean:.2f}")
    print(f"Average fozziejoin time: {fozzie_mean:.2f}")
    print(f"Polars distance / fozzie: {pld_to_fozzie:.2f}")
    print(f"Polars data science / fozzie: {pds_to_fozzie:.2f}")

