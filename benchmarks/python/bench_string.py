import polars as pl
import pathlib
import fozziejoin
import time
import polars_distance as pld

# Load dataset
path = pathlib.Path("../../datasets/misspellings.csv")
orig = pl.read_csv(path)
df1 = orig[['misspelling']]
df2 = orig.clone()[['correct']]

# Benchmark loop
runs = 5
records = []

for i in range(runs):
    # Cross-join with polars-distance
    start = time.perf_counter()
    cj = df1.join(df2, how='cross').filter(
        pld.col('misspelling').dist_str.levenshtein('correct').le(1)
    )
    pld_time = time.perf_counter() - start

    # Fozziejoin
    start = time.perf_counter()
    fozzie = fozziejoin.string_distance_join(
        df1, df2,
        left_on=['misspelling'], right_on=['correct'],
        max_distance=1, method='levenshtein'
    )
    fozzie_time = time.perf_counter() - start

    # Assert results identical
    only_cj = cj.join(fozzie, how='anti', on=fozzie.columns)
    assert only_cj.shape[0] == 0
    only_fozzie = fozzie.join(cj, how='anti', on=fozzie.columns)
    assert only_fozzie.shape[0] == 0

    records.append({
        "run": i + 1,
        "python_time_sec": pld_time,
        "rust_time_sec": fozzie_time,
    })

# Save results
results_df = pl.DataFrame(records)
#results_df.write_csv("../../benchmarks/results/py_levenshtein_benchmark.csv")

# Summary
pld_mean = results_df["python_time_sec"].mean()
fozzie_mean = results_df["rust_time_sec"].mean()
ratio = results_df["python_time_sec"].mean() / results_df["rust_time_sec"].mean()

print(results_df)
print(f"\nAverage cross-join + pld time: {pld_mean:.2f}")
print(f"Average Rust time: {fozzie_mean:.2f}")
print(f"Ratio: {ratio:.2f}")

