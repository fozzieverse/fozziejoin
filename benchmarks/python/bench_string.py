import polars as pl
import pathlib
import fozziejoin
import time

# Load dataset
path = pathlib.Path("../../datasets/misspellings.csv")
df = pl.read_csv(path)
df2 = df.clone().rename({'misspelling': 'misspelling.y', 'correct': 'correct.y'})

# Jaccard q-gram implementation
def jaccard_qgram(s1: str, s2: str, q: int = 3) -> float:
    def qgrams(s): return set(s[i:i+q] for i in range(len(s) - q + 1))
    q1, q2 = qgrams(s1), qgrams(s2)
    if not q1 or not q2:
        return 0.0
    return len(q1 & q2) / len(q1 | q2)

# Benchmark loop
runs = 2
records = []

misspellings = df["misspelling"].to_list()
corrections = df2["correct.y"].to_list()

for i in range(runs):
    # Python implementation
    start = time.perf_counter()
    results = []
    for miss in misspellings:
        for corr in corrections:
            dist = jaccard_qgram(miss, corr, q=3)
            if dist >= 0.5:
                results.append({
                    "misspelling": miss,
                    "correct": corr,
                    "score": dist,
                })
    base = pl.DataFrame(results).unique()
    base_time = time.perf_counter() - start

    # Rust implementation
    start = time.perf_counter()
    fozzie = fozziejoin.string_distance_join(
        df, df2,
        left_on=['misspelling'], right_on=['correct.y'], max_distance=0.5, q=3
    )['misspelling', 'correct.y'].rename({'correct.y': 'correct'})
    fozzie_time = time.perf_counter() - start

    # Correctness check
    only_base = base.join(fozzie, on=fozzie.columns, how='anti')
    only_fozzie = fozzie.join(base, on=fozzie.columns, how='anti')
    mismatch = only_base.shape[0] + only_fozzie.shape[0]

    records.append({
        "run": i + 1,
        "python_time_sec": base_time,
        "rust_time_sec": fozzie_time,
        "mismatches": mismatch
    })

# Save results
results_df = pl.DataFrame(records)
results_df.write_csv("../../benchmarks/results/py_jaccard_benchmark.csv")

# Summary
pymean = results_df["python_time_sec"].mean()
rustmean = results_df["rust_time_sec"].mean()
ratio = results_df["python_time_sec"].mean() / results_df["rust_time_sec"].mean()

print(results_df)
print(f"\nAverage Python time: {pymean:.2f}")
print(f"Average Rust time: {rustmean:.2f}")
print(f"Ratio: {ratio:.2f}")
print("Total mismatches across runs:", results_df["mismatches"].sum())
