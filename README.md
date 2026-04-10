# fozziejoin

<!-- badges: start -->
[![CRAN status](https://www.r-pkg.org/badges/version/fozziejoin)](https://CRAN.R-project.org/package=fozziejoin)
<!-- badges: end -->

`fozziejoin` is an R package that performs fast fuzzy joins using Rust as a
backend. It is a performance-minded re-imagining of the very popular
[`fuzzyjoin` package]( https://CRAN.R-project.org/package=fuzzyjoin).
Performance improvements relative to `fuzzyjoin` can be significant, especially
for string distance joins. See the [benchmarks](#Benchmarks) for more details.

The following function families are available:

- `fozzie_string_join` 
- `fozzie_difference_join`
- `fozzie_distance_join`
- `fozzie_interval_join`
- `fozzie_interval_join`
- `fozzie_regex_join`
- `fozzie_temporal_join`
- `fozzie_temporal_interval_join`

These function families include related functions, such as 
`fozzie_string_inner_join`.

The name is a playful nod to “fuzzy join” — reminiscent of 
[Fozzie Bear](https://en.wikipedia.org/wiki/Fozzie_Bear) from the Muppets.
A picture of Fozzie will appear in the repo once the legal team gets braver.
**Wocka wocka!**

## Requirements

R 4.2 or greater is required for all installations. R 4.5.0 or greater is preferred.

When installing from source, you will need these additional dependencies:

- Cargo, the Rust package manager
- Rustc
- xz

To run the examples in the README or benchmarking scripts, the following are
required:

- `dplyr`
- `fuzzyjoin`
- `qdapDictionaries`
- `microbenchmark`
- `tibble`
 
### Installation

From CRAN:

```r
install.packages('fozziejoin')
```

To install the latest development version:

```r
remotes::install_github("fozzieverse/fozziejoin")
```

### Usage

Code herein is adapted from the motivating example used in the `fuzzyjoin`
package. First, we take a list of common misspellings (and their corrected
alternatives) from Wikipedia. To run in a a reasonable amount of time, we
take a random sample of 1000.

```r
library(fozziejoin)
library(tibble)
library(fuzzyjoin) # For misspellings dataset

# Load misspelling data
data(misspellings)

# Take subset of 1k records
set.seed(2016)
sub_misspellings <- misspellings[sample(nrow(misspellings), 100), ]
```

Next, we load a dictionary of words from the `qdapDictionaries` package.

```r
library(qdapDictionaries) # For dictionary
words <- tibble::as_tibble(DICTIONARY)
```

Then, we run our join function.

```r
fozzie <- fozzie_string_join(
    sub_misspellings, words, method='lv', 
    by = c('misspelling' = 'word'), max_distance=2
)
```

## Benchmarks

Select benchmark comparisons are below. See [the benchmarks directory](https://github.com/fozzieverse/fozziejoin/tree/main/benchmarks)
for the scripts ('r' subfolder) and results ('results' subfolder).
For reproducibility, benchmarks are made using a GitHub workflow: see
[GitHub Actions Workflow](https://github.com/fozzieverse/fozziejoin/blob/main/.github/workflows/run_rbase_benches.yml)
for the workflow spec. Linux users will observe the largest performance gains,
presumably due to the relative efficiency of parallelization via `rayon`.

[![Fozziejoin vs. fuzzyjoin runtime on select join methods](https://raw.githubusercontent.com/fozzieverse/fozziejoin/fec7f14a33b3aa1c9ffbc9e9f8898cdfe4492eb8/benchmarks/results/rbase_bench_plot.png)](https://raw.githubusercontent.com/fozzieverse/fozziejoin/fec7f14a33b3aa1c9ffbc9e9f8898cdfe4492eb8/benchmarks/results/rbase_bench_plot.png)

## Known behavior changes relative to `fuzzyjoin`

While `fozziejoin` is heavily inspired by `fuzzyjoin`, it does not seek to
replicate it's behavior entirely. Please submit a GitHub issue if there are
features you'd like to see! We will prioritize feature support based on
community feedback.

Below are some known differences in behavior that we do not currently plan to
address.

- `fozziejoin` allows `NA` values on the join columns specified for string distance joins. `fuzzyjoin` would throw an error. This change allows `NA` values to persist in left, right, anti, semi, and full joins. Two `NA` values are not considered a match. We find this behavior more desirable in the case of fuzzy joins.

- The prefix scaling factor for Jaro-Winkler distance (`max_prefix`) is an integer limiting the number of prefix characters used to boost similarity. In contrast, the analogous `stringdist` parameter `bt` is a proportion of the string length, making the prefix contribution relative rather than fixed.

- Some `stringdist` arguments are not supported. Implementation is challenging, but not impossible. We could prioritize their inclusion if user demand were sufficient:
    - `useBytes`
    - `weight`
    - `useNames` is not relevant to the final output of the fuzzy join. There is no need to implement this.

- For interval joins, we allow for both `real` and `integer` join types!
    - The integer mode is designed to match the behavior of `IRanges`, which is used in `fuzzyjoin`. You will need to coerce the join columns to integers to enable this mode.
    - The `real` mode behaves more like `data.table`'s `foverlaps`.
    - An `auto` mode (default) will determine the method to use based on the input column type

- `soundex` implementations differ slightly.
    - Our implementation considers multiple encodings in the case of prefixes, as is specified in the [National Archives Standard](https://www.archives.gov/research/census/soundex).
    - How consecutive similar letters and consonant separators behave is implemented differently. "Coca Cola" would match to "cuckoo" only in our system, while "overshaddowed" and "overwrought" would only match in theirs.

## Acknowledgements

- The `extendr` team. This project would not be possible without their great project. Specific shoutout to Alberson Miranda, Josiah Parry, and KB Vernon for providing feedback during the CRAN submission process.
- The `fuzzyjoin` R package. Much of the project is meant to replicate their APIs and special cases handling.
- The `stringdist` R package was used as a source of truth when developing string distance algorithms. `stringdist` is insanely performant.
- The `textdistance` Rust crate. While not used in the current implementation, its algorithms were referenced early on and adapted for our purposes. Such instances are acknowledged in various places, including the relevant source code.
- The `rapidfuzz` Rust crate. When available, we tend to use `rapidfuzz` string distance algorithms due to its stellar performance.
- The `rayon` Rust crate, which enables efficient parallel data processing.
- The Washington State Department of Health and Sean Coffinger. WA DOH and Sean graciously gave time for the development and promotion of this project.
