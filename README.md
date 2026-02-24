# Fozziejoin: Fuzzy Joins Without the Fluff

This is a monorepo for the `fozziejoin` project. Fozziejoin is designed to be
a performant option for fuzzy string distance joins. Its design is strongly
inspired by the
[`fuzzyjoin` package]( https://CRAN.R-project.org/package=fuzzyjoin)
R package.

Currently, the base R version of `fozziejoin` is the most developed. Our
priority is to get this package published to CRAN. Future work will adapt
the core functionality for use with `polars` dataframes in both R and Python.

The name is a playful nod to “fuzzy join” — reminiscent of 
[Fozzie Bear](https://en.wikipedia.org/wiki/Fozzie_Bear) from the Muppets.
A picture of Fozzie will appear in the repo once the legal team gets braver.
**Wocka wocka!**

## Roadmap

- [ ] `fozziejoin` for base R accepted to CRAN (see [./fozziejoin-r](./fozziejoin-r))
- [ ] `fozziejoin` for Python/polars (see [fozziejoin-py](./fozziejoin-py))
- [ ] `fozziejoinpl` for polars R dataframes (not yer initialized)

## Acknowledgements

- The `extendr` team. This project would not be possible without their great project.
- The `fuzzyjoin` package. Much of the project is meant to replicate their APIs and special cases handling.
- `stringdist` was used as a source of truth when developing string distance algorithms. `stringdist` is insanely performant.
- The `textdistance` Rust crate is used in many algorithms, and their implementation was referenced to adapt custom string distance algorithms for this project. Such instances are explicitly acknowledged in the source code.
- The `rapidfuzz` Rust crate. When available, we tend to use `rapidfuzz` string distance algorithms due to its stellar performance.
- The `rayon` Rust crate, which enables efficient parallel data processing.

## Contributions Welcome

We welcome contributions of all kinds- whether it's improving documentation,
reporting issues, or submitting pull requests. Your input helps make this 
project better for everyone.

This project follows the [Contributor Covenant](./CODE_OF_CONDUCT.md). By
participating, you agree to uphold its standards of respectful and inclusive
behavior.

If you experience or witness any problematic behavior, please [contact me via
GitHub](https://github.com/JonDDowns).
