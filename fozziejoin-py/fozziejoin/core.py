import polars as pl
from .fozziejoin import string_distance_join_rs

def string_distance_join(
    left: pl.DataFrame,
    right: pl.DataFrame,
    on: list[str] | None = None,
    left_on: list[str] | None = None,
    right_on: list[str] | None = None,
    how: str = "inner",
    method: str = "jaccard",
    max_distance: float = 0.5,
    q: int | None = 3,
    prefix_weight: float | None = None,
    max_prefix: int | None = None,
    distance_col: str | None = None,
    suffix: str = "_right",
    nthread: int | None = None,
) -> pl.DataFrame:
    # Use "on" convenience feature to assign left and right on
    if on is not None:
        if isinstance(on, list):
            left_on = right_on = on
        elif isinstance(on, str):
            raise ValueError(f"`on` must be a list. Try: ['{on}']")

    # At this point, left_on and right_on should be populated
    if left_on is None or right_on is None:
        raise ValueError("Must specify either `on` or both `left_on` and `right_on`")

    # And they should be lists of the same length
    if not isinstance(left_on, list) or not isinstance(right_on, list):
        raise ValueError("`left_on` and `right_on` must be lists")
    elif len(left_on) != len(right_on):
        raise ValueError("`left_on` and `right_on` must be the same size")

    # Call Rust-backed join
    return string_distance_join_rs(
        left,
        right,
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

