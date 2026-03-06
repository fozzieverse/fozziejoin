import polars as pl
import fozziejoin
import pytest

# Constant left and right DataFrames
LEFT_DF = pl.DataFrame({
    "id": [1, 2, 3, 4],
    "LEFT_FULL_NAME": ["JOHN SMITH", "JACK DOE", "SILLY BILLY", None]
})

RIGHT_DF = pl.DataFrame({
    "key": [2, 3, 4, 5, 6],
    "RIGHT_FULL_NAME": ["JON SMITH", "JACKSON DOE", "ZENDAYA", "JOHN SMITH", None]
})

@pytest.mark.parametrize(
    "method, max_distance, prefix_weight, max_prefix, q, expected_names",
    [
        ('hamming', 1, None, None, None, {"JOHN SMITH"}),
        ('levenshtein', 2, None, None, None, {"JOHN SMITH"}),
        ('levenshtein', 3, None, None, None, {"JOHN SMITH", "JACK DOE"}),
        ('dl', 2, None, None, None, {"JOHN SMITH"}),
        ('dl', 3, None, None, None, {"JOHN SMITH", "JACK DOE"}),
        ('osa', 2, None, None, None, {"JOHN SMITH"}),
        ('osa', 3, None, None, None, {"JOHN SMITH", "JACK DOE"}),
        ('lcs', 2, None, None, None, {"JOHN SMITH"}),
        ('jaccard', 0.9, None, None, 3, {"JOHN SMITH", "JACK DOE"}),
        ('cosine', 0.9, None, None, 2, {"JOHN SMITH", "JACK DOE"}),
        ('qgram', 5, None, None, 2, {"JOHN SMITH", "JACK DOE"}),
        ('jw', 0.5, 0.1, 2, 2, {"JOHN SMITH", "JACK DOE"}),
    ]
)
def test_string_join(method, max_distance, prefix_weight, max_prefix, q, expected_names):
    """Parameterize the string distance join tests with various configurations."""
    joined = fozziejoin.string_distance_join(
        LEFT_DF, RIGHT_DF,
        left_on=['LEFT_FULL_NAME'],
        right_on=['RIGHT_FULL_NAME'],
        how='inner',
        method=method,
        max_distance=max_distance,
        q=q,
        prefix_weight=prefix_weight,
        max_prefix=max_prefix
    )

    # Assert that the result is a DataFrame
    assert isinstance(joined, pl.DataFrame)

    # Assert expected matches
    assert set(joined["LEFT_FULL_NAME"].to_list()) == set(expected_names)

