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
    "method, max_distance, q, expected_names",
    [
        ('jaccard', 0.9, 3, {"JOHN SMITH", "JACK DOE"}),
        ('hamming', 1, None, {"JOHN SMITH"}),
        ('levenshtein', 2, None, {"JOHN SMITH"}),
        ('levenshtein', 3, None, {"JOHN SMITH", "JACK DOE"}),
        ('dl', 2, None, {"JOHN SMITH"}),
        ('dl', 3, None, {"JOHN SMITH", "JACK DOE"}),
        ('osa', 2, None, {"JOHN SMITH"}),
        ('osa', 3, None, {"JOHN SMITH", "JACK DOE"}),
        ('lcs', 2, None, {"JOHN SMITH"}),
        ('cosine', 0.9, 2, {"JOHN SMITH", "JACK DOE"}),
        ('qgram', 5, 2, {"JOHN SMITH", "JACK DOE"}),
    ]
)
def test_string_join(method, max_distance, q, expected_names):
    """Parameterize the string distance join tests with various configurations."""
    joined = fozziejoin.string_distance_join(
        LEFT_DF, RIGHT_DF,
        left_on=['LEFT_FULL_NAME'],
        right_on=['RIGHT_FULL_NAME'],
        how='inner',
        method=method,
        max_distance=max_distance,
        q=q
    )

    # Assert that the result is a DataFrame
    assert isinstance(joined, pl.DataFrame)

    # Assert expected matches
    assert set(joined["LEFT_FULL_NAME"].to_list()) == set(expected_names)

