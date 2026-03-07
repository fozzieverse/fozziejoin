import polars as pl
import fozziejoin
import pytest
from collections import Counter

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
        ('hamming', 1, None, None, None, ["JOHN SMITH"]),
        ('levenshtein', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH"]),
        ('levenshtein', 3, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
        ('dl', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH"]),
        ('dl', 3, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
        ('osa', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH"]),
        ('osa', 3, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
        ('lcs', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH"]),
        ('jaccard', 0.9, None, None, 3, ["JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
        ('cosine', 0.9, None, None, 2, ["JOHN SMITH", "JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
        ('qgram', 5, None, None, 2, ["JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
        ('jw', 0.5, 0.1, 2, 2, ["JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
    ]
)
def test_string_inner_join(method, max_distance, prefix_weight, max_prefix, q, expected_names):
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
    actual = joined["LEFT_FULL_NAME"].to_list()
    assert Counter(actual) == Counter(expected_names)


@pytest.mark.parametrize(
    "method, max_distance, prefix_weight, max_prefix, q, expected_names",
    [
        ('hamming', 1, None, None, None, ["JOHN SMITH", "JACK DOE", "SILLY BILLY", None]),
        ('levenshtein', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", "SILLY BILLY", None]),
        ('levenshtein', 3, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", "SILLY BILLY", None]),
        ('dl', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", "SILLY BILLY", None]),
        ('dl', 3, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", "SILLY BILLY", None]),
        ('osa', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", "SILLY BILLY", None]),
        ('osa', 3, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", "SILLY BILLY", None]),
        ('lcs', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", "SILLY BILLY", None]),
        ('jaccard', 0.9, None, None, 3, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", "SILLY BILLY", None]),
        ('cosine', 0.9, None, None, 2, ["JOHN SMITH", "JOHN SMITH", "JOHN SMITH", "JACK DOE", "SILLY BILLY", None]),
        ('qgram', 5, None, None, 2, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", "SILLY BILLY", None]),
        ('jw', 0.5, 0.1, 2, 2, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", "SILLY BILLY", None]),
    ]
)
def test_string_join_left(method, max_distance, prefix_weight, max_prefix, q, expected_names):
    """Parameterize the string distance join tests with various configurations using a left join."""
    joined = fozziejoin.string_distance_join(
        LEFT_DF, RIGHT_DF,
        left_on=['LEFT_FULL_NAME'],
        right_on=['RIGHT_FULL_NAME'],
        how='left',  # Change to 'left' for a left join
        method=method,
        max_distance=max_distance,
        q=q,
        prefix_weight=prefix_weight,
        max_prefix=max_prefix
    )

    # Assert that the result is a DataFrame
    assert isinstance(joined, pl.DataFrame)

    # Assert expected matches in the LEFT_FULL_NAME column
    actual = joined["LEFT_FULL_NAME"].to_list()
    assert Counter(actual) == Counter(expected_names)

@pytest.mark.parametrize(
    "method, max_distance, prefix_weight, max_prefix, q, expected_names",
    [
        ('hamming', 1, None, None, None, ["JOHN SMITH", None, None, None, None]),
        ('levenshtein', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH", None, None, None]),
        ('levenshtein', 3, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", None, None]),
        ('dl', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH", None, None, None]),
        ('dl', 3, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", None, None]),
        ('osa', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH", None, None, None]),
        ('osa', 3, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", None, None]),
        ('lcs', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH", None, None, None]),
        ('jaccard', 0.9, None, None, 3, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", None, None]),
        ('cosine', 0.9, None, None, 2, ["JOHN SMITH", "JOHN SMITH", "JOHN SMITH", "JACK DOE", None, None]),
        ('qgram', 5, None, None, 2, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", None, None]),
        ('jw', 0.5, 0.1, 2, 2, ["JOHN SMITH", "JOHN SMITH", "JACK DOE", None, None]),
    ]
)
def test_string_right_join(method, max_distance, prefix_weight, max_prefix, q, expected_names):
    """Parameterize the string distance join tests with various configurations."""
    joined = fozziejoin.string_distance_join(
        LEFT_DF, RIGHT_DF,
        left_on=['LEFT_FULL_NAME'],
        right_on=['RIGHT_FULL_NAME'],
        how='right',
        method=method,
        max_distance=max_distance,
        q=q,
        prefix_weight=prefix_weight,
        max_prefix=max_prefix
    )

    # Assert that the result is a DataFrame
    assert isinstance(joined, pl.DataFrame)

    # Assert expected matches
    actual = joined["LEFT_FULL_NAME"].to_list()
    if not Counter(actual) == Counter(expected_names):
        import pdb
        pdb.set_trace()
    assert Counter(actual) == Counter(expected_names)

@pytest.mark.parametrize(
    "method, max_distance, prefix_weight, max_prefix, q, expected_names",
    [
        ('hamming', 1, None, None, None, ["JOHN SMITH"]),
        ('levenshtein', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH"]),
        ('levenshtein', 3, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
        ('dl', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH"]),
        ('dl', 3, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
        ('osa', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH"]),
        ('osa', 3, None, None, None, ["JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
        ('lcs', 2, None, None, None, ["JOHN SMITH", "JOHN SMITH"]),
        ('jaccard', 0.9, None, None, 3, ["JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
        ('cosine', 0.9, None, None, 2, ["JOHN SMITH", "JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
        ('qgram', 5, None, None, 2, ["JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
        ('jw', 0.5, 0.1, 2, 2, ["JOHN SMITH", "JOHN SMITH", "JACK DOE"]),
    ]
)
def test_string_semi_join(method, max_distance, prefix_weight, max_prefix, q, expected_names):
    """Parameterize the string distance join tests with various configurations."""
    joined = fozziejoin.string_distance_join(
        LEFT_DF, RIGHT_DF,
        left_on=['LEFT_FULL_NAME'],
        right_on=['RIGHT_FULL_NAME'],
        how='semi',
        method=method,
        max_distance=max_distance,
        q=q,
        prefix_weight=prefix_weight,
        max_prefix=max_prefix
    )

    # Assert that the result is a DataFrame
    assert isinstance(joined, pl.DataFrame)

    # Assert expected matches
    actual = joined["LEFT_FULL_NAME"].to_list()
    assert Counter(actual) == Counter(expected_names)

@pytest.mark.parametrize(
    "method, max_distance, prefix_weight, max_prefix, q, expected_names",
    [
        ('hamming', 1, None, None, None, ["JACK DOE", "SILLY BILLY", None]),
        ('levenshtein', 2, None, None, None, ["JACK DOE", "SILLY BILLY", None]),
        ('levenshtein', 3, None, None, None, ["SILLY BILLY", None]),
        ('dl', 2, None, None, None, ["JACK DOE", "SILLY BILLY", None]),
        ('dl', 3, None, None, None, ["SILLY BILLY", None]),
        ('osa', 2, None, None, None, ["JACK DOE", "SILLY BILLY", None]),
        ('osa', 3, None, None, None, ["SILLY BILLY", None]),
        ('lcs', 2, None, None, None, ["JACK DOE", "SILLY BILLY", None]),
        ('jaccard', 0.9, None, None, 3, ["SILLY BILLY", None]),
        ('cosine', 0.9, None, None, 2, ["SILLY BILLY", None]),
        ('qgram', 5, None, None, 2, ["SILLY BILLY", None]),
        ('jw', 0.5, 0.1, 2, 2, ["SILLY BILLY", None]),
    ]
)
def test_string_anti_join(method, max_distance, prefix_weight, max_prefix, q, expected_names):
    """Parameterize the string distance join tests with various configurations."""
    joined = fozziejoin.string_distance_join(
        LEFT_DF, RIGHT_DF,
        left_on=['LEFT_FULL_NAME'],
        right_on=['RIGHT_FULL_NAME'],
        how='anti',
        method=method,
        max_distance=max_distance,
        q=q,
        prefix_weight=prefix_weight,
        max_prefix=max_prefix
    )

    # Assert that the result is a DataFrame
    assert isinstance(joined, pl.DataFrame)

    # Assert expected matches
    actual = joined["LEFT_FULL_NAME"].to_list()
    assert Counter(actual) == Counter(expected_names)


