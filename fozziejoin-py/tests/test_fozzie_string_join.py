import polars as pl
import fozziejoin

def test_string_jaccard_join():
    """The expected join values should be returned"""
    left = pl.DataFrame({
        "id": [1, 2, 3],
        "ARTS_FULL_NAME": ["JOHN SMITH", "JACK DOE", "SILLY BILLY"]
    })
    right = pl.DataFrame({
        "key": [2, 3, 4],
        "CHILD_FULL_NAME": ["JON SMITH", "JACKSON DOE", "ZENDAYA"]
    })

    joined = fozziejoin.string_distance_join(
        left, right,
        left_on='ARTS_FULL_NAME',
        right_on='CHILD_FULL_NAME',
        max_distance=0.9,
        q=3
    )

    # Assert that the result is a DataFrame
    assert isinstance(joined, pl.DataFrame)

    # Assert expected matches (you can refine this based on actual logic)
    expected_names = {"JOHN SMITH", "JACK DOE"}
    assert set(joined["ARTS_FULL_NAME"].to_list()) <= expected_names
    assert len(joined) > 0


def test_string_jaccard_join_with_null():
    """Nones should not join to one another."""
    left = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "ARTS_FULL_NAME": ["JOHN SMITH", "JACK DOE", "SILLY BILLY", None]
    })
    right = pl.DataFrame({
        "key": [2, 3, 4, 5, 6],
        "CHILD_FULL_NAME": ["JON SMITH", "JACKSON DOE", "ZENDAYA", "JOHN SMITH", None]
    })

    joined = fozziejoin.string_distance_join(
        left, right,
        left_on='ARTS_FULL_NAME',
        right_on='CHILD_FULL_NAME',
        max_distance=0.9,
        q=3
    )

    # Assert that the result is a DataFrame
    assert isinstance(joined, pl.DataFrame)

    # Assert that None values are either ignored or handled safely
    assert None not in joined["ARTS_FULL_NAME"].to_list()

    # Assert expected matches
    expected_names = {"JOHN SMITH", "JACK DOE"}
    assert set(joined["ARTS_FULL_NAME"].to_list()) <= expected_names
    assert len(joined) > 0

