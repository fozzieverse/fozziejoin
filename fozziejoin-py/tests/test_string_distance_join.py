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
        left_on=['ARTS_FULL_NAME'],
        right_on=['CHILD_FULL_NAME'],
        how='inner',
        method='jaccard',
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
        left_on=['ARTS_FULL_NAME'],
        right_on=['CHILD_FULL_NAME'],
        how='inner',
        method='jaccard',
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

def test_column_name_collision_suffix():
    """Shared column names should be renamed with the suffix on the RHS"""
    left = pl.DataFrame({
        "id": [1, 2],
        "name": ["ALICE", "BOB"]
    })
    right = pl.DataFrame({
        "id": [2, 3],
        "name": ["BOBBY", "CHARLIE"]
    })

    joined = fozziejoin.string_distance_join(
        left, right,
        on=["name"],
        how="inner",
        method="jaccard",
        max_distance=0.9,
        q=3,
        suffix="_right"
    )

    # Assert that the result is a DataFrame
    assert isinstance(joined, pl.DataFrame)

    # DFs have shared columns: assert suffixes have been applied properly
    assert "name" in joined.columns
    assert "name_right" in joined.columns
    assert "id" in joined.columns
    assert "id_right" in joined.columns

    # Assert that "name_right" contains RHS values
    rhs_values = joined["name_right"].to_list()
    assert all(val in ["BOBBY", "CHARLIE"] for val in rhs_values)


def test_multi_column_fuzzy_join():
    """Multi column joins should work"""
    # Create left and right DataFrames
    left = pl.DataFrame({
        "id": [1, 2],
        "first": ["JOHN", "JACK"],
        "last": ["SMITH", "DOE"]
    })

    right = pl.DataFrame({
        "key": [10, 20],
        "first": ["JON", "JACKSON"],
        "last": ["SMYTHE", "DOE"]
    })

    # Run fuzzy join on both columns
    result = fozziejoin.string_distance_join(
        left=left,
        right=right,
        left_on=["first", "last"],
        right_on=["first", "last"],
        how="left",
        method="jaccard",
        max_distance=0.9,
        q=3,
        distance_col="dist",
        suffix="_right"
    )

    # Check that result is a DataFrame
    assert isinstance(result, pl.DataFrame)

    # Check that all left rows are preserved
    assert result.shape[0] == left.shape[0]

    # Check that two distance columns were added
    dist_cols = [col for col in result.columns if col.startswith("dist_")]
    assert len(dist_cols) == 2
    for col in dist_cols:
        assert result[col].dtype == pl.Float64

    # Optional: check that suffix was applied to overlapping columns
    assert "first_right" in result.columns
    assert "last_right" in result.columns

