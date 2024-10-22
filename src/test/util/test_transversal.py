import pandas as pd
import numpy as np
from util.transversal import (
    get_current_timestamp,
    replace_last_occurrence,
    clean_dataframe,
    set_dynamic_column_names,
    cast_fields,
)

def test_get_current_timestamp():
    # Test with the default format
    timestamp = get_current_timestamp()
    assert len(timestamp) == 19  # Check length of default timestamp

    # Test with a custom format
    custom_timestamp = get_current_timestamp("%Y-%m-%d")
    assert len(custom_timestamp) == 10  # Check length of custom timestamp

def test_replace_last_occurrence():
    assert replace_last_occurrence("hello world", "world", "there") == "hello there"
    assert replace_last_occurrence("hello world world", "world", "there") == "hello world there"
    assert replace_last_occurrence("hello world", "worlds", "there") == "hello world"  # Non-existing replacement
    assert replace_last_occurrence("hello", "o", "a") == "hella"  # Single character replacement

def test_clean_dataframe():
    df = pd.DataFrame({"A": [1, 2, -1], "B": [4, -1, 6]})
    cleaned_df = clean_dataframe(df)
    expected_df = pd.DataFrame({"A": [1, 2, np.nan], "B": [4, np.nan, 6]})
    expected_df = expected_df.astype(cleaned_df.dtypes)

    # Assert that the cleaned DataFrame equals the expected DataFrame
    pd.testing.assert_frame_equal(cleaned_df, expected_df)

    # Test with a specific value to replace
    cleaned_df_with_zero = clean_dataframe(df, value_to_replace=-1, replace_with=0)
    expected_df_with_zero = pd.DataFrame({"A": [1, 2, 0], "B": [4, 0, 6]})

    # Ensure the expected DataFrame has the same dtype as cleaned_df_with_zero
    expected_df_with_zero = expected_df_with_zero.astype(cleaned_df_with_zero.dtypes)

    # Assert that the cleaned DataFrame equals the expected DataFrame
    pd.testing.assert_frame_equal(cleaned_df_with_zero, expected_df_with_zero)

def test_set_dynamic_column_names():
    df = pd.DataFrame(columns=["A", "B", "C"])
    renamed_df = set_dynamic_column_names(df)
    assert list(renamed_df.columns) == ["column1", "column2", "column3"]  # Check if columns are renamed correctly

    df_empty = pd.DataFrame(columns=[])
    renamed_empty_df = set_dynamic_column_names(df_empty)
    assert list(renamed_empty_df.columns) == []  # Check if empty DataFrame remains empty
