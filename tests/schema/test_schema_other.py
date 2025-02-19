"""Test schema functions."""

import sys

import polars as pl
import pytest

sys.path.append(r"C:\Workspace\tmpdb\.BoltETL")
from bolt.utils import schema


def test_check_names():
    # Matching dataframe-schema
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["one", "two", "three"]})
    exp_schema = (("a", pl.Int8), ("b", pl.String))
    assert schema.check_names(df, exp_schema) is None


def test_check_names_error():
    # Schema expects 'c' column
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["one", "two", "three"]})
    exp_schema = (("a", pl.Int8), ("b", pl.String), ("c", pl.String))
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        schema.check_names(df, exp_schema)


def test_check_names_schema_error():
    # Schema does not expect 'z' column
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["one", "two", "three"], "z": [4, 5, 6]})
    exp_schema = (("a", pl.Int8), ("b", pl.String))
    with pytest.raises(pl.exceptions.SchemaError):
        schema.check_names(df, exp_schema)


def test_apply_sorting():
    df = pl.DataFrame(
        {"a": [1, 2, 3], "b": ["one", "two", "three"], "y": [0, 0, 0], "z": [4, 5, 6]}
    )
    exp_df_drop = pl.DataFrame(
        {"z": [4, 5, 6], "a": [1, 2, 3], "b": ["one", "two", "three"]}
    )
    exp_schema_drop = (("z", pl.Int8), ("a", pl.Int8), ("b", pl.String), ("y", None))
    assert schema.apply_sorting(df, exp_schema_drop).equals(exp_df_drop)
    # With Drop
    exp_df = pl.DataFrame(
        {
            "z": [4, 5, 6],
            "a": [1, 2, 3],
            "b": ["one", "two", "three"],
            "y": [0, 0, 0],
        }
    )
    exp_schema = (("z", pl.Int8), ("a", pl.Int8), ("b", pl.String), ("y", None))
    assert schema.apply_sorting(df, exp_schema, drop_nonetypes=False).equals(exp_df)


# TODO:
# def test_validate(): ...
