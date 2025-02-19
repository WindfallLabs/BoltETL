"""Test schema enforce function."""
import datetime as dt
import sys

import polars as pl
import pytest

sys.path.append(r"C:\Workspace\tmpdb\.BoltETL")
from bolt.utils import schema


def test_enforce_numeric():
    df = pl.DataFrame({
        "str_int": ["42", "12,345"],
        "str_float": ["42.0", "12,345.01"],
    })
    to_schema = (
        ("str_int", pl.Int32()),
        ("str_float", pl.Float64()),
    )
    result = schema.enforce(df, to_schema)
    out_schema = tuple(zip(result.columns, result.dtypes))
    assert out_schema == to_schema


def test_enforce_dates():
    df = pl.DataFrame({
        "date": [dt.date(2025, 1, 1), dt.date(2025, 2, 2)],
        "str_date": ["2025-01-01", "2025-02-02"],
    })
    to_schema = (
        ("date", pl.Date()),
        ("str_date", pl.Date()),
    )
    result = schema.enforce(df, to_schema)
    out_schema = tuple(zip(result.columns, result.dtypes))
    assert out_schema == to_schema
    assert result["date"].to_list() == result["str_date"].to_list()


def test_enforce_datetimes():
    df = pl.DataFrame({
        "datetime": [dt.datetime(2025, 1, 1, 14, 25), dt.datetime(2025, 2, 2, 15, 45)],
        "str_datetime": ["2025-01-01 02:25:00.00 PM", "2025-02-02 03:45:00.00 PM"],
    })
    to_schema = (
        ("datetime", pl.Datetime()),
        ("str_datetime", pl.Datetime()),
    )
    result = schema.enforce(df, to_schema, parse_dates=True)
    out_schema = tuple(zip(result.columns, result.dtypes))
    assert out_schema == to_schema
    assert result["datetime"].to_list() == result["str_datetime"].to_list()


def test_enforce_times():
    df = pl.DataFrame({
        "time": [dt.time(14, 25), dt.time(15, 45)],
        "str_time": ["02:25:00.00 PM", "03:45:00.00 PM"],
    })
    to_schema = (
        ("time", pl.Time()),
        ("str_time", pl.Time()),
    )
    result = schema.enforce(df, to_schema, parse_dates=True)
    out_schema = tuple(zip(result.columns, result.dtypes))
    assert out_schema == to_schema
    assert result["time"].to_list() == result["str_time"].to_list()


def test_enforce_durations():
    df = pl.DataFrame({
        "duration": [dt.timedelta(hours=2, minutes=25), dt.timedelta(hours=3, minutes=45)],
        "str_duration": ["02:25:00.00", "03:45:00.00"],
    })
    to_schema = (
        ("duration", pl.Duration()),
        ("str_duration", pl.Duration()),
    )
    result = schema.enforce(df, to_schema, parse_dates=True)
    out_schema = tuple(zip(result.columns, result.dtypes))
    assert out_schema == to_schema
    assert result["duration"].to_list() == result["str_duration"].to_list()


def test_enforce_drop_none():
    df = pl.DataFrame({
        "id": [0, 1],
        "TO_DROP": ["some", "values"]
    })
    to_schema = (
        ("id", pl.Int8()),
        ("TO_DROP", None)  # <-- Column to drop
    )
    result = schema.enforce(df, to_schema)
    assert result.columns == ["id"]


def test_enforce_handle_missing():
    df = pl.DataFrame({
        "id": [0, 1]
    })
    to_schema = (
        ("id", pl.Int8()),
        ("MISSING", pl.String())  # <-- Missing column and how to handle it
    )
    # Raise
    with pytest.raises(KeyError):
        schema.enforce(df, to_schema)

    # Add
    add_result = schema.enforce(df, to_schema, handle_missing="add")
    add_schema = tuple(zip(add_result.columns, add_result.dtypes))
    assert add_schema == to_schema

    # Ignore
    ignore_result = schema.enforce(df, to_schema, handle_missing="ignore")
    ignore_schema = tuple(zip(ignore_result.columns, ignore_result.dtypes))
    assert ignore_schema == to_schema[:-1]
