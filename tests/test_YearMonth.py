"""Tests for YearMonth object."""

import sys
import datetime as dt
from pathlib import Path

import polars as pl
import pytest

# Assuming the YearMonth class is in a module called yearmonth
sys.path.append(r"C:\Workspace\tmpdb\.BoltETL")
from bolt.utils import YearMonth


def test_basic_initialization():
    # Test string initialization
    ym = YearMonth("202401")
    assert ym.year == 2024
    assert ym.month == 1
    assert str(ym) == "202401"
    assert int(ym) == 202401

    # Test integer initialization
    ym = YearMonth(202402)
    assert ym.year == 2024
    assert ym.month == 2


def test_invalid_initialization():
    # Test invalid formats
    with pytest.raises(ValueError):
        YearMonth("2024")  # Too short
    with pytest.raises(ValueError):
        YearMonth("20240101")  # Too long
    with pytest.raises(ValueError):
        YearMonth("abcdef")  # Non-numeric
    with pytest.raises(ValueError):
        YearMonth("202500")  # No Month 0
    with pytest.raises(ValueError):
        YearMonth("202513")  # No Month 13


def test_from_date():
    test_date = dt.date(2024, 3, 15)
    ym = YearMonth.from_date(test_date)
    assert ym.year == 2024
    assert ym.month == 3
    assert str(ym) == "202403"


def test_from_date_series():
    df = pl.DataFrame({"dates": [dt.date(2024, 1, 1), dt.date(2024, 2, 1)]})
    df = df.with_columns(
        YearMonth.from_date_series(pl.col("dates"))
    )
    expected = pl.Series("YMTH", [202401, 202402])
    assert df["YMTH"].equals(expected)


def test_from_date_string():
    # Test different dt.date formats
    ym = YearMonth.from_date_string("2024-01-15", "%Y-%m-%d")
    assert str(ym) == "202401"

    ym = YearMonth.from_date_string("15/03/2024", "%d/%m/%Y")
    assert str(ym) == "202403"


def test_from_filepath():
    # Test various filepath formats
    ym = YearMonth.from_filepath("202401_data.csv")
    assert str(ym) == "202401"

    ym = YearMonth.from_filepath(Path("data/202402_report.xlsx"))
    assert str(ym) == "202402"

    with pytest.raises(IndexError):
        YearMonth.from_filepath("invalid_filename.csv")


def test_from_ints():
    result = YearMonth.from_ints(2024, 1)
    assert result == YearMonth("202401")

    result = YearMonth.from_ints(2024, 12)
    assert result == YearMonth("202412")

    # Test month padding
    result = YearMonth.from_ints(2024, 2)
    assert result == YearMonth("202402")


def test_to_year_and_month():
    ym = YearMonth("202401")
    year, month = ym.to_year_and_month()
    assert year == 2024
    assert month == 1


def test_as_series():
    df = pl.DataFrame().with_columns(
        YearMonth(202502).as_series()
    )
    expected = pl.Series("YMTH", [202502])
    assert df["YMTH"].equals(expected)


def test_representation():
    ym = YearMonth("202401")
    assert repr(ym) == "<YearMonth: 202401>"
    assert str(ym) == "202401"
    assert int(ym) == 202401


def test_hash():
    d: dict[YearMonth, str] = {YearMonth(202501): "Jan", YearMonth(202502): "Feb"}
    assert set(d.keys()) == {YearMonth(202501), YearMonth(202502)}


def test_comparisons():
    ym = YearMonth(202501)
    assert ym == YearMonth(202501)
    assert ym > YearMonth(202412)
    assert ym >= YearMonth(202412)
    assert ym >= YearMonth(202501)
    assert ym < YearMonth(202602)
    assert ym <= YearMonth(202501)
    assert ym <= YearMonth(202602)
    with pytest.raises(TypeError):
        ym == "202501"
    with pytest.raises(TypeError):
        ym > "202501"
    with pytest.raises(TypeError):
        ym >= "202501"
    with pytest.raises(TypeError):
        ym < "202501"
    with pytest.raises(TypeError):
        ym <= "202501"


# Property-based tests for valid ranges
@pytest.mark.parametrize("year,month", [
    (2024, 1),
    (2024, 12),
    (1900, 1),
    (2100, 12),
])
def test_valid_year_month_combinations(year, month):
    yearmonth = int(f"{year}{month:02d}")
    ym = YearMonth(yearmonth)
    assert ym.year == year
    assert ym.month == month
