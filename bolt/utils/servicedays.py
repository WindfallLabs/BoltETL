"""Service Calender Functions
Author: Garin Wally; 2024-12-11

Functions to calculate what service is provided on what holidays and/or days
of any given year and month.
"""

import calendar
import collections
import datetime as dt
import warnings
from itertools import cycle

import holidays
import pandas as pd
import polars as pl
#from numpy import nan

from bolt.utils import config

__changelog__ = """
0.1.0 (2024-10):
- Created (still exists as 'service_days_v1')
0.2.0 (2024-12):
- Modified and moved (exists as '_servicedays.py')
0.2.1 (2025-01)
- Added `get_weekday_count`
- Added `get_service_days_many`
- Added `add_service_days`
0.3.0 (2025-02-01):
- Changed: refactored to use polars rather than pandas
"""

calendar.setfirstweekday(1)


def get_holiday_service(year: int, drop_observed=False) -> pl.DataFrame:
    """Returns a DataFrame of holidays and the service provided by MUTD."""
    # Convert holiday config to DataFrame
    service = pl.DataFrame(
        [(k, v) for k, v in config.holidays.items()],
        schema=["Holiday", "Service"],
        orient="row"
    ).with_columns(pl.col("Holiday").cast(pl.Utf8))

    # Get holiday dates
    holiday_dates = pl.DataFrame(
        [(k, v) for k, v in holidays.US(years=[year]).items()],
        schema=["Date", "Holiday"],
        orient="row"
    )

    # Cast to datetime and create day column
    holiday_dates = (
        holiday_dates
        .with_columns([
            pl.col("Date").cast(pl.Datetime),
            pl.col("Holiday").cast(pl.Utf8),
            pl.col("Date").dt.strftime("%A").alias("Day")
        ])
    )

    # Join with service type and sort
    holiday_df = (
        holiday_dates
        .join(
            service,
            on="Holiday",
            how="left"
        )
        .sort("Date", descending=True)
    )

    # Drop observed holidays if requested
    if drop_observed:
        holiday_df = holiday_df.filter(
            ~pl.col("Holiday").str.contains("observed")
        )

    # Sort by date
    holiday_df = (
        holiday_df
        .sort("Date")
        .with_row_index()
        .drop("index")
    )

    # Fill null values with "Normal" service and convert to appropriate service type
    holiday_df = holiday_df.with_columns(
        pl.when(pl.col("Service").is_null())
        .then(
            pl.when(pl.col("Day").is_in(["Saturday", "Sunday"]))
            .then(pl.col("Day"))
            .otherwise(pl.lit("Weekday"))
        )
        .otherwise(pl.col("Service"))
        .alias("Service")
    )

    return holiday_df


def get_month_days(year: int, month: int) -> pl.DataFrame:
    """Return a DataFrame of dates and day names for a month."""
    cal = calendar.Calendar()
    cal_zip = zip(cal.itermonthdays(year, month), cycle(calendar.day_name))
    df = pl.DataFrame(
        [
            (dt.datetime(year, month, day), day_name)
            for day, day_name in cal_zip
            if day > 0
        ],
        schema=["Date", "DayName"],
        orient="row"
    )
    return df


def get_weekday_count(year: int, month: int) -> dict[str, int]:
    """Return a dict of day name and the count of those days in the given month."""
    month_days: pl.Series = get_month_days(year, month).select("DayName")
    cnts: dict[str, int] = dict(
        collections.Counter(month_days.to_dict(as_series=False)["DayName"])
    )
    return cnts


def get_full_calendar(year: int, month: int, drop_observed=False) -> pl.DataFrame:
    """Returns a DataFrame of all dates in a month, the service type, and holiday names."""
    month_days: pl.DataFrame = get_month_days(year, month)
    holiday_df: pl.DataFrame = get_holiday_service(year, drop_observed=drop_observed)
    full_cal_df: pl.DataFrame = month_days.join(
        holiday_df,
        on="Date",
        how="left"
    )
    full_cal_df = full_cal_df.with_columns(
        pl.when(
            (pl.col("DayName").is_in(["Saturday", "Sunday"])) & 
            (pl.col("Service").is_null())
        )
        .then(pl.col("DayName"))
        .otherwise(pl.lit("Weekday"))
        .alias("Service")  # ServiceType
    ).select("Date", "DayName", "Service", "Holiday")  # ServiceType
    return full_cal_df


def get_service_days(year: int, month: int) -> pl.DataFrame:
    """Counts the number of days by service type."""
    full_cal_df: pl.DataFrame = get_full_calendar(
        year,
        month,
        drop_observed=True  # Static True value
    )
    # Group the full calendar dataframe by ServiceType and count the days for each type
    df: pl.DataFrame = (
        full_cal_df.group_by("Service")  # ServiceType
        .agg(
            pl.col("DayName").count(),
            pl.col("Holiday").count()
        )
    )
    # Add closed count (0) for year-months that don't have them
    if df.filter((pl.col("Service") == "Closed")).select("Service").is_empty():  # ServiceType  # ServiceType
        df = pl.concat([
                df,
                pl.DataFrame({"Service": "Closed", "DayName": 0, "Holiday": 0})  # ServiceType
            ],
            how="vertical_relaxed"
        )
    # Add Total row
    df = pl.concat([df, df.sum().fill_null("Total")])
    # Display the target time period (yearmonth)
    df = df.with_columns(
        pl.lit(int(str(year) + str(month).zfill(2))).alias("YMTH"),
        pl.col("DayName").alias("DayCount"),
        pl.col("Holiday").alias("HolidayCount")
    ).select("YMTH", "Service", "DayCount", "HolidayCount")  # ServiceType
    return df


def get_service_days_many(*yearmonths: int) -> pl.DataFrame:
    """Counts the number of days by service type for multiple year-months."""
    dfs = []
    for i in set(yearmonths):
        year = int(str(i)[:4])
        month = int(str(i)[4:])
        df = get_service_days(year, month)
        dfs.append(df)
    x = pl.concat(dfs, how="vertical").sort("YMTH")
    return x


def add_service_days(df: pl.DataFrame, ymth_col: str = "YMTH", service_col: str = "Service"):
    """Adds a new 'Service Days' column to a DataFrame given a Year-Month
    column and 'Service' column."""
    #df = df.copy()
    service_df: pl.DataFrame = (
        get_service_days_many(*set(df[ymth_col]))
        .select("YMTH", "Service", "DayCount")
    )
    return df.join(service_df, on=["YMTH", "Service"])
