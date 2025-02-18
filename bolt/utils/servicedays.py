"""Service Calender Functions
Author: Garin Wally; 2024-12-11

Functions to calculate what service is provided on what holidays and/or days
of any given year and month.
"""

import calendar
import collections
import datetime as dt
from itertools import cycle

import holidays
import polars as pl

# from numpy import nan
from bolt.utils import YearMonth, config

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
0.3.1 (2025-02-18)
- Added CalendarDim class to create `dim_calendar` SQL table
"""

# Years ahead
HORIZON = 5

calendar.setfirstweekday(1)


class CalendarDim():
    def __init__(self):
        """Create the Calendar dimension (`dim_calendar` table)."""
        self.years = list(range(2020, dt.date.today().year + HORIZON))
        # Additional VIEWs based on dim_calendar
        self.views = [
            "CREATE OR REPLACE VIEW dimv_service_days AS SELECT YMTH, Service, COUNT(Service) AS Days FROM dim_calendar GROUP BY YMTH, Service ORDER BY YMTH, Service",
            # A VIEW for NTD S-10 "Services Operated (Days)""
            "CREATE OR REPLACE VIEW view_services_operated AS SELECT FY, Service, COUNT(Service) AS Days FROM dim_calendar GROUP BY FY, Service ORDER BY FY, Service",
            "CREATE OR REPLACE VIEW dimv_month_days AS SELECT YMTH, Service, COUNT(Service) AS Days FROM dim_calendar GROUP BY YMTH, Service ORDER BY YMTH, Service",
            "",  # TODO: replace servicedays functions with VIEWs?
        ]

        # Dataframe of holiday dates and service
        holiday_dates = (
            # Get all holiday dates
            (
                pl.DataFrame(
                    [(k, v) for k, v in holidays.US(years=self.years).items()],
                    schema=["Date", "HolidayName"],
                    orient="row",
                )
                .with_columns(
                    pl.col("HolidayName").str.replace("(observed)", "").strip_chars(" ")
                )
            )
            .join(
                # Get Agency's holidays and service
                pl.DataFrame(
                    [(k, v) for k, v in config.holidays.items()],
                    schema=["Holiday", "Service"],
                    orient="row",
                ),
                on="Holiday")
        )

        # Dataframe of full calendar
        cal = calendar.Calendar()
        zips = []
        for year in self.years:
            for m in range(1, 13):
                cal_zip = zip(cal.itermonthdays(year, m), cycle(calendar.day_name))
                zips.append(
                    # Get full calendar
                    pl.DataFrame(
                        [
                            (dt.date(year, m, day), day_name)
                            for day, day_name in cal_zip
                            if day > 0
                        ],
                        schema=["Date", "DayName"],
                        orient="row",
                    )
                    # Join with holidays
                    .join(holiday_dates, on="Date", how="left")
                )

        # Full dataframe of full calendar, holidays, and other date-dimensional info        
        df = (
            pl.concat(zips, how="vertical")
            .with_columns(
                # Get service type for holidays (convert "Normal" appropriately)
                pl.when((pl.col("Service").is_null()) | (pl.col("Service") == "Normal"))
                .then(
                    pl.when(pl.col("DayName").is_in(["Saturday", "Sunday"]))
                    .then(pl.col("DayName"))
                    .otherwise(pl.lit("Weekday"))
                )
                .otherwise(pl.col("Service"))
                .alias("Service"),
                # Get Year
                pl.col("Date").dt.year().alias("Year"),
                # Get Year-Month (YMTH)
                pl.col("Date").dt.strftime("%Y%m").cast(pl.Int64).alias("YMTH"),
                # Get Fiscal Year
                (
                    pl.when(pl.col("Date").dt.month() >= 7)
                    .then(pl.col("Date").dt.year() + 1)
                    .otherwise(pl.col("Date").dt.year())
                    .alias("FY")
                ),
                # Month
                pl.col("Date").dt.month().alias("Month"),
                # Quarter
                pl.col("Date").dt.quarter().alias("Quarter"),
                # Fiscal Quarter
                (
                    pl.col("Date").dt.quarter()
                    .map_elements(
                        lambda x: {1: 3, 2: 4, 3: 1, 4: 2}[x], return_dtype=pl.Int8)
                    .alias("FQ")
                ),
            )
            .select(
                "Date",
                "YMTH",
                "Year",
                "FY",
                "Quarter",
                "FQ",
                "Month",
                "DayName",
                "Holiday",
                "Service"
            )
        )

        # Validate agency holidays
        for k, v in config.holidays.items():
            if v == "Normal":
                continue
            assert df.filter(pl.col("Holiday") == k)["Service"].unique().item() == v
        
        self.data = df


def get_holiday_service(year: int, drop_observed=False) -> pl.DataFrame:
    """Returns a DataFrame of holidays and the service provided by MUTD."""
    # Convert holiday config to DataFrame
    service = pl.DataFrame(
        [(k, v) for k, v in config.holidays.items()],
        schema=["Holiday", "Service"],
        orient="row",
    ).with_columns(pl.col("Holiday").cast(pl.Utf8))

    # Get holiday dates
    holiday_dates = pl.DataFrame(
        [(k, v) for k, v in holidays.US(years=[year]).items()],
        schema=["Date", "Holiday"],
        orient="row",
    )

    # Cast to datetime and create day column
    holiday_dates = holiday_dates.with_columns(
        [
            pl.col("Date").cast(pl.Datetime),
            pl.col("Holiday").cast(pl.Utf8),
            pl.col("Date").dt.strftime("%A").alias("Day"),
        ]
    )

    # Join with service type and sort
    holiday_df = holiday_dates.join(service, on="Holiday", how="left").sort(
        "Date", descending=True
    )

    # Drop observed holidays if requested
    if drop_observed:
        holiday_df = holiday_df.filter(~pl.col("Holiday").str.contains("observed"))

    # Sort by date
    holiday_df = holiday_df.sort("Date").with_row_index().drop("index")

    # Fill null values with "Normal" service and convert to appropriate service type
    holiday_df = holiday_df.with_columns(
        pl.when(pl.col("Service").is_null())
        .then(
            pl.when(pl.col("Day").is_in(["Saturday", "Sunday"]))
            .then(pl.col("Day"))
            .otherwise(pl.lit("Weekday"))
        )
        .otherwise(pl.col("Service"))
        .alias("Service"),
        # Set date to date (not datetime)
        pl.col("Date").dt.date(),
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
        orient="row",
    )
    df = df.with_columns(pl.col("Date").dt.date())
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
    df: pl.DataFrame = month_days.join(holiday_df, on="Date", how="left")
    # Set normal Weekday service
    df = df.with_columns(
        pl.when(
            pl.col("Service").is_null()
            & ~pl.col("DayName").is_in(["Saturday", "Sunday"])
        )
        .then(pl.lit("Weekday"))
        .otherwise(pl.col("Service"))
        .alias("Service")
    )
    # Get normal weekend service
    df = df.with_columns(
        pl.when(
            pl.col("Service").is_null()
            & pl.col("DayName").is_in(["Saturday", "Sunday"])
        )
        .then(pl.col("DayName"))
        .otherwise(pl.col("Service"))
        .alias("Service")  # ServiceType
    )
    df = df.select("Date", "DayName", "Service", "Holiday").sort("Date")
    return df


def get_full_calendar_many(*yearmonths: int):
    dfs = []
    for ymth in set(yearmonths):
        dfs.append(get_full_calendar(*YearMonth(ymth).to_year_and_month()))
    df = (
        pl.concat(dfs, how="vertical_relaxed")
        .with_columns(pl.col("Date").dt.date())
        .sort("Date")
    )
    return df


def get_service_days(year: int, month: int) -> pl.DataFrame:
    """Counts the number of days by service type."""
    full_cal_df: pl.DataFrame = get_full_calendar(
        year,
        month,
        drop_observed=True,  # Static True value
    )
    # Group the full calendar dataframe by ServiceType and count the days for each type
    df: pl.DataFrame = full_cal_df.group_by("Service").agg(  # ServiceType
        pl.col("DayName").count(), pl.col("Holiday").count()
    )
    # Add closed count (0) for year-months that don't have them
    if (
        df.filter((pl.col("Service") == "Closed")).select("Service").is_empty()
    ):  # ServiceType  # ServiceType
        df = pl.concat(
            [
                df,
                pl.DataFrame(
                    {"Service": "Closed", "DayName": 0, "Holiday": 0}
                ),  # ServiceType
            ],
            how="vertical_relaxed",
        )
    # Add Total row
    df = pl.concat([df, df.sum().fill_null("Total")])
    # Display the target time period (yearmonth)
    df = df.with_columns(
        pl.lit(str(year) + str(month).zfill(2), dtype=pl.String).alias("YMTH"),
        pl.col("DayName").alias("DayCount"),
        pl.col("Holiday").alias("HolidayCount"),
    ).select("YMTH", "Service", "DayCount", "HolidayCount")  # ServiceType
    return df


def get_service_days_many(*yearmonths: int) -> pl.DataFrame:
    """Counts the number of days by service type for multiple year-months."""
    dfs = []
    for i in set(yearmonths):
        year = int(str(i)[:4])
        month = int(str(i)[4:])
        dfs.append(get_service_days(year, month))
    df = pl.concat(dfs, how="vertical_relaxed").sort("YMTH")
    return df


def add_service(df: pl.DataFrame, ymth_col: str = "YMTH"):
    """Adds a 'Service' column to a dataframe using a date column."""
    cal_df: pl.DataFrame = get_full_calendar_many(*set(df[ymth_col])).select(
        "Date", "Service"
    )
    return df.join(cal_df, on=["Date"])


def add_service_days(
    df: pl.DataFrame, ymth_col: str = "YMTH", service_col: str = "Service"
):
    """Adds a new 'Service Days' column to a DataFrame given a Year-Month
    column and 'Service' column."""
    service_df: pl.DataFrame = get_service_days_many(*set(df[ymth_col])).select(
        "YMTH", "Service", "DayCount"
    )
    return df.join(service_df, on=[ymth_col, service_col])
