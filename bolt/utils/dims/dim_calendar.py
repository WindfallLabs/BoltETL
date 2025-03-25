"""Calendar Dimension."""

import calendar
import datetime as dt
from itertools import cycle

import holidays
import polars as pl

from bolt.core._datasource import Datasource
from bolt.core._options import Options

calendar.setfirstweekday(1)

options = Options(
    # TODO: some way to toggle ignored
    kwargs={"year_range": 25}
)

dim_calendar = Datasource(
    name="dim_calendar", source_dir=None, source_filename=None, options=options
)


@dim_calendar.data_wrapper
def data(obj):
    this_year = dt.datetime.now().year
    years = range(
        this_year - obj.options.year_range, this_year + obj.options.year_range
    )

    cal = calendar.Calendar()

    holiday_dates = (
        pl.DataFrame(
            [(k, v) for k, v in holidays.US(years=years).items()],
            schema=["Date", "HolidayName"],
            orient="row",
        )
        .with_columns(
            pl.col("HolidayName")
            .str.replace("(observed)", "")
            .str.strip_chars(" ()")
            .alias("Holiday")
        )
        .sort(pl.col("Date"))
    )

    zips = []
    for year in years:
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

    df_calendar = (
        pl.concat(zips, how="vertical")
        .with_columns(
            # Get Year
            pl.col("Date").dt.year().alias("Year"),
            # Get Year-Month (YMTH)
            pl.col("Date").dt.strftime("%Y%m").cast(pl.Int64).alias("YMTH"),
            # Get Fiscal Year
            (
                pl.when(pl.col("Date").dt.month() >= 7)
                .then(pl.col("Date").dt.year() + 1)
                .otherwise(pl.col("Date").dt.year())
                .alias("FiscalYear")
            ),
            # Month
            pl.col("Date").dt.month().alias("Month"),
            pl.col("Date").dt.strftime("%B").alias("MonthName"),
            pl.col("Date").dt.strftime("%b").alias("MonthAbbr"),
            # Quarter
            pl.col("Date").dt.quarter().alias("Quarter"),
            # Fiscal Quarter
            (
                pl.col("Date")
                .dt.quarter()
                .map_elements(
                    lambda x: {1: 3, 2: 4, 3: 1, 4: 2}[x], return_dtype=pl.Int8
                )
                .alias("FiscalQuarter")
            ),
            # Day Abbr
            pl.col("Date").dt.strftime("%a").alias("DayAbbr"),
            # Day Type
            (
                pl.when(pl.col("DayName").is_in(["Saturday", "Sunday"]))
                .then(pl.lit("Weekend"))
                .otherwise(pl.lit("Weekday"))
                .alias("DayType")
            ),
        )
        .select(
            "Date",
            "YMTH",
            "Year",
            "FiscalYear",
            "Quarter",
            "FiscalQuarter",
            "Month",
            "MonthName",
            "MonthAbbr",
            "DayName",
            "DayAbbr",
            "DayType",
            "Holiday",
        )
    )

    return df_calendar
