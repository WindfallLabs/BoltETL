"""MUTD Service Calendar Functions
Author: Garin Wally; 2024-12-11

Functions to calculate what service is provided on what holidays and/or days
of any given year and month.
"""

import calendar
import datetime as dt
import warnings
from itertools import cycle

import holidays
import pandas as pd
from numpy import nan

calendar.setfirstweekday(1)

__version__ = "2.0.0"


# Dictionary of transit service provided by MUTD for holidays
mutd_holidays = {
    # Closed
    "New Year's Day": "Closed",
    "Independence Day": "Closed",
    "Thanksgiving": "Closed",
    "Christmas Day": "Closed",
    # Sunday Service
    "Memorial Day": "Sunday",
    "Labor Day": "Sunday",
    "Veterans Day": "Sunday",
    "Martin Luther King Jr. Day": "Sunday",
    "Washington's Birthday": "Sunday",
    # Normal Service (holidays)
    # NOTE: 'Normal' gets converted to 'Weekday', 'Saturday', 'Sunday'
    "Juneteenth National Independence Day": "Normal",
    "Columbus Day": "Normal",  # NOTE: `holidays` library doesn't call it "Indigenous Peoples' Day"
}


def _match_day(day):
    sat = "Saturday"
    sun = "Sunday"
    if day == sat:
        return sat
    elif day == sun:
        return sun
    return "Weekday"


def get_holiday_service(year: int, drop_observed=False) -> pd.DataFrame:
    """Returns a DataFrame of holidays and the service provided by MUTD."""
    service = pd.DataFrame(
        [(k, v) for k, v in mutd_holidays.items()], columns=["Holiday", "Service"]
    ).set_index("Holiday")

    holiday_dates = pd.DataFrame(
        [(k, v) for k, v in holidays.US(years=[year]).items()],
        columns=["Date", "Holiday"],
    ).set_index("Holiday")
    # Cast to datetime
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        holiday_dates["Date"] = pd.to_datetime(holiday_dates["Date"]).dt.to_pydatetime()
    # Show day name (actual)
    holiday_dates["Day"] = holiday_dates["Date"].dt.strftime("%A")
    # Join with service type and sort
    holiday_df = (
        holiday_dates.join(service).sort_values("Date", ascending=False).reset_index()
    )
    # According to Heather, drivers get the actual holiday off not "observed" holidays
    # i.e. Drop the observed holidays
    if drop_observed:
        observed_idx: list[int] = holiday_df[
            holiday_df["Holiday"].str.contains("observed")
        ].index.tolist()
        holiday_df.drop(index=observed_idx, inplace=True)
    holiday_df.sort_values("Date", inplace=True, key=lambda x: x.dt.strftime("%Y%m%d"))
    holiday_df.reset_index(drop=True, inplace=True)
    # Fill null values with "Normal" service
    holiday_df["Service"] = holiday_df["Service"].fillna("Normal")
    # Convert 'Normal' to appropriate service type
    holiday_df.loc[holiday_df["Service"] == "Normal", "Service"] = holiday_df.loc[
        holiday_df["Service"] == "Normal", "Day"
    ].apply(_match_day)
    return holiday_df


def get_month_days(year: int, month: int) -> pd.DataFrame:
    """Return a DataFrame of dates and day names for a month."""
    cal = calendar.Calendar()
    cal_zip = zip(cal.itermonthdays(year, month), cycle(calendar.day_name))
    df = pd.DataFrame(
        [
            (dt.datetime(year, month, day), day_name)
            for day, day_name in cal_zip
            if day > 0
        ],
        columns=["Date", "DayName"],
    ).set_index("Date")
    return df


def get_full_calendar(year: int, month: int, drop_observed=False) -> pd.DataFrame:
    """Returns a DataFrame of all dates in a month, the service type, and holiday names."""
    month_days: pd.DataFrame = get_month_days(year, month)
    holiday_df: pd.DataFrame = get_holiday_service(year, drop_observed=drop_observed)
    full_cal_df: pd.DataFrame = month_days.join(
        holiday_df.set_index("Date"), rsuffix="_"
    )
    # Create a new ServiceType column using the values from Service if not null and 'Saturday', 'Sunday'
    full_cal_df["ServiceType"] = full_cal_df.apply(
        lambda x: x["DayName"]
        if x["DayName"] in ["Saturday", "Sunday"] and x["Service"] is nan
        else x["Service"],
        axis=1,
    )
    # All other (nan) values should be 'Weekday'
    full_cal_df["ServiceType"] = full_cal_df["ServiceType"].fillna("Weekday")
    full_cal_df.drop(["Day", "Service"], axis=1, inplace=True)
    return full_cal_df[["DayName", "ServiceType", "Holiday"]]


def get_service_days(year: int, month: int) -> pd.DataFrame:
    """Counts the number of days by service type."""
    full_cal_df: pd.DataFrame = get_full_calendar(
        year, month, drop_observed=True
    )  # Static True value
    # Group the full calendar dataframe by ServiceType and count the days for each type
    df: pd.DataFrame = (
        full_cal_df.groupby("ServiceType")[["DayName", "Holiday"]]
        .count()
        .rename({"DayName": "Days"}, axis=1)
    )
    # Add closed count (0) for year-months that don't have them
    if "Closed" not in df.index:
        df.loc["Closed"] = (0, 0)
    # Add Total row
    df.loc["Total"] = df.sum()
    # Display the target time period (yearmonth)
    df.columns.name = str(year) + str(month).zfill(2)
    return df


def get_service_days_many(year_months: list[int]) -> pd.DataFrame:
    """Counts the number of days by service type for multiple year-months."""
    dfs = []
    for i in set(year_months):
        year = int(str(i)[:4])
        month = int(str(i)[4:])
        df = get_service_days(year, month)
        df["YMTH"] = i
        dfs.append(df)
    x = pd.concat(dfs).reset_index().set_index("YMTH")
    x.columns.name = None
    return x


def add_service_days(df: pd.DataFrame, ymth_col: str = "YMTH", service_col: str = "Service"):
    """Adds a new 'Service Days' column to a DataFrame given a Year-Month
    column and 'Service' column."""
    df = df.copy()
    service_df = (
        get_service_days_many(set(df[ymth_col]))
        .rename({"Days": "Service Days"}, axis=1)
        .reset_index().rename({"ServiceType": service_col}, axis=1)
        .drop("Holiday", axis=1)
    )
    df = pd.merge(df, service_df, on=[ymth_col, service_col])
    return df
