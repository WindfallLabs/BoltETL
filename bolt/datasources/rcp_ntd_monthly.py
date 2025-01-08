"""NTD MONTHLY report from Ridecheck+"""
from math import ceil

import pandas as pd

from bolt.utils import servicedays as sd
from . import Datasource, YearMonth


def to_int(s):
    if isinstance(s, str):
        return int(s.replace(",", ""))
    return s


def to_float(s):
    if isinstance(s, str):
        return float(s.replace(",", ""))
    return s


class NTDMonthly(Datasource):
    def __init__(self):
        self.table_name = "RCPMonthlyNTD"
        self.raw_path = r"raw\RCP - NTD Month Report\*-NTD_MONTH_REPORT.XLSX"
        self.cache_path = "RCPMonthlyNTD.feather"
        # Define datatype for raw data
        self.raw: tuple[str, pd.DataFrame]|None = None
        self.data: pd.DataFrame|None = None

    def extract(self):
        """Open each Excel file into a tuple of filepath-dataframe pairs."""
        self.raw = [(p, pd.read_excel(p, dtype_backend="pyarrow")) for p in self.files]
        return

    def transform(self):
        """Process each raw dataframe and concat."""
        dfs = []
        for fpath, df in self.raw:
            dfs.append(self.process_one(fpath, df))
        self.data = pd.concat(dfs).reset_index(drop=True)
        return

    def validate(self):
        return  # TODO:

    def load(self, database):
        self.df.to_sql(self.name, database.engine)
        return

    def process_one(self, filepath: str, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Processes the NTD_MONTHLY_REPORT from Ridecheck+."""
        df = raw_df.copy()
        ymth = YearMonth.from_filepath(filepath)

        try:
            # In the case that there's a TOTAL row
            drop_idx = int(df.loc[~df["SERVICE_PERIOD"].str.contains("Weekday|Sunday|Saturday")].index[0])
            df.drop(drop_idx, inplace=True)
        except IndexError:
            pass

        # CORRECTIONS: Convert str values to int/float
        int_cols = [
            "BOARD",
            "ALIGHT",
            "PASS_MILES",
            "GROSS_SURVEYS",
            #"AVG_SURVEYS",
            #"ASCH_TRIPS",
            "MTH_BOARD",
            "MTH_REV_HOURS",
            "MTH_PASS_MILES"
        ]
        df[int_cols] = df[int_cols].map(to_int)

        float_cols = [
            "REV_MILES",
            "BOARD_PER_MI",
            "BOARD_PER_HR",
            "AVG_TRIP_LEN",
            "FREQUENCY"
        ]
        df[float_cols] = df[float_cols].map(to_float)

        # Additions
        df["YMTH"] = ymth.yearmonth

        ntd_rename_cols = {
            "YMTH": "YMTH",
            "SERVICE_PERIOD": "Service",
            "MTH_BOARD": "Ridership",
            "REV_MILES": "Revenue Miles (Avg)",
            "MTH_REV_HOURS": "Revenue Hours",
            "MTH_PASS_MILES": "Passenger Miles",
        }

        # Fixed Route Stats
        df = df[ntd_rename_cols.keys()].rename(ntd_rename_cols, axis=1)
        service_df = sd.get_service_days(ymth.year, ymth.month)

        # Monthly stats
        weekday_avg_ridership = ceil(
            df.loc[df["Service"] == "Weekday"]["Ridership"].item()
            / service_df.loc["Weekday", "Days"].item()
        )
        sat_avg_ridership = ceil(
            df.loc[df["Service"] == "Saturday"]["Ridership"].item()
            / service_df.loc["Saturday", "Days"].item()
        )
        sun_avg_ridership = ceil(
            df.loc[df["Service"] == "Sunday"]["Ridership"].item()
            / service_df.loc["Sunday", "Days"].item()
        )
        df["Avg Daily Ridership"] = 0.0
        df.loc[df["Service"] == "Weekday", "Avg Daily Ridership"] = (
            weekday_avg_ridership
        )
        df.loc[df["Service"] == "Saturday", "Avg Daily Ridership"] = (
            sat_avg_ridership
        )
        df.loc[df["Service"] == "Sunday", "Avg Daily Ridership"] = sun_avg_ridership

        return df
