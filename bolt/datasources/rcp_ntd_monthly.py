"""NTD MONTHLY report from Ridecheck+"""

from math import ceil

import pandas as pd

from bolt.utils import YearMonth, types
from bolt.utils import servicedays as sd

from . import Datasource


class NTDMonthly(Datasource):
    def __init__(self):
        super().__init__()

    def transform_one(self, filepath: str, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Processes the NTD_MONTHLY_REPORT from Ridecheck+."""
        df = raw_df.copy()
        ymth = YearMonth.from_filepath(filepath)

        try:
            # In the case that a TOTAL row exists
            drop_idx = int(
                df.loc[
                    ~df["SERVICE_PERIOD"].str.contains("Weekday|Sunday|Saturday")
                ].index[0]
            )
            df.drop(drop_idx, inplace=True)
        except IndexError:
            pass

        # CORRECTIONS: Convert str values to int/float
        int_cols = [
            "BOARD",
            "ALIGHT",
            "PASS_MILES",
            "GROSS_SURVEYS",
            # "AVG_SURVEYS",
            # "ASCH_TRIPS",
            "MTH_BOARD",
            "MTH_REV_HOURS",
            "MTH_PASS_MILES",
        ]
        df[int_cols] = df[int_cols].map(types.to_int)

        float_cols = [
            "REV_MILES",
            "BOARD_PER_MI",
            "BOARD_PER_HR",
            "AVG_TRIP_LEN",
            "FREQUENCY",
        ]
        df[float_cols] = df[float_cols].map(types.to_float)

        # Additions
        df["YMTH"] = ymth.yearmonth

        # Rename
        ntd_rename_cols = {
            #"YMTH": "YMTH",
            "SERVICE_PERIOD": "Service",
            "MTH_BOARD": "Ridership",
            "REV_MILES": "Revenue Miles (Avg)",
            "MTH_REV_HOURS": "Revenue Hours",
            "MTH_PASS_MILES": "Passenger Miles",
        }
        df.rename(ntd_rename_cols, axis=1, inplace=True)

        return df

    def transform(self):
        """Process each raw dataframe and concat."""
        dfs = []
        for fpath, df in self.raw:
            dfs.append(self.transform_one(fpath, df))
        df = pd.concat(dfs).reset_index(drop=True)

        # Join with service days
        sdays = sd.get_service_days_many(set(df["YMTH"])).reset_index()
        sdays.rename({"ServiceType": "Service"}, axis=1, inplace=True)
        sdays.drop("Holiday", axis=1, inplace=True)
        df = pd.merge(df, sdays, on=["YMTH", "Service"])

        df["Revenue Miles (Total)"] = df["Revenue Miles (Avg)"] * df["Days"]  # Un-Averaged
        df["Avg Daily Ridership"] = (df["Ridership"] / df["Days"]).apply(lambda x: ceil(x) * 1.0)

        # Return final data
        self.data = df
        return
