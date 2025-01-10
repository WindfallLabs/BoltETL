"""DriverShifts report from Via"""
import pandas as pd

from bolt.utils import YearMonth, types
from . import Datasource


class DriverShifts(Datasource):
    def __init__(self):
        self.init()

    def transform_one(self, filepath: str, df: pd.DataFrame) -> pd.DataFrame:
        """Processes Driver Shifts from VOC."""
        shift_date_cols = [
            "Date",
            "Planned Shift Start Time",
            "Shift Start Time",
            "Planned Shift End Time",
            "Shift End Time",
        ]

        df = df.copy()
        yearmonth: int = YearMonth.from_filepath(filepath).yearmonth
        # Parse dates
        df[shift_date_cols] = df[shift_date_cols].map(pd.to_datetime)
        # Cast types
        df["Plate Number"] = df["Plate Number"].astype(types.pyarrow_string)

        # Rename existing 'Service' field to 'Service Type'
        df.rename({"Service": "Service Type"}, axis=1, inplace=True)

        # Shift Durations
        df["Planned Shift Duration"] = (
            df["Planned Shift End Time"] - df["Planned Shift Start Time"]
        ).apply(lambda x: x.total_seconds() / 60 / 60)

        df["Shift Duration"] = (
            df["Shift End Time"] - df["Shift Start Time"]
        ).apply(lambda x: x.total_seconds() / 60 / 60)

        # Extra Time
        df["Extra Time"] = (
            df["Shift Duration"] - df["Planned Shift Duration"]
        )

        # Missed Signoff
        df["Missed Signoff"] = "No"
        df.loc[df["Extra Time"] > 2, "Missed Signoff"] = "Yes"

        # Zero-Out Extra Time that's not a Missed Signoff
        df.loc[df["Missed Signoff"] != "Yes", "Extra Time"] = 0

        # Create new aggregating 'Service' column {Sun, Sat, Weekday}
        df["Day of Week"] = df["Date"].apply(lambda x: x.strftime("%a"))
        df["Service"] = "Weekday"
        df.loc[df["Day of Week"] == "Sat", "Service"] = "Saturday"
        df.loc[df["Day of Week"] == "Sun", "Service"] = "Sunday"
        df["YMTH"] = yearmonth

        # TODO: narrow down which columns are useful and limit to only those; then update the SQL file.

        return df

    def transform(self):
        """Process each raw dataframe and concat."""
        dfs = []
        for fpath, df in self.raw:
            dfs.append(self.transform_one(fpath, df))
        self.data = pd.concat(dfs).reset_index(drop=True)
        return
