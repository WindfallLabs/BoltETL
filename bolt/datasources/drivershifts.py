"""DriverShifts report from Via"""
import pandas as pd

from bolt.utils import YearMonth, types
from . import Datasource


class DriverShifts(Datasource):
    def __init__(self):
        super().__init__()

    def transform_one(self, filepath: str, df: pd.DataFrame) -> pd.DataFrame:
        """Processes individual Driver Shifts files from VOC."""
        df = df.copy()
        yearmonth: int = YearMonth.from_filepath(filepath).yearmonth
        df["YMTH"] = yearmonth
        self.logger.info(f"Added YMTH ({yearmonth}) to {filepath}")
        return df

    def transform(self):
        """Process the concatinated Driver Shifts files."""
        dfs = []
        for fpath, df in self.raw:
            dfs.append(self.transform_one(fpath, df))
        df = pd.concat(dfs).reset_index(drop=True)
        self.logger.info(f"Concatinated {len(self.raw)} files")

        shift_date_cols = [
            "Date",
            "Planned Shift Start Time",
            "Shift Start Time",
            "Planned Shift End Time",
            "Shift End Time",
        ]

        # Parse dates
        df[shift_date_cols] = df[shift_date_cols].map(pd.to_datetime)
        self.logger.info(f"Converted fields to pd.datetime: {shift_date_cols}")

        # Drop 'Plate Number' field due to type errors
        #df["Plate Number"] = df["Plate Number"].to_string().astype(types.pyarrow_string)
        #self.logger.info("Casted field 'Plate Number' to string")
        df.drop("Plate Number", axis=1, inplace=True)
        self.logger.info("Dropped field 'Plate Number'")

        # Rename
        df.rename({"Service": "Service Type"}, axis=1, inplace=True)
        self.logger.info("Renamed field 'Service' to 'Service Type'")

        # Shift Durations
        df["Planned Shift Duration"] = (
            df["Planned Shift End Time"] - df["Planned Shift Start Time"]
        ).apply(lambda x: x.total_seconds() / 60 / 60)
        self.logger.info("Calculated field 'Planned Shift Duration'")

        df["Shift Duration"] = (
            df["Shift End Time"] - df["Shift Start Time"]
        ).apply(lambda x: x.total_seconds() / 60 / 60)
        self.logger.info("Calculated field 'Shift Duration'")

        # Extra Time
        df["Extra Time"] = (
            df["Shift Duration"] - df["Planned Shift Duration"]
        )
        self.logger.info("Calculated field 'Extra Time'")

        # Missed Signoff
        df["Missed Signoff"] = "No"
        self.logger.info("Add column: 'Missed Signoff' (default 'No')")
        df.loc[df["Extra Time"] > 2, "Missed Signoff"] = "Yes"
        self.logger.info("Calculated 'Missed Signoff' to 'Yes' where 'Extra Time' > 2")

        # Zero-Out Extra Time that's not a Missed Signoff
        df.loc[df["Missed Signoff"] != "Yes", "Extra Time"] = 0
        self.logger.info("Zeroed-Out 'Extra Time' where 'Missed Signoff' != 'Yes'")

        # Create new aggregating 'Service' column {Sun, Sat, Weekday}
        df["Day of Week"] = df["Date"].apply(lambda x: x.strftime("%a"))
        self.logger.info("New column: 'Day of Week': %a")

        df["Service"] = "Weekday"
        df.loc[df["Day of Week"] == "Sat", "Service"] = "Saturday"
        df.loc[df["Day of Week"] == "Sun", "Service"] = "Sunday"
        self.logger.info("New column and calculated: 'Service'")

        # TODO: narrow down which columns are useful and limit to only those; then update the SQL file.

        self.data = df
        return
