"""CleverReports Report 0174 - Incident Adjusted Distance and Time"""
import re
from typing import Literal

import pandas as pd

from . import Datasource, YearMonth


class CR0174(Datasource):
    def __init__(self):
        self.table_name = "CR0174"
        self.raw_path = r"raw\CR - CR0174\*CR-0174*.csv"
        self.cache_path = "CR-0174.feather"
        # Define datatype for raw data
        self.raw: tuple[str, pd.DataFrame]|None = None
        self.data: pd.DataFrame|None = None
        #super().__init__()  # TODO: this would avoid defining self.data

    def extract(self):
        """Open each csv file into a tuple of filepath-dataframe pairs."""
        self.raw = [(p, pd.read_csv(p, dtype_backend="pyarrow")) for p in self.files]
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
        """Processes a raw CR-0174 report (DataFrame)."""
        df = raw_df.copy()
        #yearmonth: int = int(re.findall(r"^\d{6}", filepath.split("\\")[-1])[0])
        ymth: YearMonth = YearMonth.from_filepath(filepath)
        service_day: Literal["Weekday", "Saturday", "Sunday"] = re.findall(r"(?i)weekday|saturday|sunday", filepath)[0].title()
        # Set columns from the first row
        df.columns = df.loc[0].values
        # Drop the first row (now a duplicate of column names)
        df.drop(0, inplace=True)
        # Rename the last row's "Route" value from NaN to "Total"
        df.loc[len(df), "Route"] = "Total"
        # Drop the "Total" row
        df.drop(df.loc[df["Route"] == "Total"].index, inplace=True)
        df["Route"] = df["Route"].apply(lambda x: x.split(" - ")[1] if "-" in x else x)
        cols: list[str] = list(df.columns)
        # Add columns
        df["YMTH"] = ymth.yearmonth
        df["Service"] = service_day
        # Configure dataframe column sorting
        cols.insert(0, "YMTH")
        cols.insert(1, "Service")
        # Reset the order of columns
        df = df[cols]
        # Specify columns to drop
        drop_columns = [
            "Deadhead Distance (S)",
            "Deadhead Distance (A)",
            "Deadhead Distance (I)",
            "Deadhead Hours (S)",
            "Deadhead Hours (A)",
            "Deadhead Hours (I)",
        ]
        # Drop 'em
        df.drop(drop_columns, axis=1, inplace=True)

        # Fix column values (str -> float)
        for col in df.columns:
            if col in ["Route", "Weekday", "Service", "YMTH", "Revenue Distance (I)", "Revenue Hours (I)"]:
                continue
            df[col] = df[col].astype(str).str.replace(",", "").astype(float)
        return df