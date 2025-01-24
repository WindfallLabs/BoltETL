"""NTD S-10 report from Via"""

import pandas as pd

from bolt.utils import YearMonth
from bolt.utils import servicedays as sd

from . import Datasource


class ViaS10(Datasource):
    def __init__(self):
        super().__init__()

    def transform_one(self, filepath: str, raw_df: pd.DataFrame) -> pd.DataFrame:
        df = raw_df.copy()
        # Add yearmonth
        ymth: YearMonth = YearMonth.from_filepath(filepath)
        df["YMTH"] = ymth.yearmonth

        # Get service days
        service_cal = sd.get_full_calendar(ymth.year, ymth.month)

        # Calc Deadheads
        df["Deadhead Miles"] = df["Actual Vehicle Miles"] - df["Vehicle Revenue Miles"]
        df["Deadhead Hours"] = df["Actual Vehicle Hours"] - df["Vehicle Revenue Hours"]

        # Add service type
        df["Date"] = pd.to_datetime(df["Date"])
        df = pd.merge(df, service_cal, left_on="Date", right_index=True)
        df.rename({"ServiceType": "Service"}, axis=1, inplace=True)

        # Order matters!
        col_names = [
            "YMTH",
            "Vehicles Operated in Maximum Service",
            "Unlinked Passenger Trips",
            "Vehicle Revenue Miles",
            "Vehicle Revenue Hours",
            "Passenger Miles Traveled",
            "Actual Vehicle Miles",
            "Actual Vehicle Hours",
            "Deadhead Miles",
            "Deadhead Hours",
        ]

        agg_funcs = [
            "max",
            "max",
            "sum",
            "sum",
            "sum",
            "sum",
            "sum",
            "sum",
            "sum",
            "sum",
        ]

        rename_values = [
            "YMTH",
            "VOMS",
            "Ridership (DR)",
            "Vehicle Revenue Miles (DR)",
            "Vehicle Revenue Hours (DR)",
            "Passenger Miles (DR)",
            "Total Vehicle Miles (DR)",
            "Total Vehicle Hours (DR)",
            "Deadhead Miles",  # Same
            "Deadhead Hours",  # Same
        ]

        # Validation:
        assert (
            len(col_names) == len(agg_funcs) == len(rename_values)
        ), "Problem with renames or agg columns"
        # Pair up the columns, aggregation funcs, and new names
        agg_dict = dict(zip(col_names, agg_funcs))
        renames = dict(zip(col_names, rename_values))
        rename_values.insert(1, "Service")

        # Aggregate
        df = (
            df.groupby("Service")
            .agg(agg_dict)
            .reset_index()
            .rename(renames, axis=1)[rename_values]
        )

        # We don't need the row with "Closed" (and it )
        if "Closed" in set(df["Service"]):
            df = df.drop(df[df["Service"] == "Closed"].index)
        # Sort
        # TODO: make this sorter a util function?
        df = df.sort_values(
            "Service", key=lambda x: x.map(["Weekday", "Saturday", "Sunday"].index)
        ).reset_index(drop=True)
        df = df.reset_index(drop=True)
        return df

    def transform(self):
        """Process each raw dataframe and concat."""
        dfs = []
        for fpath, df in self.raw:
            try:
                processed_df: pd.DataFrame = self.transform_one(fpath, df)
            except Exception as e:
                print(f"Failed to process: {fpath}")
                raise e
            dfs.append(processed_df)
        self.data = pd.concat(dfs)
        return
