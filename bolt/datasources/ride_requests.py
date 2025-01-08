"""Ride Requests report from Via"""
import datetime as dt

import pandas as pd
from rich.console import Console

from . import Datasource


console = Console()

GRACE_ARRIVAL_MINS = 4


class RideRequests(Datasource):
    def __init__(self):
        self.table_name = "RideRequests"
        self.raw_path = r"raw\Via - Ride Requests\*-Ride Requests*.xlsx"
        self.cache_path = "RideRequests.feather"
        self.raw: tuple[str, pd.DataFrame]|None = None
        self.data: pd.DataFrame|None = None

    def extract(self):
        """Open each csv file into a tuple of filepath-dataframe pairs."""
        with console.status("RideRequests: Extracting..."):
            self.raw = [(p, pd.read_excel(p, dtype_backend="pyarrow")) for p in self.files]
        return

    def transform(self):
        """Process each raw dataframe and concat."""
        with console.status("RideRequests: Transforming..."):
            req = pd.concat(i[1] for i in self.raw).reset_index(drop=True)
            req["Request Creation Date"] = pd.to_datetime(req["Request Creation Date"], errors="coerce")
            req["Request Creation Time"] = pd.to_datetime(req["Request Creation Time"], errors="coerce")
            req["Requested Pickup Time"] = pd.to_datetime(req["Requested Pickup Time"], errors="coerce")
            req["Requested Dropoff Time"] = pd.to_datetime(req["Requested Dropoff Time"], errors="coerce")
            req["Original Planned Pickup Time"] = pd.to_datetime(req["Original Planned Pickup Time"], errors="coerce")
            req["Cancellation Time"] = pd.to_datetime(req["Cancellation Time"], errors="coerce")
            req["Pickup Location Arrival Time"] = pd.to_datetime(req["Pickup Location Arrival Time"], errors="coerce")
            req["Actual Pickup Time"] = pd.to_datetime(req["Actual Pickup Time"], errors="coerce")
            req["Actual Dropoff Time"] = pd.to_datetime(req["Actual Dropoff Time"], errors="coerce")
            # Requests - New Fields
            req["Pickup Status"] = req.apply(lambda x: "Late" if x["Pickup Location Arrival Time"] > (x["Requested Pickup Time"] + dt.timedelta(minutes=GRACE_ARRIVAL_MINS)) else "On Time", axis=1)
            req["Mins Late"] = req.apply(lambda x: round((x["Pickup Location Arrival Time"] - x["Requested Pickup Time"]).total_seconds() / 60.0, 2) if x["Pickup Status"] == "Late" else 0.0, axis=1)
            #req["Late Cancellation"] = req.apply(is_late_cancellation, axis=1)
            self.data = req
        return

    def validate(self):
        return  # TODO:

    def load(self, database):
        self.df.to_sql(self.name, database.engine)
        return

    def process_one(self, filepath: str, df: pd.DataFrame) -> pd.DataFrame:
        ...
