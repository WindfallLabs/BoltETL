"""Ride Requests report from Via"""
import datetime as dt

import pandas as pd

from . import Datasource


GRACE_ARRIVAL_MINS = 4


class RideRequests(Datasource):
    def __init__(self):
        self.init()

    def transform(self):
        """Process each raw dataframe and concat."""
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
