"""Ride Requests report from Via"""
import datetime as dt

import pandas as pd
import pyarrow as pa

from . import Datasource
from bolt.utils import types


# Paratransit: cancellations must be requested 2 hours in advance
LATE_PARA_CANCELLATION_HRS = 2
# Microtransit: cancellations must be requested before 4:30 PM (16:30)
#  the day before

GRACE_ARRIVAL_MINS = 4


class RideRequests(Datasource):
    def __init__(self):
        super().__init__()

    def transform(self):
        """Process each raw dataframe and concat."""
        raw_dfs = [i[1] for i in self.raw]
        df = pd.concat(raw_dfs).reset_index(drop=True)
        self.logger.info(f"Loaded raw dataframes: {len(raw_dfs)}")

        # ====================================================================
        # Convert column types
        ## Datetime
        dt_cols = [
            "Request Creation Date",
            "Request Creation Time",
            "Requested Pickup Time",
            "Requested Dropoff Time",
            "Original Planned Pickup Time",
            "Cancellation Time",
            "Pickup Location Arrival Time",
            "Actual Pickup Time",
            "Actual Dropoff Time"
        ]
        for col in dt_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        self.logger.info(f"Converted columns to datatime: {dt_cols}")


        # ====================================================================
        # TODO: assess late pickups
        #df["Pickup Status"] = df.apply(lambda x: "Late" if x["Pickup Location Arrival Time"] > (x["Requested Pickup Time"] + dt.timedelta(minutes=GRACE_ARRIVAL_MINS)) else "On Time", axis=1)
        #df["Mins Late"] = df.apply(lambda x: round((x["Pickup Location Arrival Time"] - x["Requested Pickup Time"]).total_seconds() / 60.0, 2) if x["Pickup Status"] == "Late" else 0.0, axis=1)

        # Set Null 'Requested Pickup Time' values to 'Original Planned Pickup Time'
        df.loc[df["Requested Pickup Time"].isna(), "Requested Pickup Time"] = df["Original Planned Pickup Time"]
        self.logger.info("Set Null 'Requested Pickup Time' values to 'Original Planned Pickup Time'")


        # ====================================================================
        # Add & Calculate fields to determine if cancellations were requested
        #  after certain points to be considered Late (see global vars above)
        excused_reasons = [
            "appointment_change",
            "cancel_not_rider",
            "Cancel – rider not at fault",  # NOTE: dash char
            "other"
            ]

        # Add int column for difference between "Original Planned Pickup Time" and "Cancellation Time"
        df["Cancel Time Diff (hours)"] = None
        # Cast it to pyarrow floats (None -> pa.NA)
        df["Cancel Time Diff (hours)"] = df["Cancel Time Diff (hours)"].astype(types.pyarrow_float32)
        # Subselection of dataframe of Cancelled and Not Excused
        cancelled_not_excused = (
            (df["Request Status"] == "Cancel")
            & (~df["Cancellation Time"].isna())
            & (~df["Cancellation Reason"].isin(excused_reasons))
        )
        # Calculate it
        df.loc[cancelled_not_excused, "Cancel Time Diff (hours)"] = (
                ((df["Requested Pickup Time"] - df["Cancellation Time"]).dt.total_seconds() / 3600).round(3)
            )
        self.logger.info("Added and calculated 'Cancel Time Diff (hours)'")

        # Add Y/N column for Late Cancelations
        df["Late Cancellation"] = "N"
        df["Late Cancellation"] = df["Late Cancellation"].astype(types.pyarrow_string)
        self.logger.info("Added column: 'Late Cancellation'")
        # Late Paratransit Cancellations
        df.loc[
            (df["Service"] == "Paratransit") & (df["Cancel Time Diff (hours)"] < LATE_PARA_CANCELLATION_HRS),
            "Late Cancellation"] = "Y"
        self.logger.info("Calculated 'Late Cancellation' values for 'Paratransit' service")
        # Late Microtransit Cancellations (cancelled after 4:30 PM the day before)
        df.loc[
            (
                (df["Service"] == "Microtransit")
                & (df["Cancellation Time"] >= ((df["Original Planned Pickup Time"] - dt.timedelta(days=1)).dt.normalize() + dt.timedelta(hours=16, minutes=30)))
            ),
            "Late Cancellation"] = "Y"
        self.logger.info("Calculated 'Late Cancellation' values for 'Microtransit' service")

        # Return final data
        self.data = df
        return
