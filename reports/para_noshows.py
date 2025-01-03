import datetime as dt
from pathlib import Path

import pandas as pd
from rich.console import Console

from datasources import RideRequests, RiderAccounts  # Report


console = Console()

# Minutes not considered late
GRACE_ARRIVAL_MINS = 4


def try_strftime(x: dt.datetime, fmt: str) -> str:
    try:
        return x.strftime(fmt)
    except Exception:
        return ""


#class NoShowReport(Report):
class NoShowReport():
    def __init__(self):
        self.acc: pd.DataFrame|None = None
        self.req: pd.DataFrame|None = None
        self.data: pd.DataFrame|None = None
        self.out_path: Path = Path(r"C:\Workspace\tmpdb\Reports\ParaNoShows")
        #self.start: dt.date
        #self.end: dt.date

    def load_data(self, refresh=False):  # TODO: True causes some recursion...
        req = RideRequests()
        acc = RiderAccounts()

        if refresh:
            raise NotImplementedError("'refresh' option is in development")
            #with console.status("NoShowReport: Refreshing data..."):
            req.update()
            acc.update()
        with console.status("NoShowReport: Loading data..."):
            # Requests
            self.req = req.read_cache().data
            # Accounts
            self.acc = acc.read_cache().data
        return

    #def process(self, start: dt.date, end: dt.date):
    def process(self, start: str, end: str):
        """Main report logic.
        
        Args:
            start: min dt.date to include in processing
            end: max dt.date to include in processing
        """
        # Remove dashes, if any, and parse to datetime
        start = dt.datetime.strptime(start.replace("-", ""), "%Y%m%d")
        end = dt.datetime.strptime(end.replace("-", ""), "%Y%m%d")

        # Merge Requests and Accounts
        df = pd.merge(self.req, self.acc, how="left", on="Rider ID")
        #df["YMTH"] = df["Requested Pickup Time"].apply(lambda x: try_strftime(x, "%Y%m"))
        # Limit to only those within the timeframe (yearmonth)
        #df = df.loc[df["YMTH"] == str(yearmonth)].reset_index(drop=True)

        df = df[
            (df["Requested Pickup Time"] >= start)
            & (df["Requested Pickup Time"] <= end)
            ].reset_index()
        # Assign Penalty Points
        df["Penalty Points"] = 0

        # No-show rule/query
        no_show = df["Request Status"] == "No Show"

        # Late Cancellation (cancelled <2 hours before ride shows up, unexcused)
        excused_reasons = [
            "appointment_change",
            "cancel_not_rider",
            "Cancel – rider not at fault",  # NOTE: dash char
            "other"
            ]

        # Late paratransit cancellation rule/query
        late_para_cancel = (
            # Status of "Cancel"
            (df["Request Status"] == "Cancel")\
            # Paratransit service
            & (df["Service"] == "Paratransit")\
            # Not an excused reason
            & (~df["Cancellation Reason"].isin(excused_reasons))\
            # If the cancellation was submitted after 2-hours in advance of scheduled pickup
            #& ((df["Original Planned Pickup Time"] - df["Cancellation Time"]).dt.total_seconds() / 3600 < 2)
            & ((df["Original Planned Pickup Time"].fillna(0) - df["Cancellation Time"].fillna(0)).dt.total_seconds() / 3600 < 2)
        )

        # Late microtransit cancellation rule/query
        late_micro_cancel = (
            # Status of "Cancel"
            (df["Request Status"] == "Cancel")\
            # Microtransit service
            & (df["Service"] == "Microtransit")\
            # Not an excused reason
            & (~df["Cancellation Reason"].isin(excused_reasons))\
            # If the cancellation was submitted after 4:30 PM the day before
            #& (df["Cancellation Time"] >= ((df["Original Planned Pickup Time"] - dt.timedelta(days=1)).dt.normalize() + dt.timedelta(hours=16, minutes=30)))
            & (df["Cancellation Time"].fillna(0) >= ((df["Original Planned Pickup Time"].fillna(0) - dt.timedelta(days=1)).dt.normalize() + dt.timedelta(hours=16, minutes=30)))
        )

        # Give no-shows or late-cancellations 1 Penalty Point
        df.loc[no_show | late_para_cancel | late_micro_cancel, "Penalty Points"] = 1

        # Backup "raw" data
        self.raw = df.copy()

        # Aggregation (group-by or pivot table)
        agg = {
            "Last Name": "first",
            "First Name": "first",
            "Phone Number": "first",
            "Penalty Points": "sum",
            "Request ID": "count"
        }

        # Resulting table
        g = df.groupby("Rider ID").agg(agg).reset_index().rename({"Request ID": "Trips Booked"}, axis=1)

        # TODO: split Micro- and Paratransit trip counts
        # Split micro- and para- trips
        service_pivot = pd.pivot_table(
            df.groupby(["Rider ID", "Service"])["Request ID"].count().reset_index().rename({"Request ID": "Count"}, axis=1),
            values='Count',
            index='Rider ID',
            columns='Service',
            aggfunc='sum').reset_index()

        g = pd.merge(g, service_pivot, on="Rider ID", how="left")
        g["Microtransit"] = g["Microtransit"].fillna(0).astype(int)
        g["Paratransit"] = g["Paratransit"].fillna(0).astype(int)

        # Add fields
        # Percentage of Penalty Points to booked trips
        g["%"] = g["Penalty Points"] / g["Trips Booked"]
        g[r"% Late Cancel or No-Show"] = g["%"].apply(lambda x: f"{x:.1%}")

        # Create Warning and Suspension fields (default to N)
        g["Warning"] = "N"
        g["Suspension"] = "N"

        # Calculate warnings (3 or more Penalty Points)
        g.loc[g["Penalty Points"] >= 3, "Warning"] = "Y"

        # Rules for suspension
        suspend = (
            (g["Penalty Points"] >= 5)\
            & (g["Trips Booked"] >= 10)\
            & (g["%"] >= 0.1)  # We only need this field here (drop later)
        )

        # Drop the % (float) field
        #g.drop("%", axis=1, inplace=True)  # moved

        # Calculate suspension
        g.loc[suspend, "Suspension"] = "Y"

        # Make their own dataframes
        warning_df = g[g["Warning"] == "Y"].drop("%", axis=1)
        suspension_df = g[g["Suspension"] == "Y"].drop("%", axis=1)

        # Create a dataframe of the penalties used
        check_cols = [
            "Rider ID",
            "Last Name",
            "First Name",
            "Request ID",
            "Service",
            "Request Status",
            "Original Planned Pickup Time",
            "Cancellation Time",
            "Cancellation Reason",
            "Penalty Points"
        ]

        # Proof (dataframe of all records that got penalty points)
        penalties_df = df[(df["Penalty Points"] > 0)][check_cols].copy()
        # Damnit Excel!
        penalties_df["Request ID"] = penalties_df["Request ID"].astype(str)
        self.penalties_df = penalties_df.copy()

        # Half vs Full Month reporting - get the most recent dropoff
        recent_pickup = self.req[self.req["Request Status"] == "Completed"]["Actual Pickup Time"].max()

        #if recent_pickup.day >= 16:
        if end.day <= 15:
            mode = "HALF"
            self.data = warning_df
        else:
            mode = "FULL"
            self.data = suspension_df

        # Derive filename from max pickup date
        filename = recent_pickup.strftime(f"%Y%m-No-Show - %b {mode}.xlsx")
        # Update the out-directory
        self.out_path = self.out_path.joinpath(filename)
        return

    def export(self):
        """."""
        if len(self.out_path.name.split(".")) != 2:
            raise IOError("Out Directory has no name - run the report first")
        with pd.ExcelWriter(self.out_path) as writer:
            self.data.to_excel(
                writer,
                sheet_name="Summary",
                index=False
                )
            self.penalties_df.to_excel(
                writer,
                sheet_name="All Penalties",
                index=False
                )
        return
