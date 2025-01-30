import datetime as dt
from pathlib import Path

import pandas as pd

from bolt.datasources import RiderAccounts, RideRequests  # Report
from bolt.reports import BaseReport

# Minutes not considered late
GRACE_ARRIVAL_MINS = 4


class ParatransitNoShows(BaseReport):
    def __init__(self):
        super().__init__()
        # Data (enforce sheet order here)
        self.data: dict[str, pd.DataFrame | None] = {
            "Summary": None,
            "All Penalties": None,
        }
        self.out_path: Path = Path(r"C:\Workspace\tmpdb\Reports\ParaNoShows")
        self.logger.debug("Initialized")

    def process(self, start: str, end: str):  # NOTE: these are passed from CLI
        """Main report logic."""
        # Clean Args: Remove dashes, if any, and parse to datetime
        start = dt.datetime.strptime(start.replace("-", ""), "%Y%m%d")
        end = dt.datetime.strptime(end.replace("-", ""), "%Y%m%d")
        self.logger.info(f"Processing data from {start} to {end}")

        # Read Ride Requests
        # Changed as of 2025-01-24: Most of the field-creation and calcs were
        #  moved to the RideRequests.transform() method
        req = RideRequests().read_cache()
        req_df = req.data.copy()
        self.logger.info(f"Loaded RideRequests ({req.version})")
        req_df = req_df[
            # Filter by date range
            (req_df["Requested Pickup Time"] >= start)
            & (req_df["Requested Pickup Time"] <= end)
        ].reset_index(drop=True)
        self.logger.info("Filtered RideRequests to date range and Late/No-Show")

        # Update 'Cancel' values to 'Late Cancel' in "Request Status"
        req_df.loc[req_df["Late Cancellation"] == "Y", "Request Status"] = "Late Cancel"
        self.logger.info("Updated 'Request Status' values of 'Cancel' to 'Late Cancel'")

        # Add Penalty Points
        req_df["Penalty Points"] = 0
        self.logger.info("Added 'Penalty Points' column")
        req_df.loc[
            # For any Late Cancellation or No Show record...
            (
                (req_df["Late Cancellation"] == "Y")
                | (req_df["Request Status"] == "No Show")
            ),
            # ...set the Penalty Points to 1
            "Penalty Points",
        ] = 1

        # Read Rider Accounts
        acc = RiderAccounts().read_cache()
        self.logger.info(f"Loaded RiderAccounts ({acc.version})")

        # Combine
        df = pd.merge(
            # Ride Requests
            req_df,
            # Rider Accounts
            acc.data,
            how="left",
            on="Rider ID",
        )

        # Aggregation (group-by or pivot table)
        agg = {
            "Last Name": "first",
            "First Name": "first",
            "Phone Number": "first",
            "Penalty Points": "sum",
            "Request ID": "count",
        }

        # Resulting table
        g = (
            df.groupby("Rider ID")
            .agg(agg)
            .reset_index()
            .rename({"Request ID": "Trips Booked"}, axis=1)
        )

        # TODO: split Micro- and Paratransit trip counts
        # Split micro- and para- trips
        service_pivot = pd.pivot_table(
            df.groupby(["Rider ID", "Service"])["Request ID"]
            .count()
            .reset_index()
            .rename({"Request ID": "Count"}, axis=1),
            values="Count",
            index="Rider ID",
            columns="Service",
            aggfunc="sum",
        ).reset_index()

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
            (g["Penalty Points"] >= 5)
            & (g["Trips Booked"] >= 10)
            & (g["%"] >= 0.1)  # We only need this field here (drop later)
        )

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
            "Requested Pickup Time",
            "Cancellation Time",
            "Cancellation Reason",
            "Penalty Points",
        ]

        # Proof (dataframe of all records that got penalty points)
        penalties_df = df[(df["Penalty Points"] > 0)][check_cols].copy()
        # The "proof" sheet is all rides (within date range) for a user with 1+ penalty points
        # This is the part Wyatt didn't care for
        #users_with_penalties = set(g[g["Penalty Points"] > 0]["Rider ID"])
        #penalties_df = df[df["Rider ID"].isin(users_with_penalties)][check_cols].copy()
        #penalties_df["Request ID"] = penalties_df["Request ID"].astype(str)
        penalties_df.sort_values(["Last Name", "Requested Pickup Time"], inplace=True)
        # Set to data (order set in __init__)
        self.data["All Penalties"] = penalties_df.copy()

        # Half vs Full Month reporting - get the most recent dropoff
        recent_pickup = df[df["Request Status"] == "Completed"][
            "Actual Pickup Time"
        ].max()

        # Set data attribute
        if end.day <= 15:
            mode = "HALF"
            # self.data = warning_df
            self.data["Summary"] = warning_df
        else:
            mode = "FULL"
            # self.data = suspension_df
            self.data["Summary"] = suspension_df

        # Derive filename from max pickup date
        filename = recent_pickup.strftime(f"%Y%m-No-Show - %b {mode}.xlsx")
        # Update the out-directory
        self.out_path = self.out_path.joinpath(filename)
        return

    def run(self, start: str, end: str):
        """Execute the Paratransit No-Show Report.

        Args:
            start: (string) min dt.date to include in processing
            end: (string) max dt.date to include in processing

        Example Execution:
            `python bolt-cmd.py report ParatransitNoShows --start 20250101 --end 20250115`
        """
        self.process(start, end)
        self.export()
        return
