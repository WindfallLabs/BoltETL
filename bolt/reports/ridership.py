"""NTD Monthly Report."""

from pathlib import Path
from typing import Literal

import pandas as pd
from sqlalchemy import text

from bolt.datasources import warehouse


class MonthlyRidership:  # TODO: Report obj
    def __init__(self):
        self.data: pd.DataFrame | None = None
        self.out_path: Path = Path(r"C:\Workspace\tmpdb\Reports\ParaNoShows")

    def run(self, ymth: int, mode: Literal["MB", "DR"]):
        """Execute the NTD Monthly report.

        Args:
            ymth: (int) The target year-month to get data for
            mode: (Literal["DR", "MB"]) The transit mode to get data for

        Example Execution:
            `python bolt-cmd.py report MonthlyRidership 202501`
        """
        with open("./bolt/reports/sql/ridership_monthly.sql") as f:
            sql = f.read()
        q = (
            text(sql)
            .bindparams(ymth=ymth, mode=mode)
            .compile(compile_kwargs={"literal_binds": True})
        )
        with warehouse.connect() as db:
            df = db.sql(str(q)).df().convert_dtypes(dtype_backend="pyarrow")

        self.export(df)
        return


class QuarterlyRidership:  # TODO: Report obj
    def __init__(self):
        self.data: pd.DataFrame | None = None
        self.out_path: Path = Path(r"C:\Workspace\tmpdb\Reports\ParaNoShows")

    def run(self, ymth: int, mode: Literal["MB", "DR"]):
        """Execute the NTD Monthly report.

        Args:
            ymth: (int) The target year-month to get data for
            mode: (Literal["DR", "MB"]) The transit mode to get data for

        Example Execution:
            `python bolt-cmd.py report NTDMonthly 202501 MB`
        """
        with open("./bolt/reports/sql/ridership_quarterly.sql") as f:
            sql = f.read()
        q = (
            text(sql)
            .bindparams(ymth=ymth, mode=mode)
            .compile(compile_kwargs={"literal_binds": True})
        )
        with warehouse.connect() as db:
            df = db.sql(str(q)).df().convert_dtypes(dtype_backend="pyarrow")

        self.export(df)
        return
