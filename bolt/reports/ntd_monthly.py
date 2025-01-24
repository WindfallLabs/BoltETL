"""NTD Monthly Report."""

from pathlib import Path
from typing import Literal

from sqlalchemy import text

from bolt.datasources import warehouse
from bolt.reports import BaseReport


class NTDMonthly(BaseReport):  # TODO: Report obj
    def __init__(self):
        # self.data: pd.DataFrame|None = None
        super().__init__()

    def run(self, ymth: str, mode: Literal["MB", "DR"]):
        """Execute the NTD Monthly report.

        Args:
            ymth: (int) The target year-month to get data for
            mode: (Literal["DR", "MB"]) The transit mode to get data for

        Example Execution:
            `python bolt-cmd.py report NTDMonthly 202501 --mode MB`
        """
        self.out_path: Path = Path(
            rf"C:\Workspace\tmpdb\Reports\{ymth}-NTDMonthly.xlsx"
        )

        with open("./bolt/reports/sql/ntd_monthly.sql") as f:
            sql = f.read()
        q = (
            text(sql)
            .bindparams(ymth=ymth, mode=mode)
            .compile(compile_kwargs={"literal_binds": True})
        )
        with warehouse.connect() as db:
            print(str(q))
            self.data[mode] = (
                db.sql(str(q)).df().convert_dtypes(dtype_backend="pyarrow")
            )

        self.export(append_sheets=True)
        return
