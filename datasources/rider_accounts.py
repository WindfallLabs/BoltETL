"""RiderAccount report from Via"""
import pandas as pd
from rich.console import Console

from datasources import Datasource


console = Console()


class RiderAccounts(Datasource):
    def __init__(self):
        self.table_name = "RiderAccounts"
        self.raw_path = r"raw\Via - Rider Account\Rider Account.xlsx"
        self.cache_path = "RiderAccount.feather"
        self.raw: pd.DataFrame|None = None
        self.data: pd.DataFrame|None = None

    def extract(self):
        """Open each csv file into a tuple of filepath-dataframe pairs."""
        with console.status("RiderAccount: Extracting..."):
            # TODO: warn about HIPPA
            self.raw = pd.read_excel(self.raw_path, dtype_backend="pyarrow")
        return

    def transform(self):
        """."""
        with console.status("RiderAccount: Transforming..."):
            acc = self.raw.copy()
            acc["Birth Date"] = pd.to_datetime(acc["Birth Date"], errors="coerce")
            acc["Account Creation Date"] = pd.to_datetime(acc["Account Creation Date"], errors="coerce")
            # New Fields
            acc["Since"] = acc["Account Creation Date"].apply(lambda x: int(f"{x.year}{str(x.month).zfill(2)}"))
            self.data = acc
        return

    def validate(self):
        return  # TODO:

    def load(self, database):
        self.df.to_sql(self.name, database.engine)
        return
