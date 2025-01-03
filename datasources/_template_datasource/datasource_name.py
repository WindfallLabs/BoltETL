"""DriverShifts report from Via"""
import pandas as pd

from datasources import Datasource


class DriverShifts(Datasource):
    def __init__(self):
        self.table_name = ""
        self.raw_path = r"C:\Workspace\tmpdb\Data\raw\XXX"
        self.cache_path = r"C:\Workspace\tmpdb\Data\tmp\processed_data\XXX.feather"
        # Define datatype for raw data
        self.data: pd.DataFrame|None = None

    def extract(self):
        """Open each csv file into a tuple of filepath-dataframe pairs."""
        self.raw = [(p, pd.read_excel(p, dtype_backend="pyarrow")) for p in self.files]
        return

    def transform(self):
        """Process each raw dataframe and concat."""
        dfs = []
        for fpath, df in self.raw:
            dfs.append(self.process_one(fpath, df))
        self.data = pd.concat(dfs)
        return

    def validate(self):
        return  # TODO:

    def load(self, database):
        self.df.to_sql(self.name, database.engine)
        return

    def process_one(self, filepath: str, df: pd.DataFrame) -> pd.DataFrame:
        ...
