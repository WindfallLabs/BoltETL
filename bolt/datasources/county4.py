"""Missoula County Cadastral data from Montana State Library."""
import geopandas as gpd
import pandas as pd
from pathlib import Path
from sqlalchemy import Engine, create_engine

from bolt.utils import config, types
from . import Datasource


TBL_SQL = """
    SELECT TABLE_NAME
    FROM County4.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
"""

IGNORE_TBLS =  {
    'geometry_columns',
    'parcels',
    'spatial_ref_sys'
}


class County4(Datasource):
    def __init__(self):
        self.init()
        self.engine: Engine = create_engine(self.metadata["URI"])
        self.data: dict[str, pd.DataFrame]

    @property
    def cache_path(self) -> Path:  # overwritten
        return config.cache_dir.joinpath(self.metadata["name"])

    @property
    def tables(self) -> set[str]:
        return set(pd.read_sql(TBL_SQL, self.engine)["TABLE_NAME"].tolist()).difference(IGNORE_TBLS)

    def extract(self) -> None:  # overwritten
        """Read all SQL tables (names and data) to list."""
        self.raw = [
            (tbl_name, pd.read_sql(f"SELECT * FROM {tbl_name}", self.engine))
            for tbl_name in self.tables
        ]
        return

    def transform(self) -> None:
        """Process multiple tables into name-data dict."""
        data = {k: v.copy() for k, v in self.raw}

        for tbl_name, df in data.items():
            # Filter by max TaxYear if exists
            if "TaxYear" in df.columns:
                max_tax_year = df["TaxYear"].max()
                df = df[df["TaxYear"] == max_tax_year]

            df.reset_index(inplace=True)
            data[tbl_name] = df

        self.data = data
        return

    def read_cache(self) -> None:  # overwritten
        """Read cached data from multiple files."""
        self.data = {}
        for file in self.cache_path.rglob("*.feather"):
            self.data[file.name.split(".")[0]] = pd.read_feather(file, dtype_backend="pyarrow")
        return

    def write_cache(self, *args, **kwargs) -> None:  # overwritten
        """Cache data to multiple files."""
        for filename, data in self.data.items():
            data.to_feather(self.cache_path.joinpath(f"{filename}.feather"), *args, **kwargs)
        return
