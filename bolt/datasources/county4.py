"""Missoula County Cadastral data from Montana State Library."""
import numpy as np
import pandas as pd
from pathlib import Path
from sqlalchemy import Engine, create_engine

from bolt.utils import config
from . import Datasource


TBL_SQL = """
    SELECT TABLE_NAME
    FROM County4.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
"""

IGNORE_TBLS =  {
    "geometry_columns",
    "parcels",
    "spatial_ref_sys"
}


class County4(Datasource):
    def __init__(self):
        super().__init__()
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
        self.logger.info("Extracted raw data")
        return

    def transform(self) -> None:
        """Process multiple tables into name-data dict."""
        data = {k: v.copy() for k, v in self.raw}

        for tbl_name, df in data.items():
            # Filter by max TaxYear if exists
            if "TaxYear" in df.columns:
                max_tax_year = df["TaxYear"].max()
                if max_tax_year is np.nan:
                    self.logger.warning(f"'{tbl_name}' has 'TaxYear' = np.nan")
                    continue
                df = df[df["TaxYear"] == max_tax_year]
                self.logger.info(f"Filtered '{tbl_name}' by 'TaxYear' = {max_tax_year}")

            df.reset_index(inplace=True)
            data[tbl_name] = df

        self.data = data
        return

    def read_cache(self) -> None:  # overwritten
        """Read cached data from multiple files."""
        self.data = {}
        for file in self.cache_path.rglob("*.feather"):
            self.data[file.name.split(".")[0]] = pd.read_feather(file, dtype_backend="pyarrow")
            self.logger.info(f"Read cached file: {file}")
        return

    def write_cache(self, *args, **kwargs) -> None:  # overwritten
        """Cache data to multiple files."""
        for filename, data in self.data.items():
            data.to_feather(self.cache_path.joinpath(f"{filename}.feather"), *args, **kwargs)
            self.logger.info(f"Cache file written: {filename}")
        return
