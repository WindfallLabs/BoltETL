"""Datasource ABC."""
import re
from abc import ABC, abstractmethod
from enum import Enum
from glob import glob
from pathlib import Path
from typing import Annotated, Any
from typing_extensions import Doc

import pandas as pd
import geopandas as gpd
from sqlalchemy import Engine

from bolt.utils import config, version


class SourceType(Enum):
    FILE = "file"
    FILES = "files"
    URL = "url"


class YearMonth:  # TODO: move to utils
    def __init__(self, yearmonth: str|int):
        self.yearmonth = int(yearmonth)
        self.year = int(str(yearmonth)[:4])
        self.month = int(str(yearmonth)[4:])

    @classmethod
    def from_filepath(cls, filepath: str|Path):
        ymth = re.findall(r"^\d{6}", Path(filepath).name)[0]
        return YearMonth(ymth)

    def __repr__(self):
        return "<YearMonth: {self.yearmonth}>"

    def __str__(self):
        return str(self.yearmonth)


class Datasource[T](ABC):
    """Abstract class defining Datasource classes.
    """
    # A single-point to change these base directories
    #DATA_DIR = r"C:\Workspace\tmpdb\Data"
    #CACHE_DIR = rf"{DATA_DIR}\cached"

    def __init__(self):
        self.table_name: Annotated[
            str,
            Doc("The name of the output database table")
            ] = ""

        self._raw_path: Annotated[
            str|None,
            Doc("Contains the full path to the raw datasource")
         ] = None

        self._cache_file: Annotated[
            str|None,
            Doc("Contains the filename of the cached data")
        ] = None

        self.raw: Annotated[
            Any,
            Doc("DataFrame of raw data (created by `extract`)")
        ] = None

        self.data: Annotated[
            pd.DataFrame|gpd.GeoDataFrame|None,
            Doc("DataFrame of processed data (created by `transform` or loaded from file with `load`)")
        ] = None

        # Check for required attributes
        _req_attrs = ["table_name", "raw_path"]
        for _attr in _req_attrs:
            if not getattr(self, _attr):
                raise AttributeError(f"`{self.__class__.__name__}.{_attr}` not defined")

    def load_config_data(self) -> None:
        """Loads metadata from config (toml) file."""
        self._cfg = config.data[self.__class__.__name__]
        return

    @property
    def name(self):
        """Shortcut for class name."""
        return self.__class__.__name__

    @property
    def version(self):
        return version.from_file_mdate(self.cache_path)

    @property
    def raw_path(self) -> str:
        """Return the path (string) to raw datasource."""
        return self._raw_path
    
    #@raw_path.setter
    #def raw_path(self, filepath: str):
    #    """Set the path (string) to raw datasource."""
    #    if self.DATA_DIR not in filepath:
    #        self._raw_path = str(Path(self.DATA_DIR) / filepath)
    #    else:
    #        self._raw_path = filepath

    @property
    def cache_path(self) -> str:
        """Return the path (string) to cached data."""
        #return str(Path(self.CACHE_DIR).joinpath(self._cache_file))
        if cache.data[self.name].get("use_cache", True) is False:
            raise IOError(f"Cache not enabled for {self.name}")
        return str(config.cache_dir.joinpath(self.filename))
    
    #@cache_path.setter
    #def cache_path(self, filename: str):
    #    """Set the path (string) to cached data."""
    #    self._cache_file = filename

    @property
    def source_type(self) -> SourceType:
        if self.raw_path.startswith("http"):
            return SourceType.URL
        if isinstance(self.raw_path, Path) and self.raw_path.exists():
            return SourceType.FILE
        if "*" in self.raw_path:
            return SourceType.FILES
        return None

    @property
    def files(self):
        if self.source_type == SourceType.FILES:
            return glob(self.raw_path)
        raise NotImplementedError(f"{self.source_type} has no `.file` property")

    @abstractmethod
    def extract(self) -> None:
        """Defines how the source/raw data is read into `self.raw` DataFrame."""
        self.raw = ...
        return

    @abstractmethod
    def transform(self) -> None:
        """Defines how raw data is transformed (processed/cleaned)."""
        # df = ...
        # self.data = df
        return

    @abstractmethod
    def load(self, dst: Engine, *args, **kwargs):
        """Define how the the processed data is loaded into a target database."""
        self.data.to_sql(self.table_name, dst, *args, **kwargs)
        return

    def write_cache(self, *args, **kwargs) -> None:
        """Optionally define how to cache processed data."""
        self.data.to_feather(self.cache_path, *args, **kwargs)
        return

    def read_cache(self, *args, **kwargs) -> T:
        """Optionally define how to load cached data into `self.data` DataFrame."""
        self.data = pd.read_feather(self.cache_path, *args, **kwargs, dtype_backend="pyarrow")
        return self

    def validate(self):
        """Describe the rules that the data must adhere to before exported via `load`."""
        pass

    def update(self):
        """Convenience method to combine Extract, Transform, and Cache methods."""
        self.extract()
        self.transform()
        self.write_cache()
        return


    # ========================================================================
    # Helper functions
    # ========================================================================

    #@staticmethod
    #def get_yearmonth(filepath: str) -> int:
    #    """Parse a filename for a YYYYMM 'yearmonth' (e.g. 202412).
    #    
    #    NOTE: A yearmonth should be the first 6 digits of all filenames.
    #    """
    #    #return int(re.findall(r"^\d{6}", filepath.split("\\")[-1])[0])
    #    return YearMonth.from_filepath(filepath)

    @staticmethod
    def get_number_of_days(yearmonth, day) -> int:
        """Counts the number of the given day for the given yearmonth."""
        ...



'''
db = DevDB()
db.add_report(ElectrificationReport)
db.add_datasource(drivershifts)
db.load_all()


class FutureResult():
    def __init__(self, table_name):
        pass


derived_data = FutureResult("future_table")
'''
