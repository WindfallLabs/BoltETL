"""Datasource ABC."""
import datetime as dt
from abc import ABC, abstractmethod
from getpass import getuser
from pathlib import Path
from platform import node
from typing import Annotated
from typing_extensions import Doc

import pandas as pd
import geopandas as gpd
from pydantic import BaseModel
from sqlalchemy import Engine

from bolt.utils import config, version


SUPPORTED_CACHE_TYPES = ("DISABLE", "feather")

READERS = {
    "xlsx": pd.read_excel,
    "csv": pd.read_csv,
    # TODO: more filetype readers?
}


class CacheMetaData(BaseModel):
    filename: str
    export_date: str
    processed_by: str
    #log_path: str
    #log_md5: str
    datasource_version: str
    #datasource_md5: str
    sources: list[str]


class Datasource[T](ABC):
    """Abstract class defining Datasource classes."""
    def __init__(self):
        self.metadata: Annotated[
            dict,  # TODO: dict template / typing?
            Doc("Data definition / metadata loaded from 'config.toml'")
        ]

        self.raw: Annotated[
            list[tuple[str, pd.DataFrame|gpd.GeoDataFrame]] | None,
            Doc("DataFrame of GeoDataFrame of raw data (created by `extract`)")
        ] = None

        self.data: Annotated[
            pd.DataFrame|gpd.GeoDataFrame|None,
            Doc("DataFrame or GeoDataFrame of processed data (created by `transform` or loaded from cache with `read_cache`)")
        ] = None

    def init(self) -> None:
        """Loads metadata from config (toml) file."""
        try:
            # TODO: consider pydantic for validation and type casting
            self.metadata = {
                k: Path(v) if "_dir" in k or "_path" in k else v
                for k, v in config.metadata[self.__class__.__name__].items()
            }
        except KeyError:
            raise KeyError(f"Name mismatch: '{self.name}' not in config.toml")
        return

    @property
    def name(self):
        """Shortcut for class name."""
        return self.__class__.__name__

    @property
    def version(self):
        return version.from_file_mdate(self.cache_path)

    @property
    def source_files(self) -> list[str]:
        return [
            str(p.absolute()) for p
            in Path(self.metadata["source_dir"]).rglob(self.metadata["filename"])
            # Ignore source / raw files that start with "_"
            if not p.name.startswith("_")
        ]

    @property
    def cache_path(self) -> Path:
        """Return the path (string) to cached data file."""
        cache_filetype = self.metadata.get("cache_filetype", "feather")  # TODO: DOCUMENT that caching to .feather is default
        if cache_filetype not in SUPPORTED_CACHE_TYPES:
            raise TypeError(f"'{cache_filetype}' file type not (yet) supported")
        if cache_filetype == "DISABLE":
            raise IOError(f"Caching is disabled for '{self.name}'")
        # Default
        return config.cache_dir.joinpath(f'{self.metadata["name"]}.{cache_filetype}')

    def extract(self):
        """Open the raw data source file(s). Can be over-written to customize."""
        ext = self.metadata["filename"].split(".")[-1].lower()
        if self.metadata.get("load_with_geopandas", False):
            # TODO: would we ever read multiple?
            self.raw = gpd.read_file(self.source_files[0], layer=self.metadata.get("layer", 0))
        else:
            read_func = READERS[ext]
            self.raw = [(p, read_func(p, dtype_backend="pyarrow")) for p in self.source_files]
        return

    @abstractmethod
    def transform(self) -> None:
        """Defines how raw data is transformed (processed/cleaned)."""
        # df = ...
        # self.data = df
        return

    def load(self, dst: Engine, *args, **kwargs):  # TODO: what is this good for?
        """Defines how the the processed data is loaded into a target database."""
        self.data.to_sql(self.name, dst, *args, **kwargs)
        return

    def write_cache(self, *args, **kwargs) -> None:  # TODO: support for filetypes other than feather?
        """How to cache processed data."""
        self.data.to_feather(self.cache_path, *args, **kwargs)

        # TODO: dump cache metadata
        #self.cache_metadata().dump()...
        return

    def cache_metadata(self):
        cmeta = CacheMetaData(
            filename="{self.name}.feather",
            export_date=dt.datetime.now().strftime("%Y-%m-%d"),
            processed_by=f"{getuser()}/{node()}",
            #log_path=self.log.path,
            #log_md5=hashfile(self.log.path),
            datasource_version=str(self.version),  # TODO: wip
            #datasource_md5=hashfile(Path(__file__).absolute()),
            sources=sorted(self.source_files, reverse=True)
        )
        return cmeta

    def read_cache(self, *args, **kwargs) -> None:  # TODO: support for filetypes other than feather?
        """Loads data attribute from cache file."""
        if self.metadata.get("load_with_geopandas", False):
            self.data = gpd.read_feather(self.cache_path, *args, **kwargs)  # TODO: convert columns to pyarrow types?
        else:
            self.data = pd.read_feather(self.cache_path, *args, **kwargs, dtype_backend="pyarrow")
        
        # Load cache metadata
        # TODO: json.load(.../cached/.metadata/{filename}) -> bolt.config.metadata[filename]
        return

    def validate(self):
        """Describe the rules that the data must adhere to before exported via `load`."""
        pass

    def update(self):
        """Convenience method to combine Extract, Transform, and Cache methods."""
        if hasattr(self, "download"):
            self.download(self.metadata["download_path"])  # TODO: WIP
        self.extract()
        self.transform()
        self.write_cache()
        return
