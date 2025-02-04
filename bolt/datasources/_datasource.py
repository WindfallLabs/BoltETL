"""Datasource ABC."""

import datetime as dt
from abc import ABC, abstractmethod
from getpass import getuser
from pathlib import Path
from platform import node
from typing import Annotated

import geopandas as gpd
import pandas as pd
import polars as pl
from pydantic import BaseModel
from sqlalchemy import Engine
from typing_extensions import Doc

from bolt.utils import config, make_logger, version

SUPPORTED_CACHE_TYPES = ("DISABLE", "feather")

READERS = {
    #"xlsx": pd.read_excel,
    #"csv": pd.read_csv,
    # TODO: more filetype readers?
    "xlsx": pl.read_excel,
    "csv": pl.read_csv,
}

SCANNERS = {
    "csv": pl.scan_csv,
}


class CacheMetaData(BaseModel):
    filename: str
    export_date: str
    processed_by: str
    # log_path: str
    # log_md5: str
    datasource_version: str
    # datasource_md5: str
    sources: list[str]


class Datasource[T](ABC):
    """Abstract class defining Datasource classes."""

    def __init__(self):
        self.logger = make_logger(self.name, config.log_dir)
        self.lazy_load_raw = True

        self.metadata: Annotated[
            dict,  # TODO: dict template / typing?
            Doc("Data definition / metadata loaded from 'config.toml'"),
        ]

        self.raw: Annotated[
            list[tuple[str, pl.DataFrame | pd.DataFrame | gpd.GeoDataFrame]] | None,
            Doc("DataFrame of GeoDataFrame of raw data (created by `extract`)"),
        ] = None

        self.data: Annotated[
            pl.DataFrame | pd.DataFrame | gpd.GeoDataFrame | None,
            Doc(
                "DataFrame or GeoDataFrame of processed data (created by `transform` or loaded from cache with `read_cache`)"
            ),
        ] = None

        self.is_enriched: bool = False

        try:
            # TODO: consider pydantic for validation and type casting
            self.metadata = {
                k: Path(v) if "_dir" in k or "_path" in k else v
                for k, v in config.metadata[self.name].items()
            }
        except KeyError:
            raise KeyError(f"Name mismatch: '{self.name}' not in config.toml")
        # self.logger.debug(f"Initialized {self.name}")

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
            str(p.absolute())
            for p in Path(self.metadata["source_dir"]).rglob(self.metadata["filename"])
            # Ignore source / raw files that start with "_"
            if not p.name.startswith("_") and not p.name.startswith("~")
        ]

    @property
    def cache_path(self) -> Path:
        """Return the path (string) to cached data file."""
        cache_filetype = self.metadata.get(
            "cache_filetype", "feather"
        )  # TODO: DOCUMENT that caching to .feather is default
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
            layer = self.metadata.get("layer", 0)
            self.raw = gpd.read_file(self.source_files[0], layer=layer)
            self.logger.debug(f"Extracted raw data ({layer}; with geopandas)")
        else:
            read_func = READERS.get(ext, None)
            scan_func = SCANNERS.get(ext, None)
            #self.raw = [
            #    (p, read_func(p, dtype_backend="pyarrow")) for p in self.source_files
            #]
            if self.lazy_load_raw and scan_func:
                self.raw = [
                    (p, scan_func(p, ignore_errors=True)) for p in self.source_files
                ]
            else:
                self.raw = [
                    (p, read_func(p)) for p in self.source_files
                ]
            self.logger.debug("Extracted raw data")
        # TODO: self.logger.debug("Raw files loaded: 8/8")
        return

    @abstractmethod
    def transform(self) -> None:
        """Defines how raw data is transformed (processed/cleaned)."""
        # df = ...
        # self.data = df
        pass

    def load(self, dst: Engine, *args, **kwargs):  # TODO: what is this good for?
        """Defines how the the processed data is loaded into a target database."""
        self.data.to_sql(self.name, dst, *args, **kwargs)
        return

    def write_cache(self, *args, **kwargs) -> None:
        """How to cache processed data."""
        self.data.write_ipc(self.cache_path)
        self.logger.info(f"Wrote cache file: {self.cache_path}")
        metadata = self.cache_metadata()
        # self.logger.info(f"Metadata (processed_by): {metadata.processed_by}")
        # self.logger.info(f"Metadata (version): {metadata.datasource_version}")
        # self.logger.info(f"Metadata (sources): {metadata.sources}")
        # ....

        # TODO: dump cache metadata
        # self.cache_metadata().dump()...
        return

    def cache_metadata(self):
        cmeta = CacheMetaData(
            filename=f"{self.name}.feather",
            export_date=dt.datetime.now().strftime("%Y-%m-%d"),
            processed_by=f"{getuser()}/{node()}",
            # log_path=self.log.path,
            # log_md5=hashfile(self.log.path),
            datasource_version=str(self.version),  # TODO: wip
            # datasource_md5=hashfile(Path(__file__).absolute()),
            sources=sorted(self.source_files, reverse=True),
        )
        return cmeta

    def read_cache(self, *args, **kwargs) -> T:
        """Loads data attribute from cache file."""
        h = "HASH"  # TODO: file hash/metadata
        if self.metadata.get("load_with_geopandas", False):
            self.data = gpd.read_feather(  # TODO: test this
                self.cache_path, *args, **kwargs
            )
            # self.logger.info(f"Cached file read (with geopandas): {self.version} {h}")
        else:
            self.data = pl.read_ipc(
                self.cache_path, *args, **kwargs
            )
            # self.logger.info(f"Cached file read: {self.version} {h}")  # TODO: file hash

        # Load cache metadata
        # TODO: json.load(.../cached/.metadata/{filename}) -> bolt.config.metadata[filename]
        return self

    def validate(self):
        """Describe the rules that the data must adhere to before exported via `load`."""
        pass

    def update(self):
        """Convenience method to combine Extract, Transform, Enrich, and Cache methods."""
        self.logger.info("Beginning full update process")
        if hasattr(self, "download"):
            self.logger.info("'download' method found and running")
            self.download()
        self.extract()
        self.transform()
        self.write_cache()
        self.logger.info("Update complete")
        return
