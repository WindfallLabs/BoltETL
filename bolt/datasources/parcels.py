"""Missoula County Parcel data from Montana State Library."""
import geopandas as gpd
from rich.console import Console

from bolt.utils import cast_many, pyarrow_string, pyarrow_uint16
from . import Datasource

console = Console()


class Parcels[T](Datasource):
    def __init__(self):
        self.table_name = "Parcels"
        #self.raw_path = r"raw\MSL - Missoula Parcels\Missoula_Parcels.gdb"
        self.cache_path = "Parcels.feather"
        self.raw: gpd.GeoDataFrame|None = None
        self.data: gpd.GeoDataFrame|None = None
        self.final_crs: str = "epsg:6515"

    def write_cache(self, *args, **kwargs) -> None:
        """Optionally define how to cache processed data."""
        # NOTE: GeoDataFrames have native `.to_feather` and `.read_feather`
        #  methods that `wkb.dumps`/`wkb.loads` 'geometry' columns
        self.data.to_feather(self.cache_path, *args, **kwargs)
        return

    def read_cache(self, *args, **kwargs) -> T:
        """Optionally define how to load cached data into `self.data` DataFrame."""
        self.data = gpd.read_feather(self.cache_path, *args, **kwargs)
        return self

    def extract(self):
        """Open the parcels (ESRI) geodatabase."""
        with console.status(f"{self.name}: Extracting..."):
            self.raw = gpd.read_file(self.raw_path, layer=0)
        return

    def transform(self):
        """."""
        drop_columns = [
            "COUNTYCD",
            "CountyName",
            "CountyAbbr",
            #
            "Shape_Length",
            "Shape_Area",
        ]
        with console.status(f"{self.name}: Transforming..."):
            # Copy and reproject raw data
            df = self.raw.copy().to_crs(self.final_crs)
            # Drop useless columns
            df.drop(drop_columns, axis=1, inplace=True)

            # Convert object columns to PyArrow strings
            str_cols = [
                col for col in df.columns
                if col != "geometry" and df[col].dtype.name == "object"
            ]
            df = cast_many(df, str_cols, pyarrow_string)

            # Set TaxYear to PyArrow uint
            df["TaxYear"] = df["TaxYear"].astype(pyarrow_uint16)

            # Remove non-ID'd parcels (e.g. rivers, lakes, etc.)
            df = df[~df["PARCELID"].isna()]

            # Fix Polygon errors (force all to MULTIPOLYGON after buffering all by 0 ft)
            df["geometry"] = df.geometry.apply(lambda x: gpd.tools.collect(x.buffer(0), True))
            # ...
            self.data = df
        return

    def validate(self):
        """Validation"""  # TODO: WIP
        assert set(self.data.geometry.geom_type) == {'MultiPolygon'}
        return

    def load(self, database):
        self.df.to_sql(self.name, database.engine)
        return
