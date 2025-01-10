"""Missoula County Parcel data from Montana State Library."""
import geopandas as gpd

from bolt.utils import CRS, download, types
from . import Datasource


class Parcels(Datasource):
    def __init__(self):
        self.init()

    def download(self):
        """Download and overwrite existing Missoula County parcel data."""
        #target_file = self.metadata["source_dir"].joinpath(self.metadata["filename"])
        download(self.metadata["source_url"], self.metadata["source_dir"])
        return

    def transform(self):
        """Process parcel data."""
        drop_columns = [
            "COUNTYCD",
            "CountyName",
            "CountyAbbr",
            #
            "Shape_Length",
            "Shape_Area",
        ]

        # Copy and reproject raw data
        df = self.raw.copy().to_crs(CRS)
        # Drop useless columns
        df.drop(drop_columns, axis=1, inplace=True)

        # Convert object columns to PyArrow strings
        str_cols = [
            col for col in df.columns
            if col != "geometry" and df[col].dtype.name == "object"
        ]
        df = types.cast_many(df, str_cols, types.pyarrow_string)

        # Set TaxYear to PyArrow uint
        df["TaxYear"] = df["TaxYear"].astype(types.pyarrow_uint16)

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
