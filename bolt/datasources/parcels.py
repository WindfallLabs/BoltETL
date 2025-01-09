"""Missoula County Parcel data from Montana State Library."""
import geopandas as gpd

from bolt.utils import CRS, cast_many, pyarrow_string, pyarrow_uint16
from . import Datasource


class Parcels(Datasource):
    def __init__(self):
        self.init()

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

        # Copy and reproject raw data
        df = self.raw.copy().to_crs(CRS)
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
