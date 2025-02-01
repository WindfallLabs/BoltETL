import pandas as pd
import polars as pl
import geopandas as gpd

DataFrame = pd.DataFrame | pl.DataFrame | gpd.GeoDataFrame
