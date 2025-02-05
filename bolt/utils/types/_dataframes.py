import geopandas as gpd
import pandas as pd
import polars as pl

DataFrame = pd.DataFrame | pl.DataFrame | gpd.GeoDataFrame
