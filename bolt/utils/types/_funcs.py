"""Type casting functions."""
import geopandas as gpd
import pandas as pd


def cast_many(df: pd.DataFrame|gpd.GeoDataFrame, columns: list[str], dtype: type|pd.ArrowDtype) -> pd.DataFrame|gpd.GeoDataFrame:
    """Cast many columns to a specified PyArrow DType."""
    df = df.copy()
    for col in columns:
        df[col] = df[col].astype(dtype)
    return df


# WIP
'''
def cast_ints(df: pd.DataFrame|gpd.GeoDataFrame) -> pd.DataFrame|gpd.GeoDataFrame:
    """Casts """
    df = df.copy()
    for col in df.columns:
        dtype_name = df[col].dtype.name
        vals = df[col].dropna()
        
        try:
            # Ints
            if all(df[col].dropna().apply(lambda x: float(x).is_integer())) is True:
                if vals.min() >= 0:
                    dtype_name = "uint"
                else:
                    dtype_name = "int"
            else:
                dtype = pd.ArrowDtype(pa.float32())
        
            df[col] = df[col].astype(dtype)

        except Exception:
            continue
'''

def to_int(s):
    if isinstance(s, str):
        return int(s.replace(",", ""))
    return s


def to_float(s):
    if isinstance(s, str):
        return float(s.replace(",", ""))
    return s
