"""Type casting functions."""

import geopandas as gpd
import pandas as pd


def cast_many(
    df: pd.DataFrame | gpd.GeoDataFrame, columns: list[str], dtype: type | pd.ArrowDtype
) -> pd.DataFrame | gpd.GeoDataFrame:
    """Cast many columns to a specified PyArrow DType."""
    df = df.copy()
    for col in columns:
        df[col] = df[col].astype(dtype)
    return df


def clean_strings(s: pd.Series, remove_decimals=True) -> pd.Series:
    """Launders a series of any values to strings."""
    col: pd.Series = s.copy()
    col = col.astype(str)
    col = col.str.strip()
    if remove_decimals:
        col = col.apply(lambda x: x.split(".")[0] if "." in x else x)
    col = col.replace("nan", None)
    col = col.replace("", None)
    return col


def cast_df_to_pyarrow(
    df, schema: dict[pd.ArrowDtype, list[str]], create_missing=False
):
    """Casts columns of a Dataframe to PyArrow types given a schema."""
    df = df.copy()
    ext = tuple(
        {value: key for key, values in schema.items() for value in values}.items()
    )

    for col_name, arrow_type in ext:
        if col_name not in df.columns and create_missing:
            df[col_name] = pd.Series([None] * len(df), dtype=arrow_type)
        else:
            is_str = df[col_name].dtype.name.startswith("string")
            tname = arrow_type.name
            if is_str and tname.startswith("float"):
                df[col_name] = df[col_name].apply(to_float)
            df[col_name] = df[col_name].astype(arrow_type)

    return df


def to_int(s: str) -> int:
    if isinstance(s, str):
        return int(s.replace(",", ""))
    return s


def to_float(s: str) -> float:
    if isinstance(s, str):
        return float(s.replace(",", ""))
    return s
