from pathlib import Path
from typing import Literal

import pandas as pd  # type: ignore[import-untyped]
import polars as pl


def dict_to_sheets(
    data: dict[str, pd.DataFrame | pl.DataFrame],
    out_path: Path,
    mode: Literal["a", "w"] = "w",  # NOTE: "a" mode is untested
) -> None:
    """Write Excel sheets using a dict of DataFrames.

    NOTE: It is assumed that keys starting with '_' should be ignored.

    Args:
        data (dict[str, pd.DataFrame]): Dict of {sheet_name: dataframes}
            (polars.DataFrames are converted to pandas.DataFrames)
        out_path (Path): Filepath to save output Excel file
        mode (str): Write mode for pd.ExcelWriter
    """
    if not isinstance(data, dict):
        raise AttributeError("`data` attr must be dict[str, pd.DataFrame]")

    with pd.ExcelWriter(out_path, mode=mode) as writer:
        for sheet_name, dataframe in data.items():
            if not isinstance(dataframe, pd.DataFrame):
                if isinstance(dataframe, pl.DataFrame):
                    dataframe = dataframe.to_pandas()
                else:
                    raise ValueError(f"Expected pd.DataFrame for '{sheet_name}', got {dataframe}")
            # Ignore sheets that start with "_"
            if sheet_name.startswith("_"):
                continue
            dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
    return
