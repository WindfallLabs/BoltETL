"""Base Report Class."""
import logging
from abc import ABC, abstractmethod

import pandas as pd

from bolt.utils import config, make_logger


class BaseReport(ABC):
    @abstractmethod
    def __init__(self):
        self.logger = make_logger(
            self.__class__.__name__,
            config.log_dir,
        )

    def export(self) -> None:
        """Export the dataframe(s) in `self.data`."""
        if getattr(self, "data", None) is None:
            raise AttributeError(f"'{self.__class__.__name}' has no 'data' attribute")

        # Export dict[str, pd.DataFrame] to Excel sheets
        if isinstance(self.data, dict):
            with pd.ExcelWriter(self.out_path) as writer:
                for sheet_name, dataframe in self.data.items():
                    if not isinstance(dataframe, pd.DataFrame):
                        raise ValueError(
                            f"Expected pd.DataFrame for '{sheet_name}', got {dataframe}")
                    # Ignore sheets that start with "_"
                    if sheet_name.startswith("_"):
                        continue
                    dataframe.to_excel(
                        writer,
                        sheet_name=sheet_name,
                        index=False
                        )
            return

        # Export dataframe to single sheet
        self.data.to_excel(
            self.out_path,
            index=False
        )
        return

    @abstractmethod
    def run(self):
        ...
