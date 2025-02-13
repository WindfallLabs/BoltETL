"""Base Report Class."""

from abc import ABC, abstractmethod

import pandas as pd

from bolt.utils import config, make_logger


class BaseReport(ABC):
    def __init__(self):
        self.name = self.__class__.__name__
        self.logger = make_logger(
            self.__class__.__name__,
            config.log_dir,
        )
        self.data: dict[str, pd.DataFrame | None] = {}
        self._exported = False

    def export(self, append_sheets: bool = False) -> None:
        """Exports the `self.data` attribute (dict[str, pd.DataFrame]) to
        sheets in an Excel file.

        Args:
            append_sheets (bool, default False): Adds new sheets to an existing
                output Excel file if True, or overwrites the file if False.
        """
        # Type check: `self.data` is not None
        if getattr(self, "data", None) is None:
            raise AttributeError(f"'{self.__class__.__name}' has no 'data' attribute")
        # Type check: `self.data` is a dict
        if not isinstance(self.data, dict):
            raise AttributeError("`self.data` attr must be dict[str, pd.DataFrame]")
        # Type check: `self.data` has all pd.DataFrame-type values
        if not all([isinstance(v, pd.DataFrame) for v in self.data.values()]):
            raise ValueError("All values of `self.data` must be pd.DataFrame")

        # Default mode is set to write
        xl_mode = "w"
        if self.out_path.exists() and append_sheets:
            # The file must exist in order to append
            xl_mode = "a"
        with pd.ExcelWriter(self.out_path, mode=xl_mode) as writer:
            for sheet_name, dataframe in self.data.items():
                if not isinstance(dataframe, pd.DataFrame):
                    raise ValueError(
                        f"Expected pd.DataFrame for '{sheet_name}', got {dataframe}"
                    )
                # Ignore sheets that start with "_"
                if sheet_name.startswith("_"):
                    continue
                dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
        self._exported = True
        return

    @abstractmethod
    def run(self): ...
