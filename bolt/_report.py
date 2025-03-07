"""Base Report Class."""

from functools import wraps
from logging import Logger
from typing import Any, Callable

from ._options import Options
from bolt.utils import make_logger, IOLogger


class Report[T]:
    registry: dict[str, T] = dict()

    def __init__(
        self,
        name: str,
        #output_path: str|Path|None = None,
        options: Options|None = None
    ):
        """."""
        self._name = name
        #self.output_path = output_path
        self.options = options if options else Options()

        # Data
        self._data: dict[str, Any] = dict()

        # Logging setup
        self.logger: Logger|IOLogger = make_logger(self._name, self.options.log_dir)

        # Processing attributes
        self.run: Callable|None = None

        # Register report
        if self.options.register:
            self.registry[self._name] = self

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    # ========================================================================
    # Wrapper methods
    def run_wrapper(self, func: Callable) -> Callable:
        """
        Decorator to register the class's `extract` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            source_files (list[Path]): A list of Path objects for each raw data file
            logger (logging.Logger|NullLogger): optionally log messages to file

        Returns:
            raw_data (Any): Extracted data (probably a DataFrame)
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            self._data.update(func(*args, **kwargs))
            return
        self.run = wrapper
        return wrapper

    # TODO: consider a @sheet for excel sheets

'''
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
'''