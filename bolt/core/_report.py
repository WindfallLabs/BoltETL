"""Base Report Class."""

from functools import wraps
from logging import Logger
from typing import Any, Callable

from ..utils import IOLogger, make_logger
from ._options import Options


class Report[T]:
    registry: dict[str, T] = dict()
    failed_to_load: set[tuple[str, Exception]] = set()

    def __init__(
        self,
        name: str,
        # output_path: str|Path|None = None,
        options: Options | None = None,
    ):
        """."""
        self._name = name
        # self.output_path = output_path
        self.options = options if options else Options()

        # Data
        self._data: dict[str, Any] = dict()

        # Logging setup
        self.logger: Logger | IOLogger = make_logger(self._name, self.options.log_dir)

        # Processing attributes
        self.run: Callable | None = None

        self.warehouse = None

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
    # MISC

    def _set_warehouse(self, warehouse) -> T:
        """Sets the warehouse attribute."""
        self.warehouse = warehouse
        return self

    # ========================================================================
    # Wrapper methods
    def run_wrapper(self, run_func: Callable) -> Callable:
        """
        Decorator to register the class's `extract` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            source_files (list[Path]): A list of Path objects for each raw data file
            logger (logging.Logger|NullLogger): optionally log messages to file

        Returns:
            raw_data (Any): Extracted data (probably a DataFrame)
        """

        @wraps(run_func)
        def _run_wrapper(*args, **kwargs):
            run_func(self, *args, **kwargs)
            return

        self.run = _run_wrapper
        return

    # TODO: consider a @sheet for excel sheets

    def __repr__(self):
        return f"<Report(name='{self._name}')>"
