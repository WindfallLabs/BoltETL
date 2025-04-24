"""Datasource."""

from collections.abc import Callable
from enum import Enum
from functools import wraps
from logging import Logger
from pathlib import Path
from time import perf_counter_ns
from typing import Any, Literal, Optional, Self

from duckdb import DataError
from rich.console import Console

from .._config import Config
from ..utils import IOLogger, make_logger, time_diff
from ._metadata import Metadata
from ._options import Options

'''
class ValidationError(Exception):
    """Custom exception for validation failures."""
    def __init__(self, message: str, validation_name: str):
        self.message = message
        self.validation_name = validation_name
        super().__init__(self.message)
'''


class RawDataOrigin(Enum):
    """Describes the origin of the raw data."""

    INIT = "INIT"
    EXTRACTED = "EXTRACTED"
    FROM_CACHE = "FROM_CACHE"
    DIRECTLY_SET = "DIRECTLY_SET"


class ETLState(Enum):
    INIT = "INIT"
    DOWNLOADED = "DOWNLOADED"
    EXTRACTED = "EXTRACTED"
    TRANSFORMED = "TRANSFORMED"
    VALIDATED = "VALIDATED"
    CACHE_WRITTEN = "CACHE_WRITTEN"
    CACHE_READ = "CACHE_READ"
    LOADED = "LOADED"

    def __repr__(self):
        return f"<ETLState.{self.name}>"

    def __lt__(self, other):
        this = self._member_names_.index(self.name)
        oth = other._member_names_.index(other.name)
        return this < oth

    def __le__(self, other):
        this = self._member_names_.index(self.name)
        oth = other._member_names_.index(other.name)
        return this <= oth

    def __gt__(self, other):
        this = self._member_names_.index(self.name)
        oth = other._member_names_.index(other.name)
        return this > oth

    def __ge__(self, other):
        this = self._member_names_.index(self.name)
        oth = other._member_names_.index(other.name)
        return this >= oth


type State = Literal[*ETLState.__members__.keys()]  # type: ignore


class Datasource:
    """Defines a source of data."""

    registry: dict[str, Self] = dict()
    failed_to_load: set[tuple[str, Exception]] = set()

    def __init__(
        self: Self,
        name: str,
        source_dir: Path | str = "",
        source_filename: str = "",
        cache_path: Path | str | None = None,
        metadata: Metadata | None = None,
        options: Options | None = None,
    ):
        """
        Initialize a Datasource instance.

        Args:
            name (str): Unique name for the datasource
            source_dir (Path|str): Path to the directory containing the raw source data
            source_filename (str): filename or glob pattern of raw source filename(s)
            cache_path (Path|str): Optionally set a filepath to cache to
                (you must define function wrapped with `cache_write_wrapper`)
        """
        # Instance-specific attributes
        self._name: str = name
        self.source_dir: Path = Path(source_dir) if isinstance(source_dir, str) else source_dir
        self.source_filename: str = source_filename
        self.cache_path = Path(cache_path) if isinstance(cache_path, str) else cache_path

        # Handle metadata
        self.metadata: Metadata = metadata if metadata else Metadata()
        self.metadata.datasource = self
        # TODO: hash the stack of raw data here?

        # Handle options
        self.options: Options = options if options else Options()
        self.options.parent = self

        # Logging setup
        self.logger: Logger | IOLogger = make_logger(
            self._name, Config.log_dir, self.options.log_format
        )

        # Data
        self._raw_data: Optional[Any] = None
        self._data: Optional[Any] = None

        # State
        self.state = ETLState.INIT
        self.raw_data_origin = RawDataOrigin.INIT
        self._download_time: tuple[float, float] | None = None
        self._extract_time: tuple[float, float] | None = None
        self._transform_time: tuple[float, float] | None = None
        self._load_time: tuple[float, float] | None = None
        self._cache_write_time: tuple[float, float] | None = None
        self._cache_read_time: tuple[float, float] | None = None

        # Pipeline functions
        self.download: Optional[Callable] = None
        self._download_wrapper_used = False  # Download tool is skipped during update
        self.extract: Optional[Callable] = None
        self.transform: Optional[Callable] = None
        self.load = self._default_load  # TODO: good idea?
        self.validate: Optional[Callable] = None
        self.write_cache: Optional[Callable] = None
        self.read_cache: Optional[Callable] = None

        # Tools
        self.tool_names: list[str] = []

        # Register datasource
        if self.options.register:
            self.registry[self._name] = self

    # ========================================================================
    # Properties

    @property
    def name(self) -> str:
        """Getter for datasource name"""
        return self._name

    @property
    def source_files(self) -> list[Path]:
        if not self.source_dir and not self.source_filename:
            return []
        return [
            p.absolute()
            for p in Path(self.source_dir).rglob(self.source_filename)
            # Ignore source files that start with "_" or "~"
            if not p.name.startswith("_") and not p.name.startswith("~")
        ]

    @property
    def raw_data(self) -> Optional[Any]:
        """Raw/unprocessed data (extracted)."""
        return self._raw_data

    @property
    def data(self) -> Optional[Any]:
        """Processed data (transformed, set directly, or read from cache)."""
        # Call extract automatically when DIRECTLY_SET
        if (
            self.extract
            and self._data is None
            and self.raw_data_origin == RawDataOrigin.DIRECTLY_SET
        ):
            self.extract()
        return self._data

    @property
    def has_raw_data(self) -> bool:
        """Whether or not `self.data is not None`."""
        return self.raw_data is not None

    @property
    def has_data(self) -> bool:
        """Whether or not `self.data is not None`."""
        return self.data is not None

    @property
    def is_extracted(self) -> bool:
        """Whether or not the `extract` method was called successfully."""
        return self.state >= ETLState.EXTRACTED and (self.has_raw_data or self.has_data)

    @property
    def is_transformed(self) -> bool:
        """Whether or not the `transform` method was called successfully."""
        return self.state >= ETLState.TRANSFORMED

    @property
    def is_directly_set(self) -> bool:
        """Whether or not the data was set within a `data_wrapper`."""
        return self.raw_data_origin == RawDataOrigin.DIRECTLY_SET and self.has_data

    @property
    def is_loaded(self) -> bool:
        """Whether or not the `load` method was called successfully."""
        return self.state >= ETLState.LOADED

    @property
    def is_cached_data(self) -> bool:
        """Whether or not `self.data` was read from cache."""
        return self.raw_data_origin == RawDataOrigin.FROM_CACHE

    @property
    def cache_dir(self) -> Path | None:
        """Path to cache directory, if set in options."""
        if self.cache_path:
            return self.cache_path.parent
        return None

    @property
    def download_time(self) -> str:
        """The run time of the download function."""
        if self._download_time:
            return time_diff(*self._download_time)
        return "-1"

    @property
    def extract_time(self) -> str:
        """The run time of the extract function."""
        if self._extract_time:
            return time_diff(*self._extract_time)
        return "-1"

    @property
    def transform_time(self) -> str:
        """The run time of the transform function."""
        if self._transform_time:
            return time_diff(*self._transform_time)
        return "-1"

    @property
    def load_time(self) -> str:
        """The run time of the load function."""
        if self._load_time:
            return time_diff(*self._load_time)
        return "-1"

    @property
    def cache_write_time(self) -> str:
        """The write time of the write_cache function."""
        if self._cache_write_time:
            return time_diff(*self._cache_write_time)
        return "-1"

    @property
    def cache_read_time(self) -> str:
        """The read time of the read_cache function."""
        if self._cache_read_time:
            return time_diff(*self._cache_read_time)
        return "-1"

    # ========================================================================
    # Wrapper methods (in pipeline order)

    def download_wrapper(self, download_func: Callable) -> None:
        """
        Decorator to register the class's `download` method (optional).

        The function that this decorator wraps must have the following arguments:

        Args:
            obj (self): A reference to the object/self

        Returns:
            None
        """

        @wraps(download_func)
        def _download_wrapper(*args, **kwargs) -> None:
            _start = perf_counter_ns()
            download_func(self, *args, **kwargs)
            self.state = ETLState.DOWNLOADED
            self._download_time = (_start, perf_counter_ns())
            self.logger.info(f"Download completed (in {self.download_time})")
            return

        self.download = _download_wrapper
        self._download_wrapper_used = True
        self.tool_names.append("download")  # Yes, download is a tool
        return

    def extract_wrapper(self, extract_func: Callable) -> None:
        """
        Decorator to register the class's `extract` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            obj (self): A reference to the object/self

        Returns:
            raw_data (Any): Extracted data (probably a DataFrame)

        Example:
            ```python
            import polars as pl
            from boltetl import Datasource

            test_datasource = Datasource(name="TEST")

            @test_datasource.extract_wrapper
            def extract(obj, *args, **kwargs) -> pl.DataFrame:
                data = pl.DataFrame({"name": ["Sugar", "Spice"], "species": ["cat", "cat"]})
                return data
            ```
        """

        @wraps(extract_func)
        def _extract_wrapper(**kwargs) -> None:
            # TODO: `**kwargs` above might sallow passing flags from bolt_cli.py
            _start = perf_counter_ns()
            self.logger.info(
                f"Extracting data from {len(self.source_files)} file(s) in '{self.source_dir}'"
            )
            extracted_data = extract_func(self, **kwargs)
            self._raw_data = extracted_data
            self._extract_time = (_start, perf_counter_ns())
            self.state = ETLState.EXTRACTED
            self.logger.info(f"Extracted (in {self.extract_time})")
            self.logger.info(f"- type={type(self._raw_data)}")
            self.logger.info(f"- len={len(self._raw_data)}")
            return

        self.extract = _extract_wrapper
        self.raw_data_origin = RawDataOrigin.EXTRACTED
        return

    def transform_wrapper(self, transform_func: Callable) -> None:
        """
        Decorator to register the class's `transform` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            obj (self): A reference to the object/self

        Returns:
            data (Any): Processed data (probably a DataFrame)

        Example:
            ```python
            import polars as pl
            from boltetl import Datasource

            test_datasource = Datasource(name="TEST")

            @test_datasource.extract_wrapper
            def extract(obj, *args, **kwargs) -> pl.DataFrame:
                data = pl.DataFrame({"name": ["Sugar", "Spice"], "species": ["cat", "cat"]})
                return data

            @test_datasource.transform_wrapper
            def transform(obj, *args, **kwargs) -> pl.DataFrame:
                transformed_data = obj.raw_data.with_columns(
                    pl.col("species").str.to_uppercase()
                )
                return transformed_data

            ```
        """

        @wraps(transform_func)
        def _transform_wrapper(*args, **kwargs) -> None:
            _start = perf_counter_ns()
            self._data = transform_func(self)
            self._transform_time = (_start, perf_counter_ns())
            self.state = ETLState.TRANSFORMED
            self.logger.info(f"Transformed (in {self.transform_time})")
            self.logger.info(f"- type={type(self._data)}")
            self.logger.info(f"- len={len(self._data)}")
            return

        self.transform = _transform_wrapper
        return

    def load_wrapper(self, load_func: Callable) -> None:
        """
        Decorator to register the class's `load` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            obj (self): A reference to the object/self
            warehouse (Warehouse): The user-defined warehouse

        Returns:
            None
        """
        from boltetl.core._warehouse import Warehouse  # noqa

        @wraps(load_func)
        def _load_wrapper(warehouse: Warehouse, *args, **kwargs) -> None:
            _start = perf_counter_ns()
            load_func(self, warehouse)
            self._load_time = (_start, perf_counter_ns())
            self.logger.info(f"Loaded (in {self.load_time})")
            self.state = ETLState.LOADED
            self.metadata._insert(warehouse)
            self.logger.info("Metadata inserted")
            return

        self.load = _load_wrapper
        return

    def data_wrapper(self, data_func: Callable) -> None:
        """
        Decorator that allows users to bypass `extract` and `transform` methods
        and set the Datasource's `.data` attribute directly.

        The function that this decorator wraps must have the following arguments:

        Args:
            obj (self): A reference to the object/self

        Returns:
            data (Any): Processed data (probably a DataFrame)

        Example:
            ```python
            import polars as pl
            from boltetl import Datasource

            test_datasource = Datasource(name="TEST")


            @test_datasource.data_wrapper
            def set_data(obj, *args, **kwargs) -> pl.DataFrame:
                data = pl.DataFrame({"name": ["Sugar", "Spice"], "species": ["cat", "cat"]})
                return data
            ```
        """

        @wraps(data_func)
        def _data_wrapper(*args, **kwargs) -> None:
            self._data = data_func(self)
            # NOTE: self._raw_data should be left None
            self.state = ETLState.TRANSFORMED
            self.raw_data_origin = RawDataOrigin.DIRECTLY_SET
            return

        _data_wrapper()
        return

    def validate_wrapper(self, validate_func: Callable) -> None:
        """
        Decorator to register the class's `validate` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            arg (type): Desc

        Returns:
            Callable: Decorated function
        """

        @wraps(validate_func)
        def _validate_wrapper(*args, **kwargs) -> None:
            validate_func(self)  # Execute the validation function
            self.state = ETLState.VALIDATED
            return

        # self.cache = _validate_wrapper
        return

    def cache_write_wrapper(self, cache_write_func: Callable) -> None:
        """
        Decorator to register the class's `cache` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            arg (type): Desc

        Returns:
            Callable: Decorated function
        """

        @wraps(cache_write_func)
        def _cache_write_wrapper(*args, **kwargs) -> None:
            if not (self.write_cache and self.cache_path):
                raise AttributeError(
                    "`cache_path` attribute and `write_cache` function must both exist"
                )

            _start = perf_counter_ns()
            self.logger.info(f"Caching data to '{self.cache_dir}'")
            cache_write_func(self)
            self._cache_write_time = (_start, perf_counter_ns())
            self.state = ETLState.CACHE_WRITTEN
            # TODO: write metadata JSON file
            self.logger.info(f"Cached transformed data (in {self.cache_write_time})")
            return

        self.write_cache = _cache_write_wrapper
        return

    def cache_read_wrapper(
        self, cache_read_func: Callable
    ) -> None:  # TODO: needs attention/testing
        """
        Decorator to register the class's `read_cache` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            arg (type): Desc

        Returns:
            Callable: Decorated function
        """

        @wraps(cache_read_func)
        def _cache_read_wrapper(*args, **kwargs) -> None:
            _start = perf_counter_ns()
            self._data = cache_read_func(self)
            # NOTE: self._raw_data should be left None
            if self._data is None:
                raise ValueError("Cache-reading function must return data")
            self._cache_read_time = (_start, perf_counter_ns())
            self.logger.info(f"Cached data read (in {self.cache_read_time})")
            # self.state = ETLState.TRANSFORMED
            self.state = ETLState.CACHE_READ
            self.raw_data_origin = RawDataOrigin.FROM_CACHE
            # TODO: read metadata JSON file
            return

        self.read_cache = _cache_read_wrapper
        return

    def tool(self, tool_func: Callable) -> None:
        """
        Decorator to register a "tool" method on the class.
        (This basically amounts to defining a method on the class.)

        The function that this decorator wraps must have the following arguments:

        Args:
            arg (type): Desc

        Returns:
            Callable: Decorated function
        """

        @wraps(tool_func)
        def _tool_wrapper(*args, **kwargs) -> None:
            return tool_func(self, *args, **kwargs)  # Execute the tool

        setattr(self, tool_func.__name__, _tool_wrapper)
        self.tool_names.append(tool_func.__name__)
        return

    # ========================================================================
    # Misc methods

    def _default_load(self, warehouse) -> None:
        # Default data loader
        self.logger.info("Loading data (with default loader)")
        try:
            _start = perf_counter_ns()
            df = self.data  # noqa: F841
            with warehouse.connect() as con:
                con.sql(f"CREATE OR REPLACE TABLE {self.name} AS SELECT * FROM df")
            self._load_time = (_start, perf_counter_ns())
        except Exception as e:
            self.logger.critical("FAILED to load (with default loader)")
            raise e
        self.logger.info(f"Loaded (in {self.load_time})")
        self.metadata._insert(warehouse)
        self.logger.info("Metadata inserted")
        return

    @staticmethod
    def get_console(tool_kwargs: dict) -> Console:
        """Utility method for passing a rich.console.Console to the tool."""
        return tool_kwargs.get("console", Console())

    def set_state(self, state: State):
        self.state = ETLState.__members__[state]
        return
        

    # ========================================================================
    # Update method

    def update(
        self,
        warehouse,
        download=True,
        read_cache=True,
        validate=True,
        write_cache=True,
        console=None,
        **kwargs,
    ) -> None:
        """
        Executes the ETL operations.

        Args:
            warehouse (boltetl.Warehouse): The user's warehouse/database (boltetl.env.warehouse)

        Returns:
            data (Any): Processed data (probably a DataFrame)

        Raises:
            ValueError: If extract function is not defined
            ValidationError: If any validation fails
        """
        _status: str = f"{self.name}: Setting up..."
        with console.status(f"      [cyan]{_status}[/]"):
            self.logger.info(f"Starting update for {self._name}")
            self.logger.debug(f"kwargs={kwargs}")
            self.logger.debug(f"Metadata:\n{self.metadata.to_json()}")
            self.logger.debug(f"Options:\n{self.options.to_json()}")  # TODO: json_indent=4 ?
            if console:
                self.logger.debug(f"Console passed as argument (quiet={console.quiet})")
            else:
                from rich.console import Console

                console = Console(quiet=True)
                self.logger.debug(f"New console created (quiet={console.quiet})")

        # --------------------------------------------------------------------
        # Download (optional)
        _status = f"{self.name}: Downloading..."
        with console.status(f"      [cyan]{_status}[/]"):
            if self.download and self._download_wrapper_used:
                if download:  # arg
                    self.logger.info(_status)
                    self.download(**kwargs)
                else:
                    self.logger.info("Download skipped")

        # --------------------------------------------------------------------
        # Extract / Read Cache
        if self.read_cache is not None and read_cache is True:  # arg
            _status = f"{self.name}: Reading cache..."
            with console.status(f"      [cyan]{_status}[/]"):
                self.logger.info(_status)
                self.read_cache()  # Sets state to TRANSFORMED
        elif self.extract is not None and self.state < ETLState.EXTRACTED:
            _status = f"{self.name}: Extracting..."
            with console.status(f"      [cyan]{_status}[/]"):
                self.logger.info(_status)
                self.extract(**kwargs)
        else:
            self.logger.info("No extract function defined")

        # --------------------------------------------------------------------
        # Transform data if `transform` function was defined
        _status = f"{self.name}: Transforming..."
        with console.status(f"      [cyan]{_status}[/]"):
            if self.transform is not None and self.state < ETLState.TRANSFORMED:
                self.logger.info(_status)
                self.transform()
            # Support the lack of transform
            elif self.transform is None and self.state < ETLState.TRANSFORMED:
                self.logger.warning("No transform function defined; setting `data` to `raw_data`")
                self._data = self._raw_data

        # --------------------------------------------------------------------
        # Validate
        _status = f"{self.name}: Validating..."
        with console.status(f"      [cyan]{_status}[/]"):
            if self.validate and validate:  # arg
                self.logger.info(_status)
                self.validate()

        # --------------------------------------------------------------------
        # Cache (Write)
        # Optionally write transformed data to cache (disk)
        _status = f"{self.name}: Writing cache..."
        with console.status(f"      [cyan]{_status}[/]"):
            if (
                self.write_cache is not None
                and self.raw_data_origin != RawDataOrigin.FROM_CACHE
                and write_cache is True
            ):
                self.logger.info(_status)
                self.write_cache()

        # --------------------------------------------------------------------
        # Load
        _status = f"{self.name}: Loading..."
        with console.status(f"      [cyan]{_status}[/]"):
            if self.load is not None:
                self.logger.info(_status)
                self.load(warehouse)
            else:
                self.logger.info("Loading (using default function)")
                self._default_load(warehouse)

        # --------------------------------------------------------------------
        # Confirm load success
        # TODO: this just isn't the way we should do this... keeping code here for now
        # _status = f"{self.name}: Confirming load..."
        # with console.status(f"      [cyan]{_status}[/]"):
        #     tables = set(warehouse.list_tables())
        #     if isinstance(self.data, (tuple, list, dict)):
        #         if isinstance(self.data, (tuple, list)):
        #             names = set([i[0] for i in self.data])
        #         else:
        #             names = set([i for i in self.data.keys()])
        #         if not names.issubset(tables):
        #             self.logger.critical("FAILURE: Table load could not be confirmed")
        #             raise DataError(f"Not all tables ({len(names)}) exist after attempting load")
        #     elif self.name not in tables:
        #         self.logger.critical("FAILURE: Table load could not be confirmed")
        #         raise DataError(f"Table '{self.name}' does not exist after attempting load")
        #     self.logger.info("Table load confirmed")

        # _status = f"{self.name}: Complete"
        # with console.status(f"      [cyan]{_status}[/]"):
        #     self.logger.info(f"Updated '{self.name}'!")
        # --------------------------------------------------------------------
        return

    # ========================================================================
    # Dunder methods

    def __repr__(self):
        """
        String representation of the Datasource instance.
        """
        return f"<Datasource(name='{self._name}')>"


# Inject a smarter docstring
Datasource.set_state.__doc__ = f"""
    Expose a state setter to users.

    Args:
        state ({State.__value__}): Manually/override the datasource's (ETL) state
    """
