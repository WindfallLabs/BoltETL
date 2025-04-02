"""Datasource ABC."""

from enum import Enum
from functools import wraps
from logging import Logger
from pathlib import Path
from time import perf_counter_ns
from typing import Any, Callable, Optional

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
    INIT = "INIT"
    EXTRACTED = "EXTRACTED"
    FROM_CACHE = "FROM_CACHE"
    DIRECTLY_SET = "DIRECTLY_SET"


class ETLState(Enum):
    INIT = "INIT"
    EXTRACTED = "EXTRACTED"
    TRANSFORMED = "TRANSFORMED"
    READ_FROM_CACHE = "READ_FROM_CACHE"
    DIRECTLY_SET = "DIRECTLY_SET"
    VALIDATED = "VALIDATED"
    LOADED = "LOADED"
    # READ_FROM_WAREHOUSE = "READ_FROM_WAREHOUSE"

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


class Datasource[T]:
    """."""

    registry: dict[str, T] = dict()
    failed_to_load: set[tuple[str, Exception]] = set()

    def __init__(
        self,
        name: str,
        source_dir: Path | None = None,
        source_filename: str | None = None,
        metadata: Metadata | None = None,
        options: Options | None = None,
    ):
        """
        Initialize a Datasource instance.

        Args:
            name (str): Unique identifier for the datasource
            source_dir (Path): Path to the directory containing the raw source data
            source_filename (str): filename or glob pattern of raw source filename(s)
        """
        # Instance-specific attributes
        self._name = name
        self.source_dir = source_dir
        self.source_filename = source_filename

        # Handle metadata
        self.metadata = metadata if metadata else Metadata()
        self.metadata.datasource = self
        # TODO: hash the stack of raw data here?

        # Handle options
        self.options = options if options else Options()
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
        self._extract_time: tuple[float, float]|None = None
        self._transform_time: tuple[float, float]|None = None
        self._load_time: tuple[float, float]|None = None

        # Processing attributes
        self.extract: Optional[Callable] = None
        self.transform: Optional[Callable] = None
        self.cache_write: Optional[Callable] = None
        self.cache_read: Optional[Callable] = None
        self.validate: Optional[Callable] = None
        self.load: Optional[Callable] = None

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
        if self._data is None and self.raw_data_origin == RawDataOrigin.DIRECTLY_SET:
            self.extract()
        return self._data

    @property
    def has_raw_data(self):
        """Whether or not `self.data is not None`."""
        return self.raw_data is not None

    @property
    def has_data(self):
        """Whether or not `self.data is not None`."""
        return self.data is not None

    @property
    def is_extracted(self):
        """Whether or not the `extract` method was called successfully."""
        return self.state >= ETLState.EXTRACTED and (self.has_raw_data or self.has_data)

    @property
    def is_transformed(self):
        """Whether or not the `transform` method was called successfully."""
        return self.state >= ETLState.TRANSFORMED

    @property
    def is_directly_set(self):
        """Whether or not the data was set within a `data_wrapper`."""
        return self.raw_data_origin == RawDataOrigin.DIRECTLY_SET and self.has_data

    @property
    def is_loaded(self):
        """Whether or not the `load` method was called successfully."""
        return self.state >= ETLState.LOADED

    @property
    def is_cached_data(self):
        """Whether or not `self.data` was read from cache."""
        return self.raw_data_origin == RawDataOrigin.FROM_CACHE

    @property
    def has_cached_data(self):
        """Whether or not transformed data exists on-disk."""
        return self.options.cache_path and self.options.cache_path.exists()

    @property
    def extract_time(self):
        """The run time of the extract function."""
        if self._extract_time:
            return time_diff(*self._extract_time)
        return "-1"

    @property
    def transform_time(self):
        """The run time of the transform function."""
        if self._transform_time:
            return time_diff(*self._transform_time)
        return "-1"

    @property
    def load_time(self):
        """The run time of the load function."""
        if self._load_time:
            return time_diff(*self._load_time)
        return "-1"

    # ========================================================================
    # Wrapper methods

    def extract_wrapper(self, extract_func: Callable) -> Callable:
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
            from bolt import Datasource

            test_datasource = Datasource(name="TEST")

            @test_datasource.extract_wrapper
            def extract(obj, *args, **kwargs) -> pl.DataFrame:
                data = pl.DataFrame({"name": ["Sugar", "Spice"], "species": ["cat", "cat"]})
                return data
            ```
        """

        @wraps(extract_func)
        def _extract_wrapper(*args, **kwargs):
            _start = perf_counter_ns()
            extracted_data = extract_func(self)
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

    def transform_wrapper(self, transform_func: Callable) -> Callable:
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
            from bolt import Datasource

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
        def _transform_wrapper(*args, **kwargs):
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

    def load_wrapper(self, load_func: Callable) -> Callable:
        """
        Decorator to register the class's `load` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            obj (self): A reference to the object/self
            warehouse (Warehouse): The user-defined warehouse

        Returns:
            None
        """
        from bolt.core._warehouse import Warehouse  # noqa

        @wraps(load_func)
        def _load_wrapper(warehouse: Warehouse, *args, **kwargs):
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

    def data_wrapper(self, data_func: Callable) -> Callable:  # TODO:
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
            from bolt import Datasource

            test_datasource = Datasource(name="TEST")


            @test_datasource.data_wrapper
            def set_data(obj, *args, **kwargs) -> pl.DataFrame:
                data = pl.DataFrame({"name": ["Sugar", "Spice"], "species": ["cat", "cat"]})
                return data
            ```
        """

        @wraps(data_func)
        def _data_wrapper(*args, **kwargs):
            data = data_func(self)
            self._raw_data = data  # TODO: or should raw be None?
            self._data = data
            return

        self.extract = _data_wrapper
        self.state = ETLState.TRANSFORMED
        self.raw_data_origin = RawDataOrigin.DIRECTLY_SET
        return

    def cache_write_wrapper(self, cache_write_func: Callable) -> Callable:
        """
        Decorator to register the class's `cache` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            arg (type): Desc

        Returns:
            Callable: Decorated function
        """

        @wraps(cache_write_func)
        def _cache_write_wrapper(*args, **kwargs):
            cache_write_func(self)
            # TODO: write metadata JSON file
            self.logger.info(
                f"Cache written: saved tranformed data to disk ('{self.options.cache_path}')"
            )
            return

        self.cache_write = _cache_write_wrapper
        return

    def cache_read_wrapper(
        self, cache_read_func: Callable
    ) -> Callable:  # TODO: needs attention/testing
        """
        Decorator to register the class's `read_cache` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            arg (type): Desc

        Returns:
            Callable: Decorated function
        """

        @wraps(cache_read_func)
        def _cache_read_wrapper(*args, **kwargs):
            self._data = cache_read_func(self)
            self.state = ETLState.TRANSFORMED
            self.raw_data_origin = RawDataOrigin.FROM_CACHE
            # TODO: read metadata JSON file
            self.logger.info(
                f"Cache read: read tranformed data from disk ('{self.options.cache_path}')"
            )
            return

        self.cache_read = _cache_read_wrapper
        return

    def validate_wrapper(self, validate_func: Callable) -> Callable:
        """
        Decorator to register the class's `validate` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            arg (type): Desc

        Returns:
            Callable: Decorated function
        """

        @wraps(validate_func)
        def _validate_wrapper(*args, **kwargs):
            validate_func(self)  # Execute the validation function
            self.state = ETLState.VALIDATED
            return

        # self.cache = _validate_wrapper
        return

    # ========================================================================
    # Misc methods

    def _default_load(self, warehouse):
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

    # def read_warehouse(self) -> None:
    #     """Load the processed data from the warehouse/database."""
    #     # TODO: should we enable users to access end-of-lifecycle data as a 'source'?
    #     import bolt.env
    #     df: pl.DataFrame = bolt.env.warehouse.get_data(self.name)
    #     self._data = df
    #     self.state = ETLState.READ_FROM_WAREHOUSE
    #     return

    # ========================================================================
    # Update method

    def update(self, warehouse) -> Any:  # TODO: consider 'execute_pipeline()'
        """
        Executes the ETL operations.

        Args:
            warehouse (bolt.Warehouse): The user's warehouse/database (bolt.env.warehouse)

        Returns:
            data (Any): Processed data (probably a DataFrame)

        Raises:
            ValueError: If extract function is not defined
            ValidationError: If any validation fails
        """
        self.logger.info(f"Starting update for {self._name}")
        self.logger.debug(f"Metadata:\n{self.metadata.to_json(json_indent=4)}")
        self.logger.debug(f"Options:\n{self.options.to_json()}")  # TODO: json_indent=4

        # Check that `extract` method is set
        # Unless directly set with `data_wrapper`
        if not self.extract or self.state >= ETLState.EXTRACTED:
            extract_error_msg = "An `extract` function is required"
            self.logger.critical(extract_error_msg)
            raise ValueError(extract_error_msg)

        # Check that `load` method is set
        # if not self.load and TEST_FLAG:
        #     load_error_msg = "A `load` function is required"  # TODO: is it though?
        #     self.logger.critical(load_error_msg)
        #     raise ValueError(load_error_msg)

        # --------------------------------------------------------------------
        # Extract data
        if self.raw_data_origin == RawDataOrigin.EXTRACTED:
            self.logger.info(
                f"Extracting data from {len(self.source_files)} file(s) in '{self.source_dir}'"
            )
        self.extract()

        # --------------------------------------------------------------------
        # Transform data if `transform` function was defined
        if self.transform:
            self.logger.info("Applying transformation(s)")
            self.transform()
        else:
            self.logger.warning("No extract function defined")

        # --------------------------------------------------------------------
        # Cache
        # Transformed data optionally saved to disk
        if self.cache_write is None and self.options.cache_path:
            self.logger.warning(
                f"No `cache` function specified; but `options.cache_path` was: '{self.options.cache_path}'"
            )
        elif self.cache_write and self.options.cache_path is None:
            self.logger.warning(
                "`cache` function specified, but no value set to `options.cache_path`"
            )
        elif self.cache_write and self.options.cache_path:
            self.logger.info(f"Caching data to '{self.options.cache_path}'")
            self.cache_write()
            # Else, no caching

        # --------------------------------------------------------------------
        # Validate
        # TODO: validation
        if self.validate:
            self.logger.info("Validating data")
            self.validate()

        # --------------------------------------------------------------------
        # Load
        if self.load:
            self.logger.info("Loading data")
            self.load(warehouse)
        else:
            self._default_load(warehouse)

        self.logger.info("Update completed")
        # --------------------------------------------------------------------
        return

    # ========================================================================
    # Dunder methods

    def __repr__(self):
        """
        String representation of the Datasource instance.
        """
        return f"<Datasource(name='{self._name}')>"
