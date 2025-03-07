"""Datasource ABC."""

from dataclasses import dataclass, field
from functools import wraps
#from getpass import getuser
from hashlib import sha256
from logging import Logger
from pathlib import Path
#from platform import node
from typing import Any, Callable, Optional, List, Tuple

from ._options import Options
from bolt.utils import config, make_logger, IOLogger


'''
class ValidationError(Exception):
    """Custom exception for validation failures."""
    def __init__(self, message: str, validation_name: str):
        self.message = message
        self.validation_name = validation_name
        super().__init__(self.message)
'''


@dataclass  # TODO: if this used pydantic instead, we could dump as JSON (to database)
class Metadata:
    """
    Metadata dataclass.

    Args (all are optional):
        description (str): String description of the datasource
        tags (set[str]): Tags
        vendor (str): Name of software vendor
        software (str): Name of source software
        ...
        kwargs (dict): Misc key-value pairs to add to the dataclass
    """
    description: str|None = None
    data_dict: dict[str, str]|None = None
    tags: set[str]|None = None
    vendor: str|None = None
    software: str|None = None
    source_url: str|None = None
    # TODO: api: str|None = None
    # TODO: database_uri: str|None = None
    # TODO: documentation path?
    schema: list[tuple[str, type]]|None = None  # TODO: final vs prelim?
    datasource = None
    sources_hash = None
    kwargs: field(default_factory=dict) = None  # type: ignore

    def __post_init__(self):
        if self.tags:
            for tag in self.tags:
                if not tag.startswith("#"):
                    self.tags.remove(tag)
                    self.tags.add(f"#{tag}")

        # self.sources_hash = hash_sources()
        if self.kwargs:
            [setattr(self, k, v) for k, v in self.kwargs.items()]

    def hash_sources(self):
        """Gets a hash of the source files."""
        hashes = []
        try:
            for p in self.datasource.source_files:
                p: Path = Path(p)
                if p.is_dir():
                    # raise AttributeError("TODO: Hash cannot be performed on folder")
                    continue
                with p.open("rb") as f:
                    hashes.append(sha256(f.read()).hexdigest())
        except Exception:
            raise AttributeError(f"TODO: Hash cannot be performed on {self.datasource.name}")
        self.sources_hash = sha256("".join(hashes).encode("UTF8")).hexdigest()[:7]
        return self.sources_hash


class Datasource[T]:
    """."""
    registry: dict[str, T] = dict()

    def __init__(
        self,
        name: str,
        source_dir: Path,
        source_filename: str,
        metadata: Metadata|None = None,
        options: Options|None = None
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
        self.metadata = metadata if metadata else Metadata()
        self.metadata.datasource = self
        self.options = options if options else Options()

        # TODO: hash the stack of raw data?

        # Logging setup
        self.logger: Logger|IOLogger = make_logger(self._name, self.options.log_dir)

        # Validation-specific attributes
        self._validation_funcs: List[Tuple[int, str, Callable]] = []

        # Data
        self._raw_data: Optional[Any] = None
        self._data: Optional[Any] = None

        # Processing attributes
        self.extract: Optional[Callable] = None
        self.transform: Optional[Callable] = None
        self.cache: Optional[Callable] = None

        # Register datasource
        if self.options.register:
            self.registry[self._name] = self

    @property
    def name(self) -> str:
        """Getter for datasource name"""
        return self._name

    @property
    def source_files(self) -> list[Path]:
        return [
            p.absolute()
            for p in Path(self.source_dir).rglob(self.source_filename)
            # Ignore source files that start with "_" or "~"
            if not p.name.startswith("_") and not p.name.startswith("~")
        ]

    @property
    def raw_data(self) -> Optional[Any]:
        """Getter for raw dataframe."""
        return self._raw_data

    @property
    def data(self) -> Optional[Any]:
        """Getter for processed dataframe"""
        return self._data

    # ========================================================================
    # Wrapper methods
    def extract_wrapper(self, func: Callable) -> Callable:
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
            self._raw_data = func(self.source_files, self.metadata, self.options, self.logger)
            return
        self.extract = wrapper
        return wrapper

    def transform_wrapper(self, func: Callable) -> Callable:
        """
        Decorator to register the class's `transform` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            raw (Any): The raw data that was defined in the `extract` method
            metadata (Metadata): Metadata object for the Datasource
            logger (Logger): A file-logger for logging

        Returns:
            data (Any): Processed data (probably a DataFrame)
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            self._data = func(self.raw_data, self.metadata, self.options, self.logger)
            return
        self.transform = wrapper
        return wrapper

    def load_wrapper(self, func: Callable) -> Callable:  # TODO:
        """
        Decorator to register the class's `load` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            data (Any): The processed data that was defined in the `transform` method
            metadata (Metadata): Metadata object for the Datasource
            logger (Logger): A file-logger for logging

        Returns:
            None
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            self.load = func(self.raw, self.metadata, self.options, self.logger)  # TODO:
            return
        self.load = wrapper
        return wrapper

    def cache_wrapper(self, func: Callable) -> Callable:
        """
        Decorator to register the class's `cache` method.

        The function that this decorator wraps must have the following arguments:

        Args:
            arg (type): Desc

        Returns:
            Callable: Decorated function
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            #self._data = func(???, self.metadata, self.options, self.logger)  # TODO: args
            return
        self.cache = wrapper
        return wrapper

    '''
    def validates(
        self,
        name: str = "unnamed_validation",
        priority: int = 0
    ):
        """
        Decorator to add validation functions with logger access.

        Args:
            name (str): Name of the validation
            priority (int): Priority of the validation
                            (lower numbers run first, default is 0)

        Returns:
            Callable: Decorator function
        """
        def decorator(func):
            """
            Wrap the validation function to provide logger.

            Args:
                func (Callable): Validation function

            Returns:
                Callable: Wrapped validation function
            """
            @wraps(func)
            def wrapper(df):
                # Call original function with logger
                return func(df, self.logger)

            # Store wrapped function
            self._validation_funcs.append((priority, name, wrapper))

            # Sort validations by priority
            self._validation_funcs.sort(key=lambda x: x[0])  # TODO: reverse?

            return wrapper
        return decorator
    '''

    '''
    def _run_validations(self, df: Any):
        """
        Run all registered validation functions.

        Args:
            df (DataFrame): Dataframe to validate

        Raises:
            ValidationError: If any validation fails
        """
        self.logger.info(f"Starting validations for {self._name}")

        if not self._validation_funcs:
            self.logger.warning("No validations defined")
            return

        for priority, name, validation_func in self._validation_funcs:
            try:
                self.logger.info(f"Running validation: {name} (priority: {priority})")

                if not validation_func(df):
                    error_msg = f"Validation '{name}' failed"
                    self.logger.error(error_msg)
                    raise ValidationError(error_msg, name)

                self.logger.info(f"Validation '{name}' passed")

            except Exception as e:
                self.logger.error(f"Validation '{name}' failed: {str(e)}")
                raise ValidationError(
                    f"Validation '{name}' encountered an error: {str(e)}",
                    name
                )

        self.logger.info("All validations completed successfully")
    '''

    # ========================================================================
    # Update method
    def update(self) -> Any:
        """
        Executes the ETL operations.

        Returns:
            data (Any): Processed data (probably a DataFrame)

        Raises:
            ValueError: If extract function is not defined
            ValidationError: If any validation fails
        """
        self.logger.info(f"Starting update for datasource: {self._name}")

        if not self.extract:
            error_msg = "No extract function defined"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # Extract data
        self.logger.info(f"Extracting data from {self.source_dir}")
        self.extract()

        # Run validations
        #self._run_validations(self._data)  # TODO: move

        # Transform data if transform function exists
        if not self.transform:
            error_msg = "No extract function defined"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        self.logger.info("Applying transformation")
        self.transform()

        if self.cache and self.options.cache_dir:
            self.cache(self.options.cache_dir)

        self.logger.info("Update completed successfully")

        # TODO: more...
        # TODO: self.load()
        return self._data

    '''
    def save(self, path: Optional[str] = None):
        """
        Save the processed dataframe to a specified path.

        Args:
            path (Optional[str]): Path to save the dataframe.
                                  If None, uses a default naming convention.
        """
        if self._data is None:
            error_msg = "No dataframe to save. Run update() first."
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        if path is None:
            path = f"{self._name}_processed.parquet"

        self._logger.info(f"Saving dataframe to {path}")
        self._data.write_parquet(path)
        self._logger.info("Dataframe saved successfully")
    '''

    def __repr__(self):
        """
        String representation of the Datasource instance.
        """
        return (f"<Datasource(name='{self._name}')>")
