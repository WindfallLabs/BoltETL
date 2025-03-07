from . import (
    funcs,
    schema,
    version,
)
from ._config import CONFIG_PATH, Config
from ._download import download
from ._logger import make_logger, IOLogger
from ._rich import df_to_table
from ._yearmonth import YearMonth

config = Config()
#CRS = config.crs

__all__ = [
    "CRS",
    #"config",
    "CONFIG_PATH",
    "df_to_table",
    "download",
    "funcs",
    "make_logger",
    "import_user_package",
    "IOLogger",
    "schema",
    # ...
    "version",
    "YearMonth",
]
