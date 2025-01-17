from . import funcs
from . import types
from . import version
from ._config import Config, CONFIG_PATH
from ._download import download
from ._logger import make_logger
from ._yearmonth import YearMonth


config = Config()
CRS = config.crs

__all__ = [
    "CRS",
    "config",
    "CONFIG_PATH",
    "download",
    "funcs",
    "make_logger",
    "types",
    # ...
    "version",
    "YearMonth"
]
