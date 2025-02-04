from . import funcs, schema, version
from . import types  # TODO: deprecate
from ._config import CONFIG_PATH, Config
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
    "schema",
    # ...
    "version",
    "YearMonth",
]
