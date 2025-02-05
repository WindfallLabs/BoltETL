import sys

from ..utils import config
from ._datasource import Datasource

sys.path.append(str(config.data_dir))

# Assuming that the user-defined `__datasources__` module exists in `data_dir`
from __datasources__ import *

__all__ = [
    "Datasource",
]
