from . import warehouse
from ._datasource import Datasource
from ..utils import config

import sys
sys.path.append(str(config.data_dir))

# Assuming that the user-defined `__datasources__` module exists in `data_dir`
from __datasources__ import *

__all__ = [
    "Datasource",
    "warehouse",
]
