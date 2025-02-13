import sys

from ..utils import config
from ._datasource import Datasource

sys.path.append(str(config.definitions_dir))

# Assuming that the user-defined `__datasources__` module exists in `definitions_dir`
from __datasources__ import *
