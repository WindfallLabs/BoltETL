import sys

from ..utils import config
from ._datasource import Datasource  # noqa: F401

sys.path.append(str(config.definitions_dir))

# Assuming that the user-defined `__datasources__` module exists in `definitions_dir`
from __datasources__ import *  # noqa: F403
