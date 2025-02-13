import sys

from ..utils import config
from ._report import BaseReport  # noqa: F401

sys.path.append(str(config.definitions_dir))

# Assuming that the user-defined `__reports__` module exists in `definitions_dir`
from __reports__ import *  # noqa: F403
