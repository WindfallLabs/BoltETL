import sys

from ..utils import config
from ._report import BaseReport

sys.path.append(str(config.report_dir))

# Assuming that the user-defined `__datasources__` module exists in `report_dir`
from __reports__ import *
