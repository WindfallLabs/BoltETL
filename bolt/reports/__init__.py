from ._report import BaseReport
from ..utils import config

import sys
sys.path.append(str(config.report_dir))

# Assuming that the user-defined `__datasources__` module exists in `report_dir`
from __reports__ import *
