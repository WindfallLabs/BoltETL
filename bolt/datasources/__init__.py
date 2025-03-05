import importlib
import sys

from ..utils import config
from ._datasource import Datasource  # noqa: F401

# sys.path.append(str(config.definitions_dir))

# Assuming that the user-defined `__datasources__` module exists in `definitions_dir`
# from __datasources__ import *  # noqa: F403


for ds_name, metadata in config.metadata.items():
    mod_name = metadata["def_path"].split("\\")[-1].split(".")[0]
    # mod = importlib.import_module(f".datasources.__pycache__.{mod_name}", package="bolt")
    spec = importlib.util.spec_from_file_location(mod_name, metadata["def_path"])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    setattr(sys.modules[__name__], ds_name, getattr(module, ds_name))
