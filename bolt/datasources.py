import importlib
import os
import sys
from pathlib import Path

from bolt._datasource import Datasource

# Path to the folder containing Python scripts
libs = (Path(os.environ["BOLT-DATA"]) / "datasources").glob("[!_]*.py")

for lib in libs:
    name = lib.stem  # More reliable than splitting on '.'
    try:
        spec = importlib.util.spec_from_file_location(name, lib.absolute())
        if spec is not None:
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
            # Make the module accessible in the namespace
            globals()[name] = getattr(module, name)
    except Exception as e:
        Datasource.failed_to_load.add((name, e))
