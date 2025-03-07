import importlib
import os
import sys
from pathlib import Path

# Path to the folder containing Python scripts
libs = (Path(os.environ["BOLT-DATA"]) / "datasources").glob("[!_]*.py")

for lib in libs:
    name = lib.stem  # More reliable than splitting on '.'
    spec = importlib.util.spec_from_file_location(name, lib.absolute())
    if spec is not None:
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        # Make the module accessible in the current namespace
        globals()[name] = getattr(module, name)
