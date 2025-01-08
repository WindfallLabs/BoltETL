import toml
from pathlib import Path


CONFIG_PATH = r"C:\Workspace\tmpdb\.BoltETL\config.toml"


class Config:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance.load_config()
        return cls._instance

    def load_config(self):
        cfg = toml.load(CONFIG_PATH)
        self.data = cfg["data"]

        # Globals
        for k, v in cfg["global"].items():
            if k.endswith("dir") or k.endswith("path"):
                v = Path(v)
            setattr(self, k, v)
