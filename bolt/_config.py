"""Configuration object.

BoltETL uses the dotenv package to handle environments.

When initialized with `Config.init`, a ".env" file is created in the
current user's directory (e.g. 'C:\\Users\\<USER>\\.bolt' on Windows).

User's can set their own environment directory using something like:
```python
import bolt

bolt.Config.add_env("main", "C:\\Workspace\\bolt")
bolt.Config.set_default_env("main")

# or by setting the set_default arg to True
bolt.Config.add_env("main", "C:\\Workspace\\bolt", True)
```

"""

from pathlib import Path

import dotenv

_ENV_KEY = "BOLT-ENV"


class Config:
    config_dir = Path("~").expanduser() / ".bolt"
    _default_env_line = f"BOLT-DEFAULT={str(config_dir)}\n"
    env_file = config_dir / ".env"
    if env_file.exists():
        env_dir = Path(dotenv.dotenv_values(env_file)[_ENV_KEY])
    else:
        env_dir = config_dir
    log_dir: Path = env_dir / "logs"

    @classmethod
    def init(cls, overwrite=False):
        if not cls.config_dir.exists():
            cls.config_dir.mkdir()
        if not cls.env_file.exists() or overwrite:
            with cls.env_file.open("w") as f:
                f.write("# BoltETL Environments (do not edit)\n")
                f.write(cls._default_env_line)
                f.write(f"{_ENV_KEY}={str(cls.config_dir)}\n")
                f.write("# User Environments\n")
        return

    @classmethod
    def set_default_env(cls, env_name: str):
        """Write an environment path to the config file."""
        if f"{env_name}=" not in cls.env_file.open().read():
            raise KeyError(f"No environment with name '{env_name}'")
        new_default = dotenv.dotenv_values(cls.env_file)[env_name]
        # Read entire .env
        lines = cls.env_file.open().readlines()
        lines[2] = f"{_ENV_KEY}={new_default}\n"
        cls.env_file.open("w").writelines(lines)
        return

    @classmethod
    def add_env(cls, env_name: str, path: str | Path, set_default=False):
        """Direct BoltETL to an environment (for this seesion)."""
        if env_name in {_ENV_KEY, "BOLT-DEFAULT"}:
            raise KeyError(f"Cannot use reserved name '{env_name}'")
        if isinstance(path, str):
            path = Path(path)
        if not path.exists() or not path.is_dir():
            raise FileExistsError(f"Folder does not exist: '{str(path)}'")
        env = f"{env_name}={str(path)}\n"
        if env in cls.env_file.open().read():
            raise KeyError(f"Environment already exists '{env}'")
        with cls.env_file.open("a") as f:
            f.write(env)
        if set_default:
            cls.set_default_env(env_name)
        return

    @classmethod
    def use_env(cls, env_name: str):
        """Use a saved environment."""
        cls.env_dir = Path(dotenv.dotenv_values(cls.env_file)[env_name])
        return

    @classmethod
    def get_default_env(cls):
        """Best way to load the default environment."""
        return Path(dotenv.dotenv_values(cls.env_file)["BOLT-DEFAULT"])

    @classmethod
    def list_envs(cls):
        return list(dotenv.dotenv_values(cls.env_file).items())
