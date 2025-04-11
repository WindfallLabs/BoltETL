"""Configuration object.

BoltETL uses the dotenv package to handle environments.
Out-of-the-box, the DEFAULT environment is the current user's directory
(e.g. 'C:\\Users\\<USER>\\.bolt' on Windows). This is a static value that
never changes.

The ACTIVE environment is a reference to the activated environment's path.
It changes each time the environment is changed/set.

User's can set their own environment directory using something like:
```python
import bolt

bolt.Config.add_env("test", "C:\\Workspace\\test")
bolt.Config.add_env("main", "C:\\Workspace\\bolt")
bolt.Config.activate_env("main")

# or by setting the set_default arg to True
bolt.Config.add_env("main", "C:\\Workspace\\bolt", activate=True)
bolt.Config.add_env("test", "C:\\Workspace\\test")
```

"""

import tomllib
from pathlib import Path

import dotenv

# Future ideas:
# TODO: always-force update option for bolt-cmd
# TODO: always-skip-db option for bolt-cmd


class Config:  # TODO: refactor to an Env class
    config_dir = Path("~").expanduser() / ".bolt"
    _default_env_line = f"BOLT-DEFAULT={str(config_dir)}\n"
    env_file = config_dir / ".env"
    # TODO: config_file = config_dir / "boltetl.toml"
    if env_file.exists():
        env_dir = Path(dotenv.dotenv_values(env_file)["BOLT-ACTIVE"])  # type: ignore[arg-type]
    else:
        env_dir = config_dir
    log_dir: Path = env_dir / "logs"

    try:  # TODO: this config object is getting sloppy...
        with (env_dir / "config.toml").open() as cfg:
            cli_options = tomllib.loads(cfg.read())["cli"]
    except Exception:
        cli_options = {"logo": True, "style": "yellow on black"}

    @classmethod
    def init(cls, overwrite=False):
        if not cls.config_dir.exists():
            cls.config_dir.mkdir()
        if not cls.env_file.exists() or overwrite:
            with cls.env_file.open("w") as f:
                f.write("# BoltETL Environments (do not edit)\n")
                f.write(cls._default_env_line)
                f.write(f"BOLT-ACTIVE={str(cls.config_dir)}\n")
                f.write("# User Environments\n")
        return

    @classmethod
    def activate_env(cls, env_name: str):
        """Write an environment path to the config file."""
        if f"{env_name}=" not in cls.env_file.open().read():
            raise KeyError(f"No environment with name '{env_name}'")
        new_default = dotenv.dotenv_values(cls.env_file)[env_name]
        # Read entire .env
        lines = cls.env_file.open().readlines()
        lines[2] = f"BOLT-ACTIVE={new_default}\n"
        cls.env_file.open("w").writelines(lines)
        return

    @classmethod
    def add_env(cls, env_name: str, path: str | Path, activate=False):
        """Direct BoltETL to an environment (for this seesion)."""
        if env_name in {"BOLT-ACTIVE", "BOLT-DEFAULT"}:
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
        if activate:
            cls.activate_env(env_name)
        return

    @classmethod
    def use_env(cls, env_name: str):
        """Use a saved environment."""
        cls.env_dir = Path(dotenv.dotenv_values(cls.env_file)[env_name])  # type: ignore[arg-type]
        return

    @classmethod
    def get_default_env(cls):
        """Best way to load the default environment."""
        return Path(dotenv.dotenv_values(cls.env_file)["BOLT-DEFAULT"])

    @classmethod
    def get_env_name(cls):
        d = {v.__str__(): k for k, v in dotenv.dotenv_values(cls.env_file).items()}
        return d[cls.env_dir.__str__()]

    @classmethod
    def list_envs(cls):
        return list(dotenv.dotenv_values(cls.env_file).items())
