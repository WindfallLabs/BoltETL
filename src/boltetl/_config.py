import re
from pathlib import Path


class _Config:
    def __init__(self):
        self.refresh()

    def refresh(self):
        self.config_dir = Path("~").expanduser() / ".bolt"
        self.log_dir = self.config_dir / "logs"
        self.envs: dict[str, Path] = dict([(re.split(".env-", i.name)[1], Path(i.open().read())) for i in self.config_dir.glob(".env-*")])
        self.current_env_name: str = [i.open().read() for i in self.config_dir.glob(".active")][0]
        self.current_env = self.envs[self.current_env_name]
        self.env_dir = self.current_env
        self.cli_options = {"logo": True, "style": "yellow on black"}

    def activate_env(self, env_name: str):
        self.refresh()
        self.current_env_name = env_name
        self.current_env = self.envs[self.current_env_name]
        return

    def get_env_name(self):
        self.current_env_name: str = [i.open().read() for i in self.config_dir.glob(".active")][0]
        return self.current_env_name

    def __repr__(self):
        d = {
            k: v for k, v in self.__dict__.items() if not k.startswith("_")
        }
        #return str(d)
        s = ""
        for k, v in d.items():
            if k.startswith("_"):
                continue
            s += f"- {k}: {v}\n"
        return s


# TODO: work-around
Config = _Config()
