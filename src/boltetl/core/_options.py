import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Options:
    """
    Options dataclass.

    Args:
        log_dir (Path): The Path/directory of the output log
            (The presence/absence of this value enables/disables logging)
        log_format (str): The format used in logging (default None; uses default format)
        cache_path (Path): Filepath (specific file) where transformed data (single) may be cached
        cache_dir (Path): Directory where transformed data (multiple) may be cached
        register (bool): Whether or not to add to datasources registry
            (False disables the datasource from use in BoltETL tools)
        require_force (bool): If True, bolt_cli.py will always ignore unless the `--force` flag is used
        ...
        kwargs (dict): Misc key-value pairs to add to the dataclass

    """

    log_dir: Path | None = None  # TODO: remove?
    log_format: str | None = None
    cache_path: Path | None = None  # TODO: remove
    cache_dir: Path | None = None
    register: bool = True
    # require_force: bool = False  # TODO: Useful?
    # TODO: more?
    kwargs: field(default_factory=dict) = None  # type: ignore

    def __post_init__(self):
        if self.kwargs:
            [setattr(self, k, v) for k, v in self.kwargs.items()]
        self.parent = None

        if self.log_dir and type(self.log_dir) is str:
            self.log_dir = Path(self.log_dir)

        if self.cache_path and not self.cache_dir:
            self.cache_dir = self.cache_path.parent

        if self.cache_dir and type(self.cache_dir) is str:
            self.cache_dir = Path(self.cache_dir)

    def to_json(self):
        string_dict = {}
        for k, v in self.__dict__.items():
            string_dict[str(k)] = str(v)
        return json.dumps(string_dict, indent=4)

    def __repr__(self):
        if self.parent:
            return f"<Options(parent='{self.parent.name}')>"
        return "<Options(parent=None)>"
