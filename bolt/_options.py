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
        cache_path (Path): Path where transformed data will be saved to disk
            (only when a user-defined `cache` function uses it)
        register (bool): Whether or not to add to datasources registry
            (False disables the datasource from use in BoltETL tools)
        ...
        kwargs (dict): Misc key-value pairs to add to the dataclass

    """
    log_dir: Path|None = None
    log_format: str|None = None
    cache_path: Path|None = None
    register: bool = True
    # TODO: more?
    kwargs: field(default_factory=dict) = None  # type: ignore

    def __post_init__(self):
        if self.kwargs:
            [setattr(self, k, v) for k, v in self.kwargs.items()]

    def to_json(self):
        string_dict = {}
        for k, v in self.__dict__.items():
            string_dict[str(k)] = str(v)
        return json.dumps(string_dict, indent=4)
