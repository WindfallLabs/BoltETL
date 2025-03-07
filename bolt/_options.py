from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Options:
    """
    Options dataclass.

    Args:
        log_dir (Path): The Path/directory of the output log
            (The presence/absence of this value enables/disables logging)
        register (bool): Whether or not to add to datasources registry
            (False disables the datasource from use in BoltETL tools)
        ...
        kwargs (dict): Misc key-value pairs to add to the dataclass

    """
    log_dir: Path|None = None
    register: bool = True,
    # TODO: more?
    kwargs: field(default_factory=dict) = None  # type: ignore

    def __post_init__(self):
        if self.kwargs:
            [setattr(self, k, v) for k, v in self.kwargs.items()]
