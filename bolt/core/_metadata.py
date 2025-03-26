"""Metadat object."""

import json
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path


@dataclass  # TODO: if this used pydantic instead, we could dump as JSON (to database)
class Metadata:
    """
    Metadata dataclass.

    Args (all are optional):
        description (str): String description of the datasource
        tags (set[str]): Tags
        vendor (str): Name of software vendor
        software (str): Name of source software
        ...
        kwargs (dict): Misc key-value pairs to add to the dataclass
    """

    description: str | None = None
    data_dict: dict[str, str] | None = None
    tags: set[str] | None = None
    vendor: str | None = None
    software: str | None = None
    source_url: str | None = None
    # TODO: api: str|None = None
    # TODO: database_uri: str|None = None
    # TODO: documentation path?
    schema: list[tuple[str, type]] | None = None  # TODO: final vs prelim?
    datasource = None
    sources_hash = None
    kwargs: field(default_factory=dict) = None  # type: ignore

    def __post_init__(self):
        if self.tags:
            for tag in self.tags:
                if not tag.startswith("#"):
                    self.tags.remove(tag)
                    self.tags.add(f"#{tag}")

        # self.sources_hash = hash_sources()
        if self.kwargs:
            [setattr(self, k, v) for k, v in self.kwargs.items()]

    def hash_sources(self) -> str:
        """Gets a hash of the source files."""
        hashes: list[str] = []
        try:
            for p in self.datasource.source_files:
                p: Path = Path(p)
                if p.is_dir():
                    # raise AttributeError("TODO: Hash cannot be performed on folder")
                    continue
                with p.open("rb") as f:
                    hashes.append(sha256(f.read()).hexdigest())
        except Exception:
            raise AttributeError(
                f"TODO: Hash cannot be performed on {self.datasource.name}"
            )
        self.sources_hash = sha256("".join(hashes).encode("UTF8")).hexdigest()[
            :7
        ]  # NOTE: git uses 7
        return self.sources_hash

    def to_json(self) -> str:
        string_dict: dict[str, str] = {}
        for k, v in self.__dict__.items():
            string_dict[str(k)] = str(v)
        return json.dumps(string_dict, indent=4)

    def __repr__(self):
        if self.datasource:
            return f"<Metadata(datasource='{self.datasource.name}')>"
        return f"<Metadata(datasource=None)>"
