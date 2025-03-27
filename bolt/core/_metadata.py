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
    kwargs: field(default_factory=dict) = None  # type: ignore

    def __post_init__(self):
        self._sources_hash = None
        if self.tags:
            for tag in self.tags:
                if not tag.startswith("#"):
                    self.tags.remove(tag)
                    self.tags.add(f"#{tag}")

        if self.kwargs:
            [setattr(self, k, v) for k, v in self.kwargs.items()]

    @property
    def sources_hash(self) -> str:
        """Gets a hash of the source files."""
        if self._sources_hash:
            return self._sources_hash
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
        self._sources_hash = (
            sha256("".join(hashes).encode("UTF8")).hexdigest()[:7]  # NOTE: git uses 7
        )
        return self._sources_hash

    def to_json(self, json_indent: int = 0):
        d: dict = {}
        for k, v in self.__dict__.items():
            if k in {
                "datasource",
                "options",
                "schema",
                "sources_hash",
                "_sources_hash",
            }:
                continue
            if type(v) in {set, tuple}:
                v = list(v)
            elif type(v) is dict:
                v = json.dumps(v)
            try:
                d[k] = v
            except TypeError:
                d[k] = "ERROR"
        j: str = json.dumps(d, indent=json_indent)
        return j

    def _insert(self, warehouse):
        meta: str = self.to_json()
        meta_hash = sha256(meta.encode("UTF8")).hexdigest()[:7]
        s = f"INSERT OR REPLACE INTO bolt_metadata VALUES ('{self.datasource.name}', TODAY(), '{self.sources_hash}', '{meta_hash}', '{meta}');"
        try:
            with warehouse.connect() as con:
                con.sql(s)
            return True
        except Exception as e:
            # TODO: pre-raise handling
            raise e

    def __repr__(self):
        if self.datasource:
            return f"<Metadata(datasource='{self.datasource.name}')>"
        return "<Metadata(datasource=None)>"
