"""Database tables and views."""

import re
from pathlib import Path

from bolt.core._options import Options

REGEX = re.compile(r"(?:FROM|JOIN|UPDATE|INSERT INTO|PIVOT) (\b\w+\b)")
# IGNORE_REGEX = re.compile(r"(?:WITH) (\b\w+\b)")
IGNORE_REGEX = re.compile(r"(\w+)\s+?\bAS\b")


class SQL[T]:
    registry: dict[str, T] = dict()
    failed_to_load: set[tuple[str, Exception]] = set()

    def __init__(
        self,
        name: str | None = None,
        ttype: str | None = None,  # TODO: require? useful?
        path: Path | None = None,
        ignore_dependencies: set[str] | None = None,
        options=None,
    ):
        if not name:
            if path:
                name = path.stem
            # elif self.__name__  # TODO: ...
            else:
                raise AttributeError("`name` parameter required for SQL object")
        self._name = name
        self.ttype = ttype
        self._path = path
        self.ignore_dependencies = ignore_dependencies or set()
        self.options = options if options else Options()

        # Defaults
        self._sql: str | None = None
        if self._path:
            self._sql = self._path.open().read()

        self.warehouse = None

        # Finalize
        self.registry[self.name] = self

    @property
    def name(self):
        return self._name

    @property
    def path(self):
        return self._path

    @property
    def sql(self):
        if self.path:
            content = self.path.open().read()
        else:
            content = self._sql
        return content

    @property
    def dependencies(self) -> set[str]:
        sql = self.sql
        if not sql:
            return set()
        # Remove comments
        clean_sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        clean_sql = re.sub(r"^\s*--.*$", "", clean_sql, flags=re.MULTILINE)
        deps = {
            dep
            for dep in REGEX.findall(clean_sql)
            if dep not in IGNORE_REGEX.findall(clean_sql)
            and dep not in self.ignore_dependencies
        }
        return deps

    def _set_warehouse(self, warehouse) -> T:
        """Sets the warehouse attribute."""
        self.warehouse = warehouse
        return self

    def __repr__(self):
        return f"<SQL(name='{self._name}')>"
