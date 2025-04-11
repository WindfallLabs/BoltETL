"""DuckDB Data Warehouse"""

from functools import wraps
from graphlib import TopologicalSorter
from logging import Logger
from pathlib import Path
from typing import Any, Callable, Generator, Literal
from warnings import warn

import duckdb
import polars as pl

from .._config import Config
from ..utils import IOLogger, make_logger
from ._datasource import Datasource
from ._report import Report
from ._sql import SQL

init_scripts = [
    """
    CREATE TABLE IF NOT EXISTS bolt_metadata (
        table_name VARCHAR PRIMARY KEY,
        last_updated TIMESTAMP,
        sources_hash VARCHAR(7),
        metadata_hash VARCHAR(7),
        metadata VARCHAR
    );
    """,
]


class Warehouse[T]:
    """Bolt's default data warehouse (DuckDB)."""

    _ENV_LOADED = False
    # Registries went here...

    def __init__(
        self,
        path: Path,
        script_directory: Path | None = None,
        ignored_dependencies: set[str] | None = None,
        extensions: set[str] | None = None,
        create_on_init=False,
        hide_duckdb_progress_bar=True,
        duckdb_sql_return_method: str | None = "pl",
    ):
        """Warehouse class."""
        self._path = path
        self._script_directory = script_directory
        self.ignored_dependencies = ignored_dependencies or set()
        self.extensions = extensions or set()
        self.hide_progress_bar = hide_duckdb_progress_bar
        self.duckdb_sql_return_method = duckdb_sql_return_method

        # Internal
        # Logging setup
        self.logger: Logger | IOLogger = make_logger("warehouse", Config.log_dir)
        self._is_new = not self.path.exists()  # BUG: can't use :memory:
        self._sql_functions: set[Callable] = set()
        self._scripts: list[str] = []

        if create_on_init:
            with self.connect() as con:
                con.sql("SELECT 1;")

        self.load_scripts_from_dir()  # TODO: Good and necessary?

    # ========================================================================
    # Properties

    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self._path.name

    @property
    def script_directory(self):
        return self._script_directory

    @property
    def datasource_registry(self):
        return Datasource.registry

    @property
    def report_registry(self):
        return {
            rpt_name: rpt_obj._set_warehouse(self)
            for rpt_name, rpt_obj in Report.registry.items()
            if rpt_obj.options.register
        }

    @property
    def sql_registry(self):
        return {
            script_name: sql_obj._set_warehouse(self)
            for script_name, sql_obj in SQL.registry.items()
            if sql_obj.options.register
        }

    # ========================================================================
    # Decorators

    '''
    def on_init(self):
        """Wraps a callback function to execute on db init."""
        pass  # TODO: ...

    def on_connect(self):
        """Wraps a callback function to execute on connect."""
        pass  # TODO: ...
    '''

    def sql_function(self, sql_func: Callable) -> None:
        """Wraps a function to load as an SQL function."""

        @wraps(sql_func)
        def _sql_function_wrapper(*args, **kwargs):
            self._sql_functions.add(sql_func)
            return

        _sql_function_wrapper()
        return

    def script_wrapper(self, script_func: Callable):
        """Wraps a function that returns executable SQL.

        Args:
            execute_now (bool): Executes the SQL at definition rather than
                on `warehouse.build()`.

        Returns (str): The SQL to execute
        """

        @wraps(script_func)
        def _script_wrapper(*args, **kwargs):
            script_name = script_func.__name__
            sql_obj = SQL(script_name)
            sql_obj._sql = script_func(self)
            return

        return _script_wrapper()

    # ========================================================================
    # Public Methods

    def connect(self, as_context=True):
        """Connect to the DuckDB data warehouse.

        Args:
            return_context (bool, default True): Allows this function to
                either return a duckdb.DuckDBPyConnection or control it as
                a context.

            Example:
            >>> with warehouse.connect() as con:
            >>>     con.sql("SELECT 42;")

            >>> con = warehouse.connect()
            >>> con.sql("SELECT 42;")
        """
        con = duckdb.connect(self.path)

        # Hide progress bars
        if self.hide_progress_bar:
            con.execute("PRAGMA disable_progress_bar;")

        # Install and load extensions
        for ext in self.extensions:
            if self._is_new:
                con.install_extension(ext)
            con.load_extension(ext)

        # Register SQL functions
        for fn in self._sql_functions:
            try:
                con.remove_function(fn.__name__)
            except Exception:
                pass
            try:
                con.create_function(fn.__name__, fn)
            except duckdb.CatalogException:
                # warn(f"Skipped loading function : `{fn.__name__}`")
                pass

        for init_script in init_scripts:
            con.sql(init_script)

        class _DuckDbCtx:
            def __init__(self):
                self._connection = con

            def __enter__(self):
                return self._connection

            def __exit__(self, exc_type, exc_val, exc_tb):
                self._connection.close()

            def close(self):
                warn("No need to close connection when using `as_context`")

            def sql(self, *args, **kwargs):
                raise AttributeError(
                    "Connection object has no attribute 'sql'; "
                    "did you forget to set `use_context=False`?"
                )

        if as_context:
            return _DuckDbCtx()
        return con

    def compact(self) -> tuple[float, float]:
        """Makes a new, compacted copy of the DuckDB file.

        Helps resolve file size increase issue found here:
        https://github.com/duckdb/duckdb/issues/9429
        From best practices found here:
        https://duckdb.org/docs/operations_manual/footprint_of_duckdb/reclaiming_space.html
        """
        name = str(self.path.name)
        old_size = self.path.stat().st_size / 1024
        new_db = Path(str(self.path).replace(name, "_compacting.duckdb"))
        duckdb.sql(f"ATTACH '{self.path}' AS db1;")
        duckdb.sql(f"ATTACH '{new_db}' AS db2;")
        duckdb.sql("COPY FROM DATABASE db1 TO db2;")
        duckdb.close()
        # Delete the old db
        self.path.unlink()
        # Reset the warehouse's path to the new/compacted db
        self._path = new_db.rename(new_db.parent.joinpath(name))  # TODO: should work
        new_size = self.path.stat().st_size / 1024
        return (old_size, new_size)

    def create_dependency_graph(self, small=False) -> tuple[str] | list[tuple[str, str, set]]:
        """Sorts registered SQL objects by dependency requirements."""
        if not getattr(self, "_ENV_LOADED", False):
            warn("User-defined objects not loaded. Use `import boltetl.env` to resolve.")

        default_duckdb_tables = {"duckdb_tables", "duckdb_views"}
        script_dependencies = {}
        for name, obj in self.sql_registry.items():
            clean_deps = set()
            deps: set[str] = {d for d in obj.dependencies if d not in self.ignored_dependencies}
            for dep in deps:
                if dep in self.ignored_dependencies:
                    continue
                elif dep in self.datasource_registry.keys():
                    continue  # TODO: update dependent Datasources if bolt_cli not called with "."
                elif dep in default_duckdb_tables:  # TODO:
                    continue
                clean_deps.add(dep)
            script_dependencies[name] = clean_deps
        sorter = TopologicalSorter(script_dependencies)
        sorted_graph: tuple[str] = tuple(sorter.static_order())
        if small:
            return sorted_graph

        full_graph: list[tuple[str, str, set[str]]] = []
        g: str
        for g in sorted_graph:
            # Get from SQL registry or Datasource registry
            obj = self.sql_registry.get(g, self.datasource_registry.get(g, None))
            deps = getattr(obj, "dependencies", set())
            t: str
            if obj is None:
                t = "MISSING"
            else:
                t = obj.__class__.__name__
            full_graph.append((g, t, deps))
        return full_graph

    def execution_plan(self) -> Generator[T, None, None]:
        """Generates SQL scripts sorted by dependency requirements."""
        import boltetl.env

        boltetl.env.datasources.load_all()
        graph: list[tuple[str, str, set]] = self.create_dependency_graph()

        for dep_name, dep_type, deps in graph:
            # Raise error on missing dependencies
            if dep_type == "MISSING":
                raise KeyError(
                    f"Dependency '{dep_name}' is not defined"
                )  # TODO: should we raise here or let it roll?
            obj = self.sql_registry[dep_name]
            yield obj

    def create_schema_table(
        self,
        dialect: Literal["duckdb", "arrow", "polars", "pandas", "numpy"] = "duckdb",
        ignored_tables: list[str] | None = None,
        schema_table_name: str = "table_schemas",
    ):
        """Creates a table that describes the schemas of all non-internal tables/views."""
        tbls: list[str] = self.list_tables()
        ignored_tables = ignored_tables or []
        rows = []
        with self.connect() as con:
            con.sql(f"""
                CREATE OR REPLACE TABLE {schema_table_name} (
                    datasource VARCHAR,
                    columns VARCHAR,
                    dtype VARCHAR
                );
            """)

        for tbl in tbls:
            if tbl in ignored_tables:
                continue
            tbl_rows = self.get_table_schema(tbl, dialect=dialect)
            if tbl_rows:
                rows.extend(tbl_rows)

        with self.connect() as con:
            con.executemany(f"INSERT INTO {schema_table_name} VALUES (?, ?, ?);", rows)
        return

    def execute_script(self, script_name):
        """Execute a registered SQL script by name."""
        script = self.sql_registry[script_name]
        with self.connect() as con:
            r = con.sql(script.sql).pl()
        return r

    def get_data(self, table_name) -> pl.DataFrame:
        """Return a table from the warehouse by name."""
        if table_name not in self.list_tables():
            raise KeyError(f"'{table_name}' table/view does not exist")  # TODO: exception type
        with self.connect() as con:
            try:
                df = con.sql(f"SELECT * FROM {table_name};").pl()
            except pl.exceptions.ComputeError:
                df = con.sql(f"SELECT * FROM {table_name};").df()
                df = pl.from_pandas(df)
        return df

    def get_table_schema(  # type: ignore[return]
        self,
        table: str,
        dialect: Literal["duckdb", "arrow", "polars", "pandas", "numpy"] = "polars",
    ) -> list[tuple[str, str, str]]:
        """Get the schema for a given table.

        Args:
            table (str): The name of the table
            dialect (str): Return types for the given dialect
        """
        dialect = dialect.lower()
        if dialect != "duckdb":
            sql = f"SELECT * FROM {table} LIMIT 1;"
            with self.connect() as con:
                tbl: duckdb.DuckDBPyRelation = con.sql(sql)

            # Arrow
            if dialect == "arrow":
                from pyarrow.lib import (  # type: ignore[import-untyped]
                    Schema as PyArrowSchema,
                )

                df_sch: PyArrowSchema = tbl.to_arrow_table().schema
                schema = list(
                    zip(
                        (table,) * len(df_sch.names),
                        df_sch.names,
                        [str(i) for i in df_sch.types],
                    )
                )
                return schema

            # Polars
            elif dialect == "polars":
                df: pl.DataFrame = tbl.pl()  # type: ignore[no-redef]
                schema = list(
                    zip(
                        (table,) * len(df.columns),
                        df.columns,
                        [str(i) for i in df.dtypes],
                    )
                )
                return schema

            # Numpy / Pandas
            elif dialect == "numpy" or dialect == "pandas":
                import pandas as pd  # type: ignore[import-untyped]

                with self.connect() as con:
                    df: pd.DataFrame = tbl.df().dtypes.reset_index()  # type: ignore[no-redef]
                schema = list(zip((table,) * len(df), df["index"], [i.name for i in df[0]]))
                return schema

        # DuckDB
        sql = f"SELECT column_name, column_type FROM (DESCRIBE {table});"
        with self.connect() as con:
            df: pl.DataFrame = con.sql(sql).pl()  # type: ignore[no-redef]
        schema = list((table,) + tuple(i.values()) for i in df.to_dicts())

    def compare_hashes(self, datasource):
        """Compares the hashes of the parent datasource to determine if updated recently."""
        # TODO: hash the datasource / python file
        do_update = True
        db = self.connect(False)
        try:
            if datasource.source_files:
                datasource.logger.info("Calculating hash")
                current_hash = datasource.metadata.sources_hash
                # Ignore update for datasources with no changes to the source files
                # Get the last hash (sha256) of the source files
                db_hash = db.sql(
                    f"SELECT sources_hash FROM bolt_metadata WHERE table_name = '{datasource.name}'"
                ).pl()["sources_hash"]
                # Compare hashes and skip if they are the same
                if not db_hash.is_empty() and current_hash == db_hash.item():
                    do_update = False
                    datasource.logger.debug(f"Database hash (source files): {db_hash.item()}")
                    datasource.logger.debug(f"Current hash (source files): {current_hash}")
        except Exception as e:
            datasource.logger.error(f"Comparing hashes failed: {e}")
        finally:
            db.close()

        are = "are" if not do_update else "are not"
        datasource.logger.info(f"Hashes {are} equal")
        return do_update

    def list_tables(self, include_views=True) -> list[str]:
        # BUG: include_views=False throws error:
        # BinderException: Binder Error: Referenced column "type" not found in FROM clause!
        create_sql = """
        CREATE VIEW IF NOT EXISTS view_all_tables AS
        (
            SELECT
                table_name AS "name",
                'table' AS "type"
            FROM duckdb_tables
        )
        UNION
        (
            SELECT
                view_name AS "name",
                'view' AS "type"
            FROM duckdb_views
        )
        ORDER BY "name";
        """
        if include_views:
            query = """
            SELECT name
            FROM view_all_tables;
            """
        else:
            query = """
            SELECT name
            FROM view_all_tables
            WHERE type = 'table';
            """
        with self.connect() as con:
            con.sql(create_sql)
            table_list = con.sql(query).pl()["name"].to_list()
        return table_list

    def load_dataframe(self, table_name: str, df: Any):
        """Load a DataFrame with DuckDB."""
        with self.connect() as con:
            con.sql(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        return

    def load_scripts_from_dir(self) -> None:  # Good and necessary?
        """."""
        if not self.script_directory:
            return
        for script_path in self.script_directory.glob("[!_]*.sql"):
            _ = SQL(path=script_path)  # initialization registers the objects
        return

    def rebuild(self, destroy=False, compact=True):
        """Execute SQL scripts against the warehouse."""
        # Deletes the DuckDB file if `destroy==True`
        # TODO: Execute all necessary Datasource ETL pipelines (remove from bolt_cli.py?)
        sql_file_count = 0
        with self.connect() as con:
            for sql_obj in self.execution_plan():
                # print(sql_obj.path)
                try:
                    con.sql(sql_obj.sql)
                except Exception as e:  # TODO: binder error?
                    # e.add_note(sql_obj.name)  # TODO: print sql file name
                    p = getattr(sql_obj, "path", sql_obj.name)
                    e.args = (f"{e.args[0]} --> {p}", *e.args[1:])
                    raise e
                sql_file_count += 1
        return sql_file_count

        # ...
        # Compact
        compact_msg = "[yellow]Not compacted[/]"
        if compact:
            compact_sizes = self.compact()
            compact_msg = f"Compacted Database: {compact_sizes[0]} KB -> {compact_sizes[1]} KB"
        return sql_file_count, compact_msg  # TODO: ...

    # ========================================================================
    # Dunders

    def __repr__(self):
        return f"<Warehouse(path='{str(self._path)}')>"
