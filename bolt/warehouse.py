"""Functions for the DuckDB Warehouse."""

from pathlib import Path

import duckdb

from bolt.utils import config, funcs

DB_PATH = config.data_dir.joinpath("data_warehouse.duckdb")

SQL_FUNCS = [
    funcs.fiscal_year,
]

SQL_FILES: list[Path] = config.sql_dir.rglob("[!_]*.sql")


def load_funcs(con: duckdb.DuckDBPyConnection) -> None:
    for fn in SQL_FUNCS:
        try:
            con.remove_function(fn.__name__)
        except duckdb.InvalidInputException:
            pass
        con.create_function(fn.__name__, fn)  # TODO: might throw a CatalogException
    return


def connect() -> duckdb.DuckDBPyConnection:
    """Connect to the DuckDB data warehouse."""
    con = duckdb.connect(DB_PATH)
    load_funcs(con)
    return con


def compact() -> tuple[str]:
    """Makes a compacted copy of the DuckDB Data Warehouse.

    Helps resolve file size increase issue found here:
    https://github.com/duckdb/duckdb/issues/9429
    From best practices found here:
    https://duckdb.org/docs/operations_manual/footprint_of_duckdb/reclaiming_space.html
    """
    name = str(DB_PATH.name)
    old_size = DB_PATH.stat().st_size / 1024
    new_db = Path(str(DB_PATH).replace(DB_PATH.name, "_compacting.duckdb"))
    duckdb.sql(f"ATTACH '{DB_PATH}' AS db1;")
    duckdb.sql(f"ATTACH '{new_db}' AS db2;")
    duckdb.sql("COPY FROM DATABASE db1 TO db2;")
    duckdb.close()
    DB_PATH.unlink()
    new_db.rename(new_db.parent.joinpath(name))
    new_size = DB_PATH.stat().st_size / 1024
    return f"[white]Compacted ({old_size:,.0f} KB to {new_size:,.0f} KB[/])"


def update_sql(compact_db=False) -> tuple[int, str]:
    """Update the DuckDB data warehouse."""
    # Execute SQL from files
    sql_file_count = 0
    for sql_file in SQL_FILES:
        with sql_file.open() as f:
            sql = f.read()
            with duckdb.connect(DB_PATH) as con:
                con.sql(sql)
        sql_file_count += 1
    # Compact DB
    compact_msg = ""
    if compact_db:
        compact_msg = compact()
    return (sql_file_count, compact_msg)
