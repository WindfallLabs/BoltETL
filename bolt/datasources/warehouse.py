"""Functions for the DuckDB Warehouse."""

from pathlib import Path

import duckdb
import pandas as pd

from bolt.utils import config, funcs

DB_PATH = config.data_dir.joinpath("data_warehouse.duckdb")

SQL_FUNCS = [
    funcs.fiscal_year,
]

SQL_FILES: list[Path] = list(Path(__file__).parent.joinpath("sql").glob("*.sql"))


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
    # print(f"  Compacted ({old_size:,.0f} KB to {new_size:,.0f} KB)")
    return f"Compacted ({old_size:,.0f} KB to {new_size:,.0f} KB)"


def load_cache_files(compact_db=False) -> None:
    """Loads feather files into the DuckDB Data Warehouse."""
    with duckdb.connect(DB_PATH) as con:
        tables_loaded = 0
        for i in config.cache_dir.rglob("*.feather"):
            tbl_name = Path(i).name.split(".")[0]
            # Load the feather file as a dataframe
            df = pd.read_feather(i, dtype_backend="pyarrow")  # noqa: F841
            # Load the dataframe into DuckDB
            con.sql(
                f"CREATE OR REPLACE TABLE {tbl_name} AS SELECT * FROM df"
            )  # reads from Python variables I guess?
            tables_loaded += 1

    # print(f"  Loaded tables: {tables_loaded}")
    return tables_loaded


def execute_sql():
    sql_file_count = 0
    for sql_file in SQL_FILES:
        # print(sql_file)  # DEBUG
        with sql_file.open() as f:
            sql = f.read()
            with duckdb.connect(DB_PATH) as con:
                con.sql(sql)
        sql_file_count += 1
    return sql_file_count


def update_db(compact_db=False) -> tuple[int, str]:
    """Update the DuckDB data warehouse."""
    # Load data from cached files
    tables_loaded = load_cache_files()
    # Load misc dataframes
    # misc_loaded = load_misc_dataframes()  # TODO: implement
    # Execute SQL from files
    sql_file_count = execute_sql()
    compact_msg = ""
    if compact_db:
        compact_msg = compact()
    return (tables_loaded, sql_file_count, compact_msg)
