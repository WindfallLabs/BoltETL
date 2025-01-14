"""Functions for the DuckDB Warehouse."""
from pathlib import Path

import duckdb
import pandas as pd

from bolt.utils import config


DB_PATH = config.data_dir.joinpath("data_warehouse.duckdb")


def connect():
    """Connect to the DuckDB data warehouse."""
    return duckdb.connect(DB_PATH)


def compact():
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
    print(f"  Compacted ({old_size:,.0f} KB to {new_size:,.0f} KB)")
    return


def load_cache_files(compact_db=False):
    """Loads feather files into the DuckDB Data Warehouse."""
    db = duckdb.connect(DB_PATH)
    for i in config.cache_dir.rglob("*.feather"):
        tbl_name = Path(i).name.split(".")[0]
        # Load the feather file as a dataframe
        df = pd.read_feather(i, dtype_backend="pyarrow")
        # Load the dataframe into DuckDB
        db.sql(f"CREATE OR REPLACE TABLE {tbl_name} AS SELECT * FROM df")  # reads from Python variables I guess?
    for i in config.sql_dir.rglob("*.sql"):
        sql_file = Path(i).absolute()
        with sql_file.open() as f:
            db.sql(f.read())

    loaded_tables = db.sql("SELECT table_name FROM duckdb_tables").df().count().item()
    print(f"  Loaded tables: {loaded_tables}")
    db.close()
    if compact_db:
        compact()
    return
