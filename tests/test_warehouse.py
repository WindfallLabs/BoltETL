from pathlib import Path

import pytest

from bolt.core._sql import SQL
from bolt.core._warehouse import Warehouse

WH_PATH = Path(__file__).parent / "artifacts" / "test_warehouse.duckdb"


@pytest.fixture
def warehouse():
    warehouse = Warehouse(WH_PATH)
    yield warehouse
    if WH_PATH.exists():
        WH_PATH.unlink()
    warehouse.sql_registry.clear()
    SQL.registry.clear()
    return


def test_warehouse_create_on_init():
    warehouse = Warehouse(path=WH_PATH, create_on_init=True)
    assert warehouse.path.exists() is True
    warehouse.sql_registry.clear()
    if WH_PATH.exists():
        WH_PATH.unlink()


def test_warehouse_init(warehouse):
    assert warehouse.path.exists() is False
    with warehouse.connect() as con:
        con.sql("SELECT 42;")
    assert warehouse.path.exists() is True


def test_connection(warehouse):
    with warehouse.connect() as con:
        ctx_val = con.sql("SELECT 42 AS value").pl()["value"].item()
    assert ctx_val == 42
    con = warehouse.connect(False)
    return_val = con.sql("SELECT 42 AS value").pl()["value"].item()
    assert return_val == 42
    con.close()


def test_extensions(warehouse):
    spatial_ext_sql = """
        SELECT
            extension_name,
            loaded,
            installed
        FROM duckdb_extensions()
        WHERE extension_name = 'spatial'
    """
    with warehouse.connect() as con:
        ext_not_loaded = not con.sql(spatial_ext_sql).pl()["loaded"].item()
    assert ext_not_loaded is True

    warehouse.extensions.add("spatial")
    with warehouse.connect() as con:
        df = con.sql(spatial_ext_sql).pl()
        ext_installed = df["installed"].item()
        ext_loaded = df["loaded"].item()
    assert ext_installed is True
    assert ext_loaded is True


def test_script_wrapper(warehouse):
    @warehouse.script_wrapper
    def view_all_tables(obj):
        s = """
            CREATE OR REPLACE VIEW view_all_tables AS
            (
                SELECT
                    table_name AS "name"
                FROM duckdb_tables
            )
            UNION
            (
                SELECT
                    view_name AS "name"
                FROM duckdb_views
            )
            ORDER BY "name";
        """
        return s

    assert "view_all_tables" in warehouse.sql_registry.keys()


def test_sql_function(warehouse):
    @warehouse.sql_function
    def fiscal_year(year_month: int | str) -> str:
        """Calculate a US Federal fiscal year from a YearMonth."""
        year = int(str(year_month)[:4])
        month = int(str(year_month)[4:])

        if month >= 7:
            return f"FY{str(year + 1)[2:4]}"
        return f"FY{str(year)[2:4]}"

    with warehouse.connect() as con:
        cy24 = con.sql("SELECT fiscal_year(202408) AS FY").pl()
        fy25 = con.sql("SELECT fiscal_year(202503) AS FY").pl()
    assert fy25["FY"].item() == "FY25"
    assert cy24["FY"].item() == "FY25"


def test_create_depenedency_graph(warehouse):
    @warehouse.script_wrapper
    def view_test(*args, **kwargs):
        return (
            "CREATE VIEW view_test AS SELECT * FROM table1 UNION SELECT * FROM table2"
        )

    @warehouse.script_wrapper
    def view_another(*args, **kwargs):
        return "CREATE VIEW view_another AS SELECT * FROM view_test"

    @warehouse.script_wrapper
    def view_ddb_dep(*args, **kwargs):
        return "CREATE VIEW view_ddb_dep AS SELECT * FROM duckdb_tables"

    small_graph = warehouse.create_dependency_graph(True)
    # assert "view_test" in small_graph
    assert "view_another" in small_graph
    # DuckDB tables
    assert "duckdb_tables" not in small_graph

    full_graph = warehouse.create_dependency_graph()
    graph = {i[0]: i[2] for i in full_graph}
    # assert "view_test" in graph
    assert "view_another" in graph
    assert graph["view_test"] == {"table1", "table2"}
    assert graph["view_another"] == {"view_test"}
