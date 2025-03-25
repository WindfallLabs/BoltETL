from bolt.core._sql import IGNORE_REGEX, REGEX, SQL

# @pytest.fixture
# def cleanup():
#     SQL.registry.clear()


def test_regex():
    assert REGEX.findall("FROM mytbl GROUP BY")[0] == "mytbl"
    assert REGEX.findall("FROM xyz_MyTable42 GROUP BY")[0] == "xyz_MyTable42"
    assert (
        REGEX.findall("FROM xyz_MyTable42_final_final_superdelux GROUP BY")[0]
        == "xyz_MyTable42_final_final_superdelux"
    )
    multi = """
        SELECT *
        FROM table_in_multiline_statement
        GROUP BY some_field
    """
    assert REGEX.findall(multi)[0] == "table_in_multiline_statement"

    # Ignores (CTE / with)
    assert IGNORE_REGEX.findall("WITH first_cte AS ...\n), second_cte AS ...") == [
        "first_cte",
        "second_cte",
    ]


def test_sql():
    sql_obj = SQL(name="test_sql_table")
    sql_obj._sql = "CREATE OR REPLACE TABLE test_sql_table (id VARCHAR);"
    assert sql_obj.dependencies == set()
    assert "test_sql_table" in SQL.registry
    SQL.registry.clear()
