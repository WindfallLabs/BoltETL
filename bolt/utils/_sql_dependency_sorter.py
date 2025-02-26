"""Sort SQL file dependencies."""
import re
from graphlib import TopologicalSorter
from pathlib import Path

sql_dir = Path(r"C:\Workspace\tmpdb\Data\__bolt__\sql")  # TODO: use config
sql_files = list(sql_dir.glob("*.sql"))

regex = re.compile(r"(?:FROM|JOIN|UPDATE|INSERT INTO|PIVOT) (\b\w+(?:_\w+)*\b)")


def parse_sql_file(file: Path):
    """Opens, cleans, and parses an SQL file for required tables/views."""
    fname = file.name.split(".")[0]
    with file.open() as f:
        sql = f.read()
    # Remove comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"^\s*--.*$", "", sql, flags=re.MULTILINE)
    return (
        fname,
        {
            dep for dep in regex.findall(sql)
            if dep.lower() not in fname.lower()
        }
    )


def get_sql_file_dependencies():
    """Sorts SQL files based on dependencies."""
    d = dict(map(parse_sql_file, sql_files))
    dependents = TopologicalSorter(d).static_order()
    dependent_files = tuple(
        f"{i.lower()}.sql" for i in dependents
        if sql_dir.joinpath(f"{i.lower()}.sql").exists()
    )
    return dependent_files


if __name__ == "__main__":
    print(get_sql_file_dependencies())
