import datetime as dt
import time
from pathlib import Path
from typing import Literal

import cyclopts
import geopandas as gpd
import pandas as pd
import polars as pl
from rich.console import Console

import bolt
import bolt.warehouse

__version__ = "0.2.0-dev"
t_start = time.time()  # Start process timer

console = Console()
app = cyclopts.App()

# tracker = FileTracker()

logo = """┏━━┓━━━━━┏┓━━┏┓━┏━━━┓┏━━━━┓┏┓━━━
┃┏┓┃━━━━━┃┃━┏┛┗┓┃┏━━┛┃┏┓┏┓┃┃┃━━━
┃┗┛┗┓┏━━┓┃┃━┗┓┏┛┃┗━━┓┗┛┃┃┗┛┃┃━━━
┃┏━┓┃┃┏┓┃┃┃━━┃┃━┃┏━━┛━━┃┃━━┃┃━┏┓
┃┗━┛┃┃┗┛┃┃┗┓━┃┗┓┃┗━━┓━┏┛┗┓━┃┗━┛┃
┗━━━┛┗━━┛┗━┛━┗━┛┗━━━┛━┗━━┛━┗━━━┛"""

SCRIPT = Path(__file__).name

# Populate a list of custom Datasource objects
DATASOURCES: list[bolt.datasources.Datasource] = []
for ds in bolt.config.metadata.keys():
    DS = getattr(bolt.datasources, ds, None)
    if DS and issubclass(DS, bolt.datasources.Datasource):
        DATASOURCES.append(DS)

# Populate a list of custom Report objects
REPORTS: list[bolt.reports.BaseReport] = []
for _obj_name in bolt.reports.__dir__():
    RPT = getattr(bolt.reports, _obj_name, None)
    if type(RPT).__name__ in ("str", "module"):
        continue
    if not hasattr(RPT, "__name__") or not hasattr(RPT, "__class__"):
        continue
    if RPT.__name__ == "BaseReport":
        continue
    if issubclass(RPT, bolt.reports.BaseReport):
        REPORTS.append(RPT)

# TODO: fix and re-implement
'''
@app.command
def add(datafile: str):
    """Add data files to tracked data."""
    # Get a dict of changed files by change type/status
    changes: dict = tracker.get_changes()
    # Get a list of all changed files
    all_changed_files: list[str] = []
    all_changed_files.extend(changes["new_files"])
    all_changed_files.extend(changes["modified"])
    all_changed_files.extend(changes["removed"])

    # Filter changed files by filename, or use "." for all
    files_to_add: list[str] = [p for p in all_changed_files if fnmatch(p, datafile)]
    if datafile == ".":
        files_to_add = all_changed_files.copy()

    # No changes to update
    if len(files_to_add) == 0:
        console.print("\n[green]Nothing to add.[/]")
        return

    # Pass a list of all filenames to add
    tracker.commit_changes(files_to_add)
    for file in files_to_add:
        console.print(f"        [green]{'added:'.ljust(11)} {file}[/]")
    return


@app.command
def status():
    changes: dict = tracker.get_changes()
    console.print(
        "Changes to data files:"
    )  # TODO: always prints, even if status is 'No changes'
    console.print(f'  (use "python {SCRIPT} add <file>..." to register new files)')

    # Print changes
    if changes["new_files"]:
        status = "new file:"
        for file in sorted(changes["new_files"]):
            console.print(f"        [red]{status.ljust(11)} {file}[/]")

    if changes["modified"]:
        status = "modified:"
        for file in sorted(changes["modified"]):
            console.print(f"        [red]{status.ljust(11)} {file}[/]")

    if changes["removed"]:
        status = "removed:"
        for file in sorted(changes["removed"]):
            console.print(f"        [red]{status.ljust(11)} {file}[/]")

    if not any(changes.values()):
        console.print("\n[green]No changes detected.[/]")

    return
'''


@app.command
def report(
    option: Literal["list", "info", "run"], rpt_name: str = "", *args, **kwargs
):  # TODO: write: bool = True?
    """Execute a report by report class name with kwargs.
    Use `report-info <report name>` for details about a report.

        Example
        -------
        `python bolt-cmd.py report run ParatransitNoShows --start=20250101 --end=20250131`
    """
    # NOTE: list option does not require 'rpt_name'
    if option == "list":
        console.print("Available Reports:")
        for rpt in REPORTS:
            console.print(f"        [green]{rpt.__name__}[/]")
        console.print("For more info, use: ")
        console.print("[b blue]`python bolt-cmd.py report info <report name>`[/]")
        return

    # TODO: consider an '--update' flag to update report dependencies
    # e.g. python bolt-cmd.py report run ParatransitNoShows --update

    if not rpt_name:
        raise AttributeError("'rpt_name' argument is required")

    Rpt = getattr(bolt.reports, rpt_name)  # Get python class by name
    rpt = Rpt()
    if option == "info":
        console.print(f"[green]{rpt.name}[/]")
        console.print(f"[yellow]{rpt.run.__doc__}[/]")
        return

    if option == "run":
        console.print(f"Running report: {rpt_name}...")
        console.print(f"        (args={args})")
        console.print(f"        (kwargs={kwargs})")
        # console.print("        ...", end="\r")
        try:
            rpt.run(*args, **kwargs)
            if rpt._exported:
                console.print(f"        Exported results to '{rpt.out_path}'")
        except Exception as e:
            console.print(f"        [red]Failed: {e}")
    return


# TODO: placeholder
'''
@app.command
def task(
    option: Literal["list", "add", "remove"], name: str = "", when: str = ""
) -> None:
    """Task-control (WIP).
    Schedule recurring data tasks ...or something.
    """
    if option == "list":
        console.print("Tasks List:")
        console.print("        Task1  # WIP")  # TODO: dev
    elif option == "add":
        console.print(f"Added: {name}: {when}")  # TODO: dev
    elif option == "remove":
        console.print(f"Removed: {name}")  # TODO: dev
    return
'''


@app.command
def update(
    datasource_name: str,
    ignore: list[str] | None = None,
    force=False,
    skip_db=False,
    ignore_errors=False,
    download=True,
):
    """Updates datasource by name, or all configured datasources ('.').
    Alternatively, update only the data warehouse using 'db'.
    Examples:
        `python bolt-cmd.py update .`  # updates everything
        `python bolt-cmd.py update db`  # updates only the database
        `python bolt-cmd.py update <datasource>`  # updates <datasource>
    """
    if not ignore:
        ignore = []
    # Determine datasources to process
    datasources: list[bolt.datasources.Datasource] | None = None
    ## All
    if datasource_name == ".":
        datasources = DATASOURCES
    ## Just the DB
    elif datasource_name.lower() == "db":
        datasources = []
    ## Just the specified one
    else:
        datasources = [getattr(bolt.datasources, datasource_name)]

    # Database
    db = bolt.warehouse.connect()
    db.sql(
        "CREATE TABLE IF NOT EXISTS data_updates (datasource VARCHAR PRIMARY KEY, last_updated DATE, hash VARCHAR(7));"
    )
    db.close()

    # A list of errors to print
    errors: list[tuple[str, Exception]] = []
    # Process datasources
    if datasources:
        tables_loaded = 0
        update_msg = "Updating datasources:"
        if force:
            update_msg = "Updating datasources (force=True):"
        console.print(update_msg)

        for D in datasources:
            try:
                d = D()
                if d.name in ignore:
                    console.print(f"        [yellow]Skipped: {d.name} (ignored)[/]")
                    continue
                db = bolt.warehouse.connect()

                ## Hash (sha256) the source files
                current_hash = bolt.warehouse.hash_sources(d)
                if not force:
                    # Ignore update for datasources with no changes to the source files
                    ## Get the last hash (sha256) of the source files
                    update_hash = db.sql(
                        f"SELECT hash FROM data_updates WHERE datasource = '{d.name}'"
                    ).pl()["hash"]
                    ## Compare hashes and skip if they are the same
                    if (
                        not update_hash.is_empty()
                        and current_hash == update_hash.item()
                    ):
                        console.print(
                            f"        [yellow]Skipped: {d.name} (unchanged)[/]"
                        )
                        continue
                with console.status(f"[cyan]      Updating {d.name}...[/]"):
                    df = d.update(download)  # noqa: F841
                    # Write to database
                    if isinstance(df, gpd.GeoDataFrame):
                        db.sql(
                            f"CREATE OR REPLACE TABLE {d.name} AS SELECT * FROM st_read('{d.cache_path}');"
                        )
                        tables_loaded += 1
                    elif isinstance(df, (pl.DataFrame, pd.DataFrame)):
                        db.sql(f"CREATE OR REPLACE TABLE {d.name} AS SELECT * FROM df")
                        tables_loaded += 1

                    db.sql(
                        f"INSERT OR REPLACE INTO data_updates VALUES ('{d.name}', '{dt.date.today()}', '{current_hash}')"
                    )
                    console.print(f"        [green]Updated: {d.name}[/]")
            except Exception as e:
                errors.append((d.name, e))
                console.print(f"        [red]Failed: {d.name}[/]")
                if not ignore_errors:
                    raise e
            finally:
                db.close()
        console.print(f"    Tables Loaded: {tables_loaded}")

    # Update database
    console.print("\nUpdating database:")
    if len(errors) > 0 and not ignore_errors:
        skip_db = True  # Override the skip_db flag

    if skip_db:
        console.print("        [yellow]Skipped[/].")
    else:
        with console.status("Updating database:"):
            try:
                sql_file_count, compact_msg = bolt.warehouse.update_sql(compact_db=True)
                bolt.warehouse.create_schemas()
                db_msg = (
                    f"        [green]Updated: {bolt.config.db_name}[/]\n"
                    f"            SQL Files Executed: {sql_file_count}\n"
                    f"            {compact_msg}"
                )
            except Exception as e:
                errors.append((bolt.config.db_name, e))
                db_msg = f"        [red]Failed: {bolt.config.db_name}[/]"
        console.print(db_msg)

    console.print(f"\nErrors: {len(errors)}")
    for name, err in errors:
        console.print(f"- [blue]{name}[/]: [red]{err}[/]")
    return


if __name__ == "__main__":
    try:
        # Initial blank line and app info
        console.print(f"\nBoltCMD ([b blue]v{__version__}[/])")
        app()
    except Exception:
        console.print_exception()
    finally:
        t_end = time.time()
        t = dt.timedelta(seconds=t_end - t_start)
        min = int(t.total_seconds() // 60)
        sec = t.total_seconds() % 60
        t_msg = f"{min}:{sec:.2f}"
        console.print(f"[white](Time: {t_msg})[/]\n")
