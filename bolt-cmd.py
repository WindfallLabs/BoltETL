import datetime as dt
import time
from pathlib import Path
from typing import Literal

t_init_start = time.time()

import cyclopts
import geopandas as gpd
import pandas as pd
import polars as pl
from rich.console import Console

import bolt
import bolt.warehouse

__version__ = "0.2.0-dev"

console = Console()
app = cyclopts.App()

logo = """┏━━┓━━━━━┏┓━━┏┓━┏━━━┓┏━━━━┓┏┓━━━
┃┏┓┃━━━━━┃┃━┏┛┗┓┃┏━━┛┃┏┓┏┓┃┃┃━━━
┃┗┛┗┓┏━━┓┃┃━┗┓┏┛┃┗━━┓┗┛┃┃┗┛┃┃━━━
┃┏━┓┃┃┏┓┃┃┃━━┃┃━┃┏━━┛━━┃┃━━┃┃━┏┓
┃┗━┛┃┃┗┛┃┃┗┓━┃┗┓┃┗━━┓━┏┛┗┓━┃┗━┛┃
┗━━━┛┗━━┛┗━┛━┗━┛┗━━━┛━┗━━┛━┗━━━┛"""

SCRIPT = Path(__file__).name


def get_datasources():
    """Generates a list of datasource objects (using the config)."""
    for ds in bolt.config.metadata.keys():
        DS = getattr(bolt.datasources, ds, None)
        if DS and issubclass(DS, bolt.datasources.Datasource):
            yield DS


def get_reports():
    """Generates a list of report objects."""
    for _obj_name in bolt.reports.__dir__():
        RPT = getattr(bolt.reports, _obj_name, None)
        if type(RPT).__name__ in ("str", "module"):
            continue
        if not hasattr(RPT, "__name__") or not hasattr(RPT, "__class__"):
            continue
        if RPT.__name__ == "BaseReport":
            continue
        if issubclass(RPT, bolt.reports.BaseReport):
            yield RPT


def time_diff(start: float, end: float) -> str:
    """Calculates the minutes and seconds difference between two timestamps (floats)."""
    delta = dt.timedelta(seconds=(end - start))  # TODO: .total_seconds()
    min = int(delta.total_seconds() // 60)
    sec = delta.total_seconds() % 60
    t_msg = f"{min}:{sec:.2f}"
    return t_msg


@app.command
def most_recent(datasource_name: str|None = None):
    """List the most recent raw file for a dataset (default all).

        Example
        -------
        `python bolt-cmd.py most-recent`
        `python bolt-cmd.py most-recent MyDataset`
    """
    for k, v in bolt.config.metadata.items():
        if not v.get("source_dir", False):
            continue
        if datasource_name and k != datasource_name:
            continue
        files = Path(v["source_dir"]).rglob(v["filename"])
        ages = [(f.name, f.stat().st_mtime) for f in files]
        recent: tuple[str, float] = sorted(ages, key=lambda x: x[1], reverse=True)[0]
        ts = dt.datetime.fromtimestamp(recent[1]).strftime("%Y-%m-%d %I:%M %p")
        t = dt.datetime.now() - dt.datetime.fromtimestamp(recent[1])
        stale_after = v.get("stale_after", 20)
        stale_color = "green"

        if t.days > 0:
            age = (t.days, "days")
            if age[0] >= stale_after - 5:
                stale_color = "yellow"
            # WIP:
            if age[0] > stale_after:
                stale_color = "red"
        else:
            age = (round(t.total_seconds() / 3600, 1), "hours")

        console.print(f"[cyan]{k}[/]")
        console.print(f"Filename: '{recent[0]}'")
        console.print(f"Mod Date: [white]{ts}[/]")
        console.print(f"Age:      [{stale_color}]{age[0]} {age[1]}[/]")
        console.print()
    return


@app.command
def report(
    option: Literal["list", "info", "run"], rpt_name: str = "", *args, **kwargs
):
    """Execute a report by report class name (with kwargs).

    Use `report-info <report name>` for details about a report.

        Example
        -------
        `python bolt-cmd.py report run ParatransitNoShows --start=20250101 --end=20250131`
    """
    # TODO: write: bool = True?
    # TODO: consider an '--update' flag to update report dependencies
    # e.g. python bolt-cmd.py report run ParatransitNoShows --update
    # NOTE: list option does not require 'rpt_name'
    if option == "list":
        console.print("Available Reports:")
        #for rpt in REPORTS:
        for rpt in list(get_reports()):
            console.print(f"        [green]{rpt.__name__}[/]")
        console.print("For more info, use: ")
        console.print("[b blue]    `python bolt-cmd.py report info <report name>`[/]\n")
        return

    if not rpt_name:
        raise AttributeError("'rpt_name' argument is required")

    Rpt = getattr(bolt.reports, rpt_name)  # Get python class by name
    rpt = Rpt()
    if option == "info":
        console.print(f"[white]Report Info:[/]")
        console.print(f"[green]    {rpt.name}[/]")
        console.print(f"[yellow]{rpt.run.__doc__}[/]")
        return

    if option == "run":
        console.print(f"Running report: {rpt_name}...")
        console.print(f"        (args={args})")
        console.print(f"        (kwargs={kwargs})")
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
        datasources = list(get_datasources())
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
        if len(errors) > 0:
            console.print("        [red]Skipped (errors)[/].")
        else:
            console.print("        [yellow]Skipped (ignored)[/].")
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


t_init_end = time.time()

if __name__ == "__main__":
    try:
        # Initial blank line and app info
        console.print(f"\nBoltCMD ([b blue]v{__version__}[/])")
        t_start = time.time()
        app()
    except Exception:
        console.print_exception()
    finally:
        t_end = time.time()
        # Time/Speed metrics
        console.print(f"[white](Init Time: {time_diff(t_init_start, t_init_end)})[/]")
        console.print(f"[white](Execution Time: {time_diff(t_start, t_end)})[/]\n")
