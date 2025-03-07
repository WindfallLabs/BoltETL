import datetime as dt
import time
from pathlib import Path
from typing import Literal

t_init_start = time.perf_counter_ns()

import cyclopts
import geopandas as gpd
import pandas as pd
import polars as pl
from rich.console import Console

import bolt

__version__ = bolt.__version__

console = Console()
app = cyclopts.App()

logo = """┏━━┓━━━━━┏┓━━┏┓━┏━━━┓┏━━━━┓┏┓━━━
┃┏┓┃━━━━━┃┃━┏┛┗┓┃┏━━┛┃┏┓┏┓┃┃┃━━━
┃┗┛┗┓┏━━┓┃┃━┗┓┏┛┃┗━━┓┗┛┃┃┗┛┃┃━━━
┃┏━┓┃┃┏┓┃┃┃━━┃┃━┃┏━━┛━━┃┃━━┃┃━┏┓
┃┗━┛┃┃┗┛┃┃┗┓━┃┗┓┃┗━━┓━┏┛┗┓━┃┗━┛┃
┗━━━┛┗━━┛┗━┛━┗━┛┗━━━┛━┗━━┛━┗━━━┛"""

SCRIPT = Path(__file__).name
DATASOURCES: dict[str, bolt.Datasource] = bolt.Datasource.registry
REPORTS: dict[str, bolt.Report] = bolt.Report.registry


def time_diff(start: float, end: float) -> str:
    """Calculates the minutes and seconds difference between two timestamps (floats)."""
    ms = (end - start) / 1000
    tot_secs = dt.timedelta(microseconds=ms).total_seconds()
    min = int(tot_secs // 60)
    sec = tot_secs % 60
    t_msg = f"{min}:{sec:.2f}"
    return t_msg


@app.command
def most_recent(datasource_name: str | None = None):
    """List the most recent raw file for a dataset (default all).

    Example
    -------
    `python bolt-cmd.py most-recent`
    `python bolt-cmd.py most-recent MyDataset`
    """
    for datasource_name, datasource in DATASOURCES.items():
        files = datasource.source_files
        ages = [(f.name, f.stat().st_mtime) for f in files]
        recent: tuple[str, float] = sorted(ages, key=lambda x: x[1], reverse=True)[0]
        ts = dt.datetime.fromtimestamp(recent[1]).strftime("%Y-%m-%d %I:%M %p")
        t = dt.datetime.now() - dt.datetime.fromtimestamp(recent[1])
        #stale_after = v.get("stale_after", 20)
        stale_after = 20  # TODO:
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

        console.print(f"[cyan]{datasource_name}[/]")
        console.print(f"Filename: '{recent[0]}'")
        console.print(f"Mod Date: [white]{ts}[/]")
        console.print(f"Age:      [{stale_color}]{age[0]} {age[1]}[/]")
        console.print()
    return


@app.command
def report(option: Literal["list", "info", "run"], rpt_name: str = "", *args, **kwargs):
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
        for rpt in REPORTS.values():
            console.print(f"        [green]{rpt.name}[/]")
        console.print("For more info, use: ")
        console.print("[b blue]    `python bolt-cmd.py report info <report name>`[/]\n")
        return

    if not rpt_name:
        raise AttributeError("'rpt_name' argument is required")

    rpt = REPORTS[rpt_name]
    if option == "info":
        console.print("[white]Report Info:[/]")
        console.print(f"[green]    {rpt.name}[/]")
        console.print(f"[yellow]{rpt.run.__doc__}[/]")
        return

    if option == "run":
        console.print(f"Running report: {rpt.name}...")
        console.print(f"        (args={args})")
        console.print(f"        (kwargs={kwargs})")
        try:
            rpt.run(*args, **kwargs)
            if getattr(rpt, "_exported", False):
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
        #datasources = list(get_datasources())
        datasources = list(DATASOURCES.values())
    ## Just the DB
    elif datasource_name.lower() == "db":
        datasources = []
    ## Just the specified one
    else:
        #datasources = [getattr(bolt.datasources, datasource_name)]
        datasources = [DATASOURCES[datasource_name]]

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

        #for D in datasources:
        for d in datasources:
            try:
                #d = D()
                if d.name in ignore:
                    console.print(f"        [yellow]Skipped: {d.name} (ignored)[/]")
                    continue
                db = bolt.warehouse.connect()

                ## Hash (sha256) the source files
                #current_hash = bolt.warehouse.hash_sources(d)  # TODO: replace (below)
                current_hash = d.metadata.hash_sources()
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
                    #df = d.update(download)  # noqa: F841
                    # TODO: reinstate download
                    df = d.update()
                    # TODO: d.load(db) -- to remove much of this logic
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


t_init_end = time.perf_counter_ns()

if __name__ == "__main__":
    try:
        # Initial blank line and app info
        console.print(f"\nBoltCMD ([b blue]v{__version__}[/])")
        t_start = time.perf_counter_ns()
        app()
    except Exception:
        console.print_exception()
    finally:
        t_end = time.perf_counter_ns()
        # Time/Speed metrics
        console.print(f"[white](Init Time: {time_diff(t_init_start, t_init_end)})[/]")
        console.print(f"[white](Execution Time: {time_diff(t_start, t_end)})[/]\n")
