import datetime as dt
import time
from getpass import getuser
from pathlib import Path
from platform import node
from typing import Literal

t_init_start = time.perf_counter_ns()

import cyclopts  # noqa: E402
import geopandas as gpd  # noqa: E402
import pandas as pd  # noqa: E402
import polars as pl  # noqa: E402
from rich.console import Console  # noqa: E402

import bolt  # noqa: E402
import bolt.env  # noqa: E402

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
WAREHOUSE: bolt.Warehouse = bolt.env.warehouse
USER = f"{node()}/{getuser()}"


# ============================================================================
# Utility Functions


def time_diff(start: float, end: float) -> str:
    """Calculates the minutes and seconds difference between two timestamps (floats)."""
    ms = (end - start) / 1000
    tot_secs = dt.timedelta(microseconds=ms).total_seconds()
    min = int(tot_secs // 60)
    sec = tot_secs % 60
    t_msg = f"{min}:{sec:.2f}"
    return t_msg


# ============================================================================
# App Commands


@app.command
def most_recent(datasource_name: str | None = None):
    """List the most recent raw file for a dataset (default all).

    Example
    -------
    `python bolt-cmd.py most-recent`
    `python bolt-cmd.py most-recent MyDataset`
    """
    bolt.env.datasources.load_all()
    for datasource_name, datasource in WAREHOUSE.datasource_registry.items():
        files = datasource.source_files
        source_info = f"{datasource.metadata.vendor}"
        if datasource.metadata.software:
            source_info += f" ({datasource.metadata.software})"
        ages = [(f.name, f.stat().st_mtime) for f in files]
        recent: tuple[str, float] = sorted(ages, key=lambda x: x[1], reverse=True)[0]
        ts = dt.datetime.fromtimestamp(recent[1]).strftime("%Y-%m-%d %I:%M %p")
        t = dt.datetime.now() - dt.datetime.fromtimestamp(recent[1])
        # stale_after = v.get("stale_after", 20)  # TODO: handle stale-age
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
        console.print(f"Source: {source_info}")
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
    bolt.env.reports.load_all()
    if option == "list":
        console.print("Available Reports:")
        for rpt in WAREHOUSE.report_registry.values():
            console.print(f"        [green]{rpt.name}[/]")
        console.print("For more info, use: ")
        console.print("[b blue]    `python bolt-cmd.py report info <report name>`[/]\n")
        return

    if not rpt_name:
        raise AttributeError("'rpt_name' argument is required")

    rpt = WAREHOUSE.report_registry[rpt_name]
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
            if getattr(rpt, "_exported", False):  # TODO: WIP
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
def execution_order():
    """Displays the order that registered SQL files will be executed in."""
    bolt.env.datasources.load_all()
    file_order: list[str] = [i.path.name for i in WAREHOUSE.execution_plan() if i.path]
    console.print(f"SQL Execution Order ({len(file_order)} files):")
    for n, i in enumerate(file_order):
        n += 1
        num = f"{n}"
        if n < 10:
            num = f" {n}"
        console.print(f"        {num}) [green]{i}[/]")
    console.print()
    return


@app.command
def schema(tbl: str, rows=50):
    """Shows the (polars) schema of the given table or view."""
    console.print(f"Schema of [green]{tbl}[/]")
    data = WAREHOUSE.get_data(tbl)
    df = pl.DataFrame({"column": data.columns, "dtype": data.dtypes})
    pl.Config.set_tbl_rows(rows)
    pl.Config.set_tbl_hide_dataframe_shape()
    console.print(df)
    console.print(f" Rows: {rows}/{data.shape[0]}  |  Cols: 2/2\n")
    return


@app.command
def preview(tbl: str, rows=15, cols=10):
    """Shows a preview of a given table or view."""
    console.print(f"Preview of [green]{tbl}[/]")
    pl.Config.set_tbl_rows(rows)
    pl.Config.set_tbl_cols(cols)
    pl.Config.set_tbl_hide_dataframe_shape()
    data = WAREHOUSE.get_data(tbl)
    console.print(data.head(rows))
    console.print(f" Rows: {rows}/{data.shape[0]}  |  Cols: {cols}/{data.shape[1]}\n")
    return


@app.command
def list_tables():
    """Shows a list of tables in the warehouse."""
    console.print(WAREHOUSE.list_tables())
    return


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

    Args
        ...
        ignore_errors (bool):

    Alternatively, update only the data warehouse using 'db'.
    Examples:
        `python bolt-cmd.py update .`  # updates everything
        `python bolt-cmd.py update db`  # updates only the database
        `python bolt-cmd.py update <datasource>`  # updates <datasource>
    """
    bolt.env.datasources.load_all()
    if not ignore:
        ignore = []
    # Determine datasources to process
    datasources: list[bolt.datasources.Datasource] | None = None
    ## All
    if datasource_name == ".":
        datasources = list(WAREHOUSE.datasource_registry.values())
    ## Just the DB
    elif datasource_name.lower() == "db":
        datasources = []
    ## Just the specified one
    else:
        # datasources = [getattr(bolt.datasources, datasource_name)]
        datasources = [WAREHOUSE.datasource_registry[datasource_name]]

    # A list of errors to print
    errors: list[tuple[str, Exception]] = []
    # Loading failures
    loading_error_cnt = len(bolt.Datasource.failed_to_load) + len(
        bolt.Report.failed_to_load
    )
    if loading_error_cnt > 0:
        console.print(f"[red]Import Error(s) occured:[/] {loading_error_cnt}\n")

    # Process datasources
    if datasources:
        tables_loaded = 0
        update_msg = "Updating datasources:"
        if force:
            update_msg = "Updating datasources (force=True):"
        console.print(update_msg)

        # Print loading errors
        for ds_name, err in bolt.Datasource.failed_to_load:
            if ignore_errors and ds_name not in ignore:
                ignore.append(ds_name)
            if ds_name in ignore:
                console.print(
                    f"        [yellow]Skipped: {ds_name} (error; [i]ignored[/i])[/]"
                )
            else:
                console.print(f"        [red]Error:   {ds_name} (failed to import)[/]")
                errors.append((ds_name, err))

        # Log to each Datasource's log
        for d in datasources:
            d.logger.info("============== Bolt-CMD ==============")
            d.logger.info(f"Start ({d.name})")
            d.logger.info(f"Args: `--force={force} --download={download}`")
            d.logger.info(f"Executed by {USER}")

            if d.name in ignore:
                console.print(f"        [yellow]Skipped: {d.name} ([i]ignored[/i])[/]")
                d.logger.info("Ignored (explicitly by user)")
                continue

            do_update = True

            # Try to hash and do recent update check
            try:
                db = bolt.env.warehouse.connect(False)

                ## Hash (sha256) the source files
                # TODO: hash the datasource / python file
                # TODO: hash the data
                if d.source_files:
                    d.logger.info("Calculating hash")
                    if not force:
                        # Ignore update for datasources with no changes to the source files
                        # Get the last hash (sha256) of the source files
                        update_hash = db.sql(
                            f"SELECT sources_hash FROM bolt_metadata WHERE table_name = '{d.name}'"
                        ).pl()["sources_hash"]
                        # Compare hashes and skip if they are the same
                        if (
                            not update_hash.is_empty()
                            and d.metadata.sources_hash == update_hash.item()
                        ):
                            do_update = False
            except Exception as e:
                errors.append((d.name, e))
            finally:
                db.close()

            if not do_update:
                console.print(f"        [yellow]Skipped: {d.name} (unchanged)[/]")
                d.logger.info(
                    f"Update skipped (source files unchanged; {d.metadata.sources_hash})"
                )
                continue

            try:
                db = bolt.env.warehouse.connect(False)

                # TODO: try `d.extract()`, `d.transform()`, and `d.load()` individually
                # TODO: handle misc post-load callbacks
                with console.status(f"[cyan]      Updating {d.name}...[/]"):
                    d.logger.info("Calling update command")
                    df = d.update()  # noqa: F841
                    # TODO: reinstate download option

                    # ========================================================
                    # TODO: remove all this (should be in each datasource's `load` method)
                    if isinstance(df, gpd.GeoDataFrame):
                        db.sql(
                            f"CREATE OR REPLACE TABLE {d.name} AS SELECT * FROM st_read('{d.options.cache_path}');"
                        )
                    elif isinstance(df, (pl.DataFrame, pd.DataFrame)):
                        db.sql(f"CREATE OR REPLACE TABLE {d.name} AS SELECT * FROM df")

                    # db.sql(
                    #    f"INSERT OR REPLACE INTO bolt_metadata VALUES ('{d.name}', '{dt.date.today()}', '{current_hash}')"
                    # )
                    # TODO: assert that table is inside database
                    console.print(f"        [green]Updated: {d.name}[/]")
                    d.logger.info("Update complete")
                    # ========================================================
            except Exception as e:
                d.logger.critical(f"{e}")
                errors.append((d.name, e))
                console.print(f"        [red]Failed: {d.name}[/]")
                if not ignore_errors:
                    raise e
            finally:
                db.close()
                d.logger.info("End")
            tables_loaded += 1
        console.print(f"    Tables Loaded: {tables_loaded}")
        if ignore:
            console.print(f"    Ignored: [yellow]{len(ignore)}[/]")

    # Update database
    console.print("\nUpdating database:")
    if len(errors) > 0 and not ignore_errors:
        skip_db = True  # Override the skip_db flag

    if skip_db:  # Set if errors
        if len(errors) > 0:
            console.print("        [red]Skipped (errors)[/]")
        else:
            console.print("        [yellow]Skipped ([i]ignored[/i])[/]")
    else:
        with console.status("Updating database:"):
            try:
                sql_file_count, compact_msg = WAREHOUSE.rebuild(compact=True)
                # WAREHOUSE.create_schema_table()
                db_msg = (
                    f"        [green]Updated: {WAREHOUSE.name}[/]\n"
                    f"            SQL Files Executed: {sql_file_count}\n"
                    f"            {compact_msg}"
                )
            except Exception as e:
                errors.append((WAREHOUSE.name, e))
                db_msg = f"        [red]Failed: {WAREHOUSE.name}[/]"
                console.print_exception()
        console.print(db_msg)

    # Print error info
    err_cnt = f"{len(errors)}"
    if len(errors) > 0:
        err_cnt = f"[red]{len(errors)}[/]"
    if ignore_errors:
        console.print("\nErrors: [yellow i]ignored[/]")
    else:
        console.print(f"\nErrors: {err_cnt}")
    for name, err in errors:
        console.print(f"- [blue]{name}[/]: [red]{err}[/]")
    # Report-loading errors
    for rpt_name, err in bolt.Report.failed_to_load:
        console.print(
            f"- [blue]{rpt_name}[/] (Report): [red]Failed to import:[/]\n    [red]{err}[/]"
        )
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
