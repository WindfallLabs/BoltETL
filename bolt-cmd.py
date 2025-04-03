import datetime as dt
import time
from getpass import getuser
from pathlib import Path
from platform import node
from typing import Literal

t_init_start = time.perf_counter_ns()

import cyclopts  # noqa: E402
import duckdb  # noqa: E402
import polars as pl  # noqa: E402
from rich.console import Console  # noqa: E402

import bolt  # noqa: E402
import bolt.env  # noqa: E402
from bolt.utils import time_diff  # noqa: E402

__version__ = bolt.__version__

console = Console()
app = cyclopts.App()

logo = """┏━━┓━━━━━┏┓━━┏┓━┏━━━┓┏━━━━┓┏┓━━━
┃┏┓┃━━━━━┃┃━┏┛┗┓┃┏━━┛┃┏┓┏┓┃┃┃━━━
┃┗┛┗┓┏━━┓┃┃━┗┓┏┛┃┗━━┓┗┛┃┃┗┛┃┃━━━
┃┏━┓┃┃┏┓┃┃┃━━┃┃━┃┏━━┛━━┃┃━━┃┃━┏┓
┃┗━┛┃┃┗┛┃┃┗┓━┃┗┓┃┗━━┓━┏┛┗┓━┃┗━┛┃
┗━━━┛┗━━┛┗━┛━┗━┛┗━━━┛━┗━━┛━┗━━━┛"""

ENV = bolt.Config.env_dir
SCRIPT = Path(__file__).name
WAREHOUSE: bolt.Warehouse = bolt.env.warehouse
USER = f"{node()}/{getuser()}"


# @cyclopts.Parameter(name="*")
# @dataclass
# class Common:
#     quiet: bool = False

#     #def __post_init__(self):
#     def set_quiet(self):
#         if self.quiet:
#             global console
#             console.quiet = True
#         return

# ============================================================================
# App Commands


@app.command
def env(
    option: Literal["list", "add", "activate"] | None = None,
    env_name: str = "",
    # common: Common|None = Common(),
    *args,
    **kwargs,
):
    """."""
    if not option:
        console.print()
        return
    # ------------------------------------------------------------------------
    # LIST
    if option.lower() == "list":
        envs = bolt.Config.list_envs()
        current = bolt.Config.env_dir
        console.print("Available Environments:")
        for env in envs:
            if env[0] == "BOLT-ACTIVE":
                continue
            if str(env[1]) == str(current):
                console.print(f"[green] -> {(env[0])}: {env[1]}[/]")
            else:
                console.print(f" -  {env[0]}: {env[1]}")
    # ------------------------------------------------------------------------
    # ADD
    elif option.lower() == "add":
        default = kwargs.get("default", False)
        bolt.Config.add_env(env_name, kwargs["path"], default)
        console.print(f"Added [green]{env_name}[/]")
        if default:
            console.print("[cyan](Set as default)[/]")
    # ------------------------------------------------------------------------
    # CHANGE
    elif option.lower() == "activate":
        # TODO: warn that env is already active
        bolt.Config.activate_env(env_name)
        console.print(
            f"Activated environment: [green]{env_name} ({bolt.Config.env_dir})[/]"
        )
    console.print()
    return


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
    quiet=False,
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
    if quiet:
        console.print("[black b]Updating...[/]")
        console.quiet = True
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
        datasources = [WAREHOUSE.datasource_registry[datasource_name]]

    # A list of errors to print
    errors: list[tuple[str, Exception]] = []
    # Loading failures
    loading_error_cnt = len(bolt.Datasource.failed_to_load) + len(
        bolt.Report.failed_to_load
    )
    loading_errs = set()
    if loading_error_cnt > 0:
        console.print(f"[red]Import Error(s) occured:[/] {loading_error_cnt}")
        for failed in bolt.Datasource.failed_to_load:
            loading_errs.add(failed)
            # TODO: log
            if failed in ignore or ignore_errors:
                console.print(
                    f"        [yellow]Error:   {failed[0]} ([i]ignored[/i])[/]"
                )
            else:
                console.print(f"        [red]Error:   {failed[0]}[/]")
        console.print()

    # Process datasources
    if datasources:
        tables_loaded = 0
        update_msg = "Updating datasources:"
        if force:
            update_msg = "Updating datasources (force=True):"
        console.print(update_msg)

        # Log to each Datasource's log
        for d in datasources:
            d.logger.info("============== Bolt-CMD ==============")
            d.logger.info(f"Started update for {d.name} (by {USER})")
            d.logger.info(f"Args: `--force={force} --download={download}`")

            if d.name in ignore:
                console.print(f"        [yellow]Skipped: {d.name} ([i]ignored[/i])[/]")
                d.logger.info("Ignored (explicitly by user)")
                continue

            do_update = True

            # ----------------------------------------------------------------
            # HASH
            # Do recently updated check
            do_update: bool = True
            if not force:
                d.logger.info("Comparing hashes")
                do_update = WAREHOUSE.compare_hashes(d)

            if not do_update:
                console.print(f"        [yellow]Skipped: {d.name} (unchanged)[/]")
                d.logger.info("Update skipped (source files unchanged)")
                continue

            # ----------------------------------------------------------------
            # UPDATE
            try:
                with console.status(f"[cyan]      Updating {d.name}...[/]"):
                    d.logger.info("Calling update command")
                    # TODO: try `d.extract()`, `d.transform()`, and `d.load()` individually
                    d.update(WAREHOUSE)  # TODO: reinstate download option
                    # TODO: handle misc post-load callbacks
                    # Confirm load success
                    if d.name not in WAREHOUSE.list_tables():
                        d.logger.critical("FAILURE: Table load could not be confirmed")
                        raise duckdb.DataError(
                            "Table does not exist after attempting load"
                        )
                    d.logger.info("Table load confirmed")
                    console.print(
                        f"        [green]Updated: {d.name}[/]  [blue](E:{d.extract_time} T:{d.transform_time} L:{d.load_time})[/]"
                    )
                    d.logger.info("Update complete")
                    WAREHOUSE.logger.info(f"Loaded {d.name}")
            except Exception as e:
                d.logger.critical(f"{e}")
                errors.append((d.name, e))
                console.print(f"        [red]Failed:  {d.name}[/]")
                if not ignore_errors:
                    raise e
            finally:
                d.logger.info("End")
            # ----------------------------------------------------------------
            tables_loaded += 1
        console.print(f"    Tables Loaded: {tables_loaded}")
        if ignore:
            console.print(f"    Ignored: [yellow]{len(ignore)}[/]")

    if len(datasources) > 0:
        console.print()

    # Update database
    console.print("Updating database:")
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
    err_cnt = f"{len(errors) + loading_error_cnt}"
    if len(errors) > 0:
        err_cnt = f"[red]{len(errors)}[/]"
    if ignore_errors:
        console.print("\nErrors: [yellow i]ignored[/]")
    else:
        console.print(f"\nErrors: {err_cnt}")
        for failed_ds, err in loading_errs:
            console.print(
                f"- [blue]{failed_ds}[/] (Datasource) [red]failed to import:[/]\n    [red b]{err}[/]"
            )
        for name, err in errors:
            console.print(f"- [blue]{name}[/]: [red]{err}[/]")
        # Report-loading errors
        for rpt_name, err in bolt.Report.failed_to_load:
            console.print(
                f"- [blue]{rpt_name}[/] (Report) [red]failed to import:[/]\n    [red b]{err}[/]"
            )
    return


t_init_end = time.perf_counter_ns()

if __name__ == "__main__":
    try:
        t_start = time.perf_counter_ns()
        # Initial blank line and app info
        # console.print(f"\nBoltCMD ([b blue]v{__version__}[/])")
        console.print(
            f"\nBoltCMD ([b blue]v{__version__}[/]) [green]"
            rf"\[{bolt.Config.get_env_name()}][/]"
        )
        app()
    except Exception:
        console.print_exception()
    finally:
        t_end = time.perf_counter_ns()
        # Time/Speed metrics
        console.print(f"[white](Init Time: {time_diff(t_init_start, t_init_end)})[/]")
        console.print(f"[white](Execution Time: {time_diff(t_start, t_end)})[/]\n")
