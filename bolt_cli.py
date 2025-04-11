"""A command line utility for managing ETL pipelines and environments.

For more info, use:
    `python bolt_cli.py -h`
"""

import datetime as dt
import time
from getpass import getuser
from pathlib import Path
from platform import node
from typing import Any, Callable, Literal

t_init_start = time.perf_counter_ns()

import cyclopts  # noqa: E402
import duckdb  # noqa: E402
import polars as pl  # noqa: E402
from rich.console import Console  # noqa: E402
from rich.markdown import Markdown  # noqa: E402

import boltetl  # noqa: E402
import boltetl.env  # noqa: E402
from boltetl.core._datasource import RawDataOrigin  # noqa: E402
from boltetl.utils import time_diff  # noqa: E402

__version__ = boltetl.__version__
__author__ = boltetl.__author__

logo = """┏━━┓     ┏┓ ┏┓ ┏━━━┓┏━━━━┓┏┓
┃┏┓┃     ┃┃┏┛┗┓┃┏━━┛┃┏┓┏┓┃┃┃
┃┗┛┗┓┏━━┓┃┃┗┓┏┛┃┗━━┓┗┛┃┃┗┛┃┃
┃┏━┓┃┃┏┓┃┃┃ ┃┃ ┃┏━━┛  ┃┃  ┃┃ ┏┓
┃┗━┛┃┃┗┛┃┃┗┓┃┗┓┃┗━━┓ ┏┛┗┓ ┃┗━┛┃
┗━━━┛┗━━┛┗━┛┗━┛┗━━━┛ ┗━━┛ ┗━━━┛"""

console = Console()
app = cyclopts.App()

ENV = boltetl.Config.env_dir
SCRIPT = Path(__file__).name
WAREHOUSE: boltetl.Warehouse = boltetl.env.warehouse
USER = f"{node()}/{getuser()}"
CONFIG = boltetl.Config.cli_options
STYLE = CONFIG["style"]

# ============================================================================
# App Commands


@app.command
def about() -> None:
    """Basic info about BoltETL / bolt_cli.py"""
    console.print(f"Author: [blue]{__author__}[/]")
    console.print(f"Version: [blue]{__version__}[/]")
    console.print("\nAbout:")
    console.print(f"{__doc__}\n", style=STYLE)
    console.print("Visit us at:")
    console.print(Markdown("[PyPI]() (WIP)"))
    console.print(Markdown("[GitHub](https://github.com/WindfallLabs/BoltETL)"))
    return


@app.command
def env(
    option: Literal["list", "add", "activate"] | None = None,
    env_name: str = "",
    *args,
    **kwargs,
):
    """Manage BoltETL environments."""
    if not option:
        console.print()
        return

    # ------------------------------------------------------------------------
    # list
    if option.lower() == "list":
        envs = boltetl.Config.list_envs()
        current = boltetl.Config.env_dir
        console.print("Available Environments:")
        for env in envs:
            if env[0] == "BOLT-ACTIVE":
                continue
            if str(env[1]) == str(current):
                console.print(f"[green] -> {(env[0])}: {env[1]}[/]")
            else:
                console.print(f" -  {env[0]}: {env[1]}")
    # ------------------------------------------------------------------------
    # add
    elif option.lower() == "add":
        default = kwargs.get("default", False)
        boltetl.Config.add_env(env_name, kwargs["path"], default)
        console.print(f"Added [green]{env_name}[/]")
        if default:
            console.print("[cyan](Set as default)[/]")
    # ------------------------------------------------------------------------
    # activate
    elif option.lower() == "activate":
        # TODO: warn that env is already active
        boltetl.Config.activate_env(env_name)
        console.print(f"Activated environment: [green]{env_name} ({boltetl.Config.env_dir})[/]")
    console.print()
    return


@app.command
def most_recent(datasource_name: str | None = None):
    """List the most recent raw file for a dataset (default all).

    Example
    -------
    `python bolt_cli.py most-recent`
    `python bolt_cli.py most-recent MyDataset`
    """
    boltetl.env.datasources.load_all()
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
def tool(
    datasource_name: str, tool_name: str, mode: Literal["run", "info"] = "run", *args, **kwargs
) -> Any:
    """Execute Datasource methods exposed as command line tools.

    Example:
        > python bolt_cli.py tool MyDataset Method
    """
    with console.status("Loading datasources/tool..."):
        boltetl.env.datasources.load_all()
        datasource: boltetl.Datasource = WAREHOUSE.datasource_registry[datasource_name]
        tool: Callable = getattr(datasource, tool_name)
    if mode == "info":
        console.print(f"[yellow]{tool.__doc__}[/]")
        return
    console.print(f"Executing: [green]{datasource_name}.{tool_name}[/]")
    # Pass console to tool in kwargs
    kwargs.update({"console": console})
    result = tool(*args, **kwargs)
    console.print("Done")
    return result


@app.command
def report(option: Literal["list", "info", "run"] = "run", rpt_name: str = "", *args, **kwargs):
    """List, run, or get info about custom Report objects (with kwargs).

    Use `report-info <report name>` for details about a report.

        Example
        -------
        `python bolt_cli.py report run ParatransitNoShows --start=20250101 --end=20250131`
    """
    # TODO: write: bool = True?
    # TODO: consider an '--update' flag to update report dependencies
    # e.g. python bolt_cli.py report run ParatransitNoShows --update
    boltetl.env.reports.load_all()

    # ------------------------------------------------------------------------
    # list
    if option == "list":
        console.print("Available Reports:")
        for rpt in WAREHOUSE.report_registry.values():
            console.print(f"        [green]{rpt.name}[/]")
        console.print("For more info, use: ")
        console.print("[b blue]    `python bolt_cli.py report info <report name>`[/]\n")
        return

    if not rpt_name:
        raise AttributeError("'rpt_name' argument is required")

    rpt = WAREHOUSE.report_registry[rpt_name]

    # ------------------------------------------------------------------------
    # info
    if option == "info":
        console.print("[white]Report Info:[/]")
        console.print(f"[green]    {rpt.name}[/]")
        console.print(f"[yellow]{rpt.run.__doc__}[/]")
        return

    # ------------------------------------------------------------------------
    # run (default)
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
    boltetl.env.datasources.load_all()
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
def table(mode: Literal["list", "preview", "schema"], tbl_name: str = "", rows=15, cols=10):
    """Inspect warehoused data (database tables).

    mode: list
        Shows a list of tables in the warehouse.
    mode: preview
        Shows a preview of a given table or view.
    mode: schema
        Shows the (polars) schema of the given table or view.

    """
    if mode == "list":
        console.print("Tables in warehouse:")
        tables = WAREHOUSE.list_tables()
        for tbl in tables:
            console.print(f"        [cyan]{tbl}[/]")
        return
    if not tbl_name:
        raise AttributeError("A table name is required")
    elif mode == "preview":
        console.print(f"Preview of [cyan]{tbl_name}[/]:")
        pl.Config.set_tbl_rows(rows)
        pl.Config.set_tbl_cols(cols)
        pl.Config.set_tbl_hide_dataframe_shape()
        data = WAREHOUSE.get_data(tbl_name)
        console.print(data.head(rows))
        console.print(f" Rows: {rows}/{data.shape[0]}  |  Cols: {cols}/{data.shape[1]}\n")
        return
    elif mode == "schema":
        console.print(f"Schema of [cyan]{tbl_name}[/]:")
        data = WAREHOUSE.get_data(tbl_name)
        df = pl.DataFrame({"column": data.columns, "dtype": data.dtypes})
        pl.Config.set_tbl_rows(rows)
        pl.Config.set_tbl_hide_dataframe_shape()
        console.print(df)
        console.print(f" Rows: {rows}/{data.shape[0]}  |  Cols: 2/2\n")
        return
    return


@app.command
def update(
    datasource_name: str,
    ignore: list[str] | None = None,
    force=False,
    skip_db=False,
    ignore_errors=False,
    # download=True,
    quiet=False,
    bell=False,
):
    """Updates datasource by name, or all configured datasources ('.').

    Args
        ...
        ignore_errors (bool):

    Alternatively, update only the data warehouse using 'db'.
    Examples:
        `python bolt_cli.py update .`  # updates everything
        `python bolt_cli.py update db`  # updates only the database
        `python bolt_cli.py update <datasource>`  # updates <datasource>
    """
    if quiet:
        console.print("[black b]Updating...[/]")
        console.quiet = True
    boltetl.env.datasources.load_all()
    if not ignore:
        ignore = []
    # Determine datasources to process
    datasources: list[boltetl.datasources.Datasource] | None = None
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
    loading_error_cnt = len(boltetl.Datasource.failed_to_load) + len(boltetl.Report.failed_to_load)
    loading_errs = set()
    if loading_error_cnt > 0:
        console.print(f"[red]Import Error(s) occured:[/] {loading_error_cnt}")
        for failed in boltetl.Datasource.failed_to_load:
            loading_errs.add(failed)
            # TODO: log
            if failed in ignore or ignore_errors:
                console.print(f"        [yellow]Error:   {failed[0]} ([i]ignored[/i])[/]")
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
            # d.logger.info(f"Args: `--force={force} --download={download}`")
            d.logger.info(f"Args: `--force={force}`")

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
                    # TODO: consider `d.extract()`, `d.transform()`, and `d.load()` individually
                    d.update(WAREHOUSE, force)  # TODO: reinstate download option
                    # TODO: handle misc post-load callbacks
                    # Confirm load success
                    if (
                        len(d.data) == 1 and d.name not in WAREHOUSE.list_tables()
                    ):  # TODO: not the best way to measure...
                        d.logger.critical("FAILURE: Table load could not be confirmed")
                        raise duckdb.DataError("Table does not exist after attempting load")
                    d.logger.info("Table load confirmed")
                    if d.raw_data_origin == RawDataOrigin.FROM_CACHE:
                        console.print(
                            f"        [green]Updated: {d.name}[/]  [bright_black](C:{d.cache_read_time} | L:{d.load_time})[/]"
                        )
                    else:
                        console.print(
                            f"        [green]Updated: {d.name}[/]  [bright_black](E:{d.extract_time} | T:{d.transform_time} | L:{d.load_time})[/]"
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
                # TODO: WAREHOUSE.create_schema_table()
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
        for rpt_name, err in boltetl.Report.failed_to_load:
            console.print(
                f"- [blue]{rpt_name}[/] (Report) [red]failed to import:[/]\n    [red b]{err}[/]"
            )
    # Bell
    if bell:
        console.bell()
    return


t_init_end = time.perf_counter_ns()

if __name__ == "__main__":
    try:
        t_start = time.perf_counter_ns()
        # BoltETL logo and app info
        console.rule("[bright_black b]bolt_cli.py[/]", style=STYLE)
        if CONFIG["logo"]:
            console.print(logo, style=STYLE)
        console.print(
            f"BoltETL [bright_black]|[/] ([b blue]v{__version__}[/]) [bright_black]|[/] [green]"
            rf"\[{boltetl.Config.get_env_name()}][/]"
        )
        console.print()
        app()
    except Exception:
        console.print_exception()
    finally:
        t_end = time.perf_counter_ns()
        # Time/Speed metrics
        console.print(f"\n[bright_black](Init Time: {time_diff(t_init_start, t_init_end)})[/]")
        console.print(f"[bright_black](Execution Time: {time_diff(t_start, t_end)})[/]\n")
        console.rule(style=STYLE)
