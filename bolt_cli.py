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
# Base functions

def update_warehouse(
    console,
    warehouse,
    compact=True,
    quiet=False
) -> tuple[bool, str, list[tuple[str, Exception]]]:
    """Core functionality for updating the database warehouse.
    
    This function handles the database update process and can be called
    from multiple command functions.
    
    Args:
        console: The console object for output
        warehouse: The WAREHOUSE object to update
        skip_db: Whether to skip the database refresh
        ignore_errors: Whether to ignore errors during update
        force: Force update regardless of other conditions
        quiet: Minimize console output
        
    Returns:
        tuple containing:
        - success: Boolean indicating if the update was successful
        - message: Status message about the update
        - errors: List of (name, exception) tuples for any errors
    """
    errors: list[tuple[str, Exception]] = []
    if quiet:
        console.quiet = True
    
    with console.status("      Updating database..."):
        try:
            #sql_file_count, compact_msg = warehouse.rebuild(compact=True)
            sql_file_count = warehouse.rebuild()
            # TODO: warehouse.create_schema_table()
            success = True
            update_message = (
                f"        [green]Updated[/]\n"
                f"            SQL Files Executed: {sql_file_count}"
            )
        except Exception as e:
            errors.append((warehouse.name, e))
            update_message = f"        [red]Failed: {warehouse.name}[/]"
            console.print_exception()
            success = False
        finally:
            console.print(update_message)

    # compact option
    if compact:
        with console.status("      Compacting..."):
            try:
                size_before, size_after = WAREHOUSE.compact()
                reduction = size_before - size_after
                percent = (reduction / size_before) * 100 if size_before > 0 else 0
                compact_message = (
                    f"        [green]Compacted[/]\n"
                    f"            {size_before / 1024**2:.5f} MB → "
                    f"{size_after / 1024**2:.5f} MB "
                    f"[bright_black](-{percent:.1f}%)[/]"
                )
            except Exception as e:
                errors.append((warehouse.name, e))
                compact_message = f"        [red]Failed: {warehouse.name}[/]"
                console.print_exception()
                success = False
            finally:
                console.print(compact_message)

    return errors


# ============================================================================
# Shell (idea)

from cmd import Cmd


class Prompt(Cmd):
    prompt = "BoltETL> "
    intro = "BoltETL shell started!\n"

    def do_exit(self, inp):
        print("Bye")
        return True

    def do_quit(self, inp):
        return self.do_exit(inp)

    def do_q(self, inp):
        return self.do_exit(inp)

    def do_export(self, inp):
        ds, ft = inp.split(" ")
        data = WAREHOUSE.get_data(ds)
        # TODO: ...
        return

    def do_extract(self, inp):
        with console.status("Initializing..."):
            boltetl.env.datasources.load_all()
        with console.status("Extracting..."):
            ds = WAREHOUSE.datasource_registry[inp]
            ds.extract()
        return

    def do_transform(self, inp):
        with console.status("Transforming..."):
            ds = WAREHOUSE.datasource_registry[inp]
            ds.transform()
        return

    def do_load(self, inp):
        with console.status("Loading..."):
            ds = WAREHOUSE.datasource_registry[inp]
            ds.load(WAREHOUSE)
        return

    def do_show_datasource(self, inp):
        with console.status("Displaying..."):
            ds = WAREHOUSE.datasource_registry[inp]
        console.print(ds.data)
        return

    def do_sql(self, query: str):
        with WAREHOUSE.connect() as con:
            data = con.sql(query).pl()
        pl.Config.set_tbl_cols(round(console.width/20))
        console.print(data)
        return

    def do_list_tables(self, inp):
        console.print(WAREHOUSE.list_tables())
        return

    def do_refresh(self, inp):
        update_warehouse(console, WAREHOUSE)
        return


@app.command
def shell() -> None:
    Prompt().cmdloop()



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
    env_name: str | None = None,
    *args,
    **kwargs,
) -> None:
    """Manage BoltETL environments.

    Args:
        option (list, add, or activate): Which option to use
        env_name (str): The name of the environment to operate against

    Example:
        `python bolt_cli.py env list`
        `python bolt_cli.py env add NewEnv`  # TODO
        `python bolt_cli.py env activate MyEnv`
    """
    if not option:
        console.print()
        return
    option = option.lower()

    # ------------------------------------------------------------------------
    # list
    if option == "list":
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
        return

    if not env_name:
        raise AttributeError("'env_name' argument is required")

    # ------------------------------------------------------------------------
    # add
    elif option == "add":
        default = kwargs.get("default", False)
        boltetl.Config.add_env(env_name, kwargs["path"], default)
        console.print(f"Added [green]{env_name}[/]")
        if default:
            console.print("[cyan](Set as default)[/]")
    # ------------------------------------------------------------------------
    # activate
    elif option == "activate":
        # TODO: warn that env is already active
        boltetl.Config.activate_env(env_name)
        console.print(f"Activated environment: [green]{env_name} ({boltetl.Config.env_dir})[/]")
    console.print()
    return


@app.command
def most_recent(datasource_name: str | None = None) -> None:
    """List the most recent raw file for a dataset (default all).

    Args:
        datasource_name (str): The name of the Datasource to get the most recent source file for
            (defaults to all)

    Example:
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
    datasource_name: str, tool_name: str, option: Literal["run", "info"] = "run", *args, **kwargs
) -> Any:
    """Execute Datasource methods exposed as command line tools.

    Example:
        `python bolt_cli.py tool MyDataset Method`
    """
    option = option.lower()
    with console.status("Loading datasources/tool..."):
        boltetl.env.datasources.load_all()
        datasource: boltetl.Datasource = WAREHOUSE.datasource_registry[datasource_name]
        tool: Callable = getattr(datasource, tool_name)
    if option == "info":
        console.print(f"[green]{datasource_name} {tool.__name__}[/] [white](tool) info:[/]")
        console.print(f"[yellow]{tool.__doc__}[/]")
        return
    console.print(f"Executing: [green]{datasource_name}.{tool_name}[/]")
    # Pass console to tool in kwargs
    kwargs.update({"console": console})
    result = tool(*args, **kwargs)
    console.print("Done")
    return result


@app.command
def report(
    option: Literal["list", "info", "run"], rpt_name: str | None = None, *args, **kwargs
) -> None:
    """List, run, or get info about custom Report objects (with kwargs).

    Use `python bolt_cli.py report info <report name>` for details about a report.

    Args:
        option (list, info, or run): The option to use
        rpt_name (str): The name of a Report object to execute or get info for

    Example:
        `python bolt_cli.py report run ParatransitNoShows --start=20250101 --end=20250131`
    """
    # TODO: write: bool = True?
    # TODO: consider an '--update' flag to update report dependencies
    # e.g. python bolt_cli.py report run ParatransitNoShows --update
    option = option.lower()
    boltetl.env.reports.load_all()

    # ------------------------------------------------------------------------
    # list
    if option == "list":
        console.print("Available Reports:")
        for rpt in WAREHOUSE.report_registry.values():
            console.print(f"        [green]{rpt.name}[/]")
        console.print("For more info, use: ")
        console.print("[b blue]    `python bolt_cli.py report info <report name>`[/]")
        return

    if not rpt_name:
        raise AttributeError("'rpt_name' argument is required")

    rpt = WAREHOUSE.report_registry[rpt_name]

    # ------------------------------------------------------------------------
    # info
    if option == "info":
        console.print(f"[green]{rpt.name}[/] [white](report) info:[/]")
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
def execution_order() -> None:
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
def table(
    option: Literal["list", "preview", "schema"], tbl_name: str = "", rows=15, cols=10
) -> None:
    """Inspect warehoused data (database tables).

    Args:
        option (list, preview, or schema): Which option to use
            ('list' shows a list of tables in the warehouse;
            'preview' shows a preview of a given table or view;
            'schema' shows the (polars) schema of the given table or view.)
    """
    option = option.lower()
    if option == "list":
        console.print("Tables in warehouse:")
        tables = WAREHOUSE.list_tables()
        for tbl in tables:
            console.print(f"        [cyan]{tbl}[/]")
        return
    if not tbl_name:
        raise AttributeError("A table name is required")
    elif option == "preview":
        console.print(f"Preview of [cyan]{tbl_name}[/]:")
        pl.Config.set_tbl_rows(rows)
        pl.Config.set_tbl_cols(cols)
        pl.Config.set_tbl_hide_dataframe_shape()
        data = WAREHOUSE.get_data(tbl_name)
        console.print(data.head(rows))
        console.print(f" Rows: {rows}/{data.shape[0]}  |  Cols: {cols}/{data.shape[1]}\n")
        return
    elif option == "schema":
        console.print(f"Schema of [cyan]{tbl_name}[/]:")
        data = WAREHOUSE.get_data(tbl_name)
        df = pl.DataFrame({"column": data.columns, "dtype": data.dtypes})
        pl.Config.set_tbl_rows(rows)
        pl.Config.set_tbl_hide_dataframe_shape()
        console.print(df)
        console.print(f" Rows: {rows}/{data.shape[0]}  |  Cols: 2/2\n")
        return
    return


# NEW
@app.command
def warehouse(
    option: Literal["info", "update"],
    ignore_errors: bool = False,
    compact: bool = True,
    quiet: bool = False,
    bell: bool = False,
) -> None:
    """Manage the data warehouse.
    
    Args:
        option (info, update, or compact): Which operation to perform
        force (bool): Force operations regardless of conditions
        ignore_errors (bool): Continue execution even if errors occur
        quiet (bool): Minimize console output
        bell (bool): Activate console bell when complete
    
    Examples:
        `python bolt_cli.py warehouse update`
        `python bolt_cli.py warehouse info`
        `python bolt_cli.py warehouse compact`
    """
    option = option.lower()
    orig_quiet = console.quiet
    console.quiet = quiet

    console.print(f"Warehouse: [green]{WAREHOUSE.name}[/]")

    # info option
    if option == "info":
        console.print(f"Location: {WAREHOUSE.path}")
        tables = WAREHOUSE.list_tables()
        console.print(f"Tables: {len(tables)}")
        #last_updated = WAREHOUSE.get_last_updated()
        #if last_updated:
        #    console.print(f"Last updated: {last_updated}")
        return
    
    # update option
    elif option == "update":
        # Call the core warehouse update function
        errors = update_warehouse(
            console, WAREHOUSE, compact, quiet
        )

    # Handle errors
    if len(errors) > 0:
        if not ignore_errors:
            for name, err in errors:
                console.print(f"- [blue]{name}[/]: [red]{err}[/]")

    # Bell notification if requested
    if bell:
        console.bell()
    
    console.quiet = orig_quiet
    return





@app.command
def update(
    datasource_name: str|Literal[".", "db"],
    skip: bool = True,
    # ===== ETL Controls (defaults are for full-update) =====
    download: bool = True,
    read_cache: bool = False,  # False forces extract and transform
    validate: bool = True,
    write_cache: bool = True,
    # ===== Other Controls =====
    lazy: bool = False,
    ignore: list[str] | None = None,
    skip_db: bool = False,
    compact: bool = True,
    ignore_errors: bool = False,
    quiet: bool = False,
    bell: bool = False,
    **kwargs
) -> None:
    """Updates datasource by name, or all configured datasources ('.').

    # NOTE!! By default, update is eager (it does as much work it can to complete the job)


    Args:
        datasource_name (str): The name of the datasource to update. Or use '.' for all,
            or 'db' to update only the data warehouse
        force (bool): Force the update (ignore things that might skip updates)
        ignore (list[str]): Datasources to ignore (use '--ignore=One --ignore=Two')
        skip_db (bool): Skip the database refresh
        ignore_errors (bool): Skips the update of a Datasource if it raises an error
        quiet (bool): Minimizes printed output
        bell (bool): Activate the console bell (ding sound) when the process is complete
        kwargs (dict): Use custom flags (e.g. `--flag=True`) and pass them to your functions as **kwargs

    Example:
        `python bolt_cli.py update .`  # updates everything
        `python bolt_cli.py update db`  # updates only the database
        `python bolt_cli.py update <datasource>`  # updates <datasource>
    """
    if lazy:
        skip = False
        download = False
        read_cache = True

    _args = locals()
    if quiet:
        console.print("[black b]Updating...[/]")
        console.quiet = True
    with console.status("Importing Datasources..."):
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
        console.print(f"Updating datasources ({len(datasources)}):")

        # Log to each Datasource's log
        for d in datasources:
            d.logger.info("============== Bolt-CMD ==============")
            d.logger.info(f"Started update for {d.name} (by {USER})")
            d.logger.info(f"Args: `{_args}`")

            if d.name in ignore:
                console.print(f"        [yellow]Skipped: {d.name} ([i]ignored[/i])[/]")
                d.logger.info("Ignored (explicitly by user)")
                continue

            do_update = True

            # ----------------------------------------------------------------
            # HASH
            # Do recently updated check
            do_update: bool = True
            #if not force:
            if skip:
                d.logger.info("Comparing hashes")
                do_update = WAREHOUSE.compare_hashes(d)

            if not do_update:
                console.print(f"        [yellow]Skipped: {d.name} (unchanged)[/]")
                d.logger.info("Update skipped (source files unchanged)")
                continue

            # ----------------------------------------------------------------
            # UPDATE
            try:
                #with console.status(f"[cyan]      Updating {d.name}...[/]"):
                d.logger.info("Calling update command")
                # ===== Do the Update =====
                d.update(
                    WAREHOUSE,
                    download=download,
                    read_cache=read_cache,
                    validate=validate,
                    write_cache=write_cache,
                    console=console,
                    **kwargs
                )
                # TODO: handle misc post-load callbacks
                # Confirm load success
                # with console.status(f"{d.name}: Confirming load..."):
                #     if (
                #         len(d.data) == 1 and d.name not in WAREHOUSE.list_tables()
                #     ):  # TODO: not the best way to measure...
                #         d.logger.critical("FAILURE: Table load could not be confirmed")
                #         raise duckdb.DataError("Table does not exist after attempting load")
                #     d.logger.info("Table load confirmed")
                if d.raw_data_origin == RawDataOrigin.FROM_CACHE:
                    console.print(
                        f"        [green]Updated: {d.name}[/]  [bright_black](C:{d.cache_read_time} | L:{d.load_time})[/]"
                    )
                else:
                    console.print(
                        f"        [green]Updated: {d.name}[/]  [bright_black](E:{d.extract_time} | T:{d.transform_time} | L:{d.load_time})[/]"
                    )
                WAREHOUSE.logger.info(f"Loaded {d.name}")
            except Exception as e:
                d.logger.critical(f"{e}")
                errors.append((d.name, e))
                console.print(f"        [red]Failed:  {d.name}[/]")
                if not ignore_errors:
                    raise e
            finally:
                d.logger.info("Complete")
            # ----------------------------------------------------------------
            tables_loaded += 1
        console.print(f"    Tables Loaded: {tables_loaded}")
        if ignore:
            console.print(f"    Ignored: [yellow]{len(ignore)}[/]")

    if len(datasources) > 0:
        console.print()

    # ========================================================================
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
        update_warehouse(console, WAREHOUSE, compact=compact)

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
