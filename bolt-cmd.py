from fnmatch import fnmatch
from pathlib import Path
from typing import Literal

import cyclopts
from rich.console import Console

import bolt
from bolt.utils.file_tracker import FileTracker
#from bolt.reports import reports


console = Console()
app = cyclopts.App()

tracker = FileTracker()


SCRIPT = Path(__file__).name
WAREHOUSE = "data_warehouse.duckdb"

DATASOURCES: list[bolt.datasources.Datasource] = []
for ds in bolt.config.metadata.keys():
    try:
        DATASOURCES.append(getattr(bolt.datasources, ds))
    except AttributeError:
        pass

REPORTS: list = [

]


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
    console.print("Changes to data files:")  # TODO: always prints, even if status is 'No changes'
    console.print(f'  (use "python {SCRIPT} add <file>..." to register new files)')

    # Print changes
    if changes['new_files']:
        status = "new file:"
        for file in sorted(changes['new_files']):
            console.print(f"        [red]{status.ljust(11)} {file}[/]")
    
    if changes['modified']:
        status = "modified:"
        for file in sorted(changes['modified']):
            console.print(f"        [red]{status.ljust(11)} {file}[/]")
    
    if changes['removed']:
        status = "removed:"
        for file in sorted(changes['removed']):
            console.print(f"        [red]{status.ljust(11)} {file}[/]")
    
    if not any(changes.values()):
        console.print("\n[green]No changes detected.[/]")

    return


@app.command
def report(option: Literal["list", "info", "run"], rpt_name: str="", **kwargs):
    """Execute a report by report class name with kwargs.
    Use `report-info <report name>` for details about a report.
    """
    # NOTE: list option does not require 'rpt_name'
    if option == "list":
        console.print("Reports available (in 'bolt.reports.__all__'):")
        for rpt_name in bolt.reports.__all__:
            console.print(f"        [blue]{rpt_name}[/]")
        return

    if not rpt_name:
        raise AttributeError("'rpt_name' argument is required")

    Rpt = getattr(bolt.reports, rpt_name)  # Get python class by name
    rpt = Rpt()
    if option == "info":
        console.print(f"{rpt.run.__doc__}")
        return

    if option == "run":
        console.print(f"Running report: {rpt_name}...")
        console.print(f"        (kwargs={kwargs})")
        console.print(f"        ...", end="\r")
        try:
            rpt.run(**kwargs)
            console.print(f"        Exported results to '{rpt.out_path}'")
        except Exception as e:
            console.print(f"        [red]Failed: {e}")
    return


@app.command
def task(option: Literal["list", "add", "remove"], name: str="", when: str="") -> None:
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


@app.command
def update(datasource_name: str, force: bool=False, skip_db: bool=False):
    """Updates datasource by name, or all configured datasources ('.').
    Alternatively, update only the data warehouse using 'db'.
    """
    if datasource_name.lower() in ("db",):
        datasources = []
    else:    
        datasources: list[bolt.datasources.Datasource] = (
            [getattr(bolt.datasources, datasource_name)] if datasource_name != "."
            else DATASOURCES
        )

    errors: list[tuple[str, Exception]] = []
    if datasources:
        if force:
            console.print(f"Updating datasources (force=True):")
        else:
            console.print(f"Updating datasources:")

        for D in datasources:
            d = D()
            console.print(f"        {d.name}...", end="\r")
            try:
                d.update()
                console.print(f"        [green]Updated: {d.name}[/]")
            except Exception as e:
                errors.append((d.name, e))
                console.print(f"        [red]Failed: {d.name}[/]")

    # Update database
    if len(errors) > 0:
        skip_db = True
    if skip_db:
        console.print(f"\nDatabase update skipped.")
    else:
        console.print(f"\nUpdating database:")
        try:
            tables_loaded, sql_file_count, compact_msg = bolt.datasources.warehouse.update_db(compact_db=True)
            console.print(f"        [green]Updated: {WAREHOUSE}[/]")
            console.print(f"            Tables Loaded: {tables_loaded}")
            console.print(f"            SQL Files Executed: {sql_file_count}")
            console.print(f"            {compact_msg}")
        except Exception as e:
            errors.append((WAREHOUSE, e))
            console.print(f"        [red]Failed: {WAREHOUSE}[/]")

    console.print(f"\nErrors: {len(errors)}")
    for name, err in errors:
        console.print(f"- [blue]{name}[/]: [red]{err}[/]")
    return


if __name__ == "__main__":
    try:
        app()
    except Exception:
        console.print_exception()
