#!/usr/bin/env python
from pathlib import Path

import cyclopts
from rich.console import Console

console = Console()
app = cyclopts.App()

SCRIPT = Path(__file__).name

TEST_FILES = [
    r"C:\Workspace\tmpdb\Data\raw\CR - CR0174\202301-Weekday-CR-0174 Incident Adjusted Distance and Time - Jan 13 2025.csv",
    r"C:\Workspace\tmpdb\Data\raw\CR - CR0174\202301-Saturday-CR-0174 Incident Adjusted Distance and Time - Jan 13 2025.csv",
    r"C:\Workspace\tmpdb\Data\raw\CR - CR0174\202301-Sunday-CR-0174 Incident Adjusted Distance and Time - Jan 13 2025.csv",
]


@app.command
def add(datafile: str):
    """Add data files to tracked data."""
    add_all = False
    if datafile in {".", "*"}:
        add_all = True
    # TODO: support wildcard globs like:
    # `python bolt-cmd.py add 202301-Weekday-*.csv`
    console.print(f"        [green]{'added:'.ljust(11)} {datafile}[/]")
    return


@app.command
def status():
    console.print("Changes to data files:")
    console.print(f'  (use "python {SCRIPT} add <file>..." to register new files)')
    for i in TEST_FILES:
        status = "new file:"
        #status = "modified:"
        #status = "removed:"
        console.print(f"        [red]{status.ljust(11)} {Path(i).name}[/]")
    return


@app.command
def report(rpt_name: str):
    ...


@app.command
def update(datasource: str):
    ...





if __name__ == "__main__":
    app()
