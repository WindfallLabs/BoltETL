"""Datasources Commandline Interface.
Author: Garin Wally; 2024-12-16
For now... might migrate to click.
"""
import datetime as dt
import json
from hashlib import sha256
from pathlib import Path

from invoke import task
from rich.console import Console

import bolt
from bolt.datasources import (
    CR0174,
    Datasource,
    DriverShifts,
    NTDMonthly,
    Parcels,
    RiderAccounts,
    RideRequests,
    S10,
)


console = Console()

# How many days old to consider 'data_inventory.json' "stale"
INVENTORY_STALE = 15


def json_data_inventory() -> dict:
    """Gets contents of the Data folder as a dict/json."""    
    files = [str(i).replace("\\", "/") for i in bolt.config.data_path.rglob("*.*")]
    d = {
        "sha256": sha256(str(files).encode("UTF8")).hexdigest(),
        "last_update": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "files": files
        }
    return d


@task
def inventory(c) -> None:
    """Creates 'data_inventory.json'."""
    with console.status("Inventorying data..."):
        data: dict = json_data_inventory()
        j: str = json.dumps(data, indent=4)
        with open("data_inventory.json", "w") as f:
            f.write(j)
    console.print("[green]Done[/]")
    return


@task
def check_inventory(c) -> None:
    """Compares current Data directory with the inventory JSON."""
    # Load inventory from file
    with open("data_inventory.json", "r") as f:
        file_inventory: dict = json.loads(f.read())
    # Take inventory of actual Data contents
    current_inventory: dict = json_data_inventory()
    # Compare sha256 hashes
    is_synced: bool = current_inventory["sha256"] == file_inventory["sha256"]
    console.print(f"Last Update: [blue]{file_inventory['last_update']}[/]")
    if is_synced:
        # Check how old the inventory is
        recent: bool = (
            dt.datetime.strptime(
                file_inventory["last_update"],
                "%Y-%m-%d %H:%M:%S"
                ) > dt.datetime.now() - dt.timedelta(days=INVENTORY_STALE)
        )
        if recent:
            console.print("Status: [green]Synchronized[/]")
        else:
            console.print("Status: [red]Stale[/]")
    else:
        console.print("Status: [red]Data is out of sync[/]")
        # Compare file lists as sets
        current_files: set[str] = set(current_inventory["files"])
        inventory_files: set[str] = set(file_inventory["files"])
        inv_missing: set[str] = current_files.difference(inventory_files)
        mv_or_rm: set[str] = inventory_files.difference(current_files)
        if inv_missing:
            console.print(f"    Inventory Missing: {inv_missing}")
        if mv_or_rm:
            console.print(f"    Files Missing or Removed: {mv_or_rm}")
    return


@task
def update(c, datasource_name: str):
    DS: Datasource = getattr(bolt.datasources, datasource_name)
    ds = DS()
    with console.status(f"Updating {datasource_name}..."):
        ds.update()
    return


# ============================================================================
# TESTS / DEBUGGING

@task
def test_etl(c):
    errors: list[tuple[str, Exception]] = []
    datasources: list[Datasource] = [
        CR0174,
        DriverShifts,
        NTDMonthly,
        Parcels,
        RiderAccounts,
        RideRequests,
        S10,
    ]
    for D in datasources:
        d = D()
        console.print(f"Testing {d.name}...")
        try:
            d.extract()
            d.transform()
        except Exception as e:
            errors.append((d.name, e))

    console.print(f"\nError Count: {len(errors)}")
    for name, err in errors:
        console.print(f"- [blue]{name}[/]: [red]{err}[/]")
    return


# ============================================================================
# REPORT RUNNERS


@task
def no_shows(c, start: str, end: str):
    """Runs the Para No-Show Report."""
    with console.status("ParaNoShows: Running Report..."):
        rpt = bolt.reports.NoShowReport()
        rpt.load_data()
        rpt.process(start, end)
        rpt.export()
    return
