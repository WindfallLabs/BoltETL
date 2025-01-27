# Changelog
[Changelog Reference](https://common-changelog.org/)

## Feature Checklists
### v0.1.0 Feature Checklist
Datasource Objects:
- [ ] `mutd_tax_districts.py` ...
- [ ] `house_districts.py` ...
- [ ] `senate_districts.py` ...
- [ ] `mutd_1976_boundary.py` ...
- [ ] `mutd_paratransit_boundary.py` ...
- [ ] `mutd_planning_boundary.py` ...
- [/] `cr0174.py:CR0174`
- [/] `drivershifts.py:DriverShifts`
- [/] `parcels.py:Parcels`
- [/] `rcp_ntd_monthly.py:NTDMonthly`
- [/] `ride_requests.py:RideRequests`
- [/] `rider_accounts.py:RiderAccounts`
- [/] `via_s10.py:S10`
- [ ] Command-line utility
    - [ ] `python bolt-cmd.py status`
    - [ ] `python bolt-cmd.py add [<filename>|"."]`
    - [ ] `python bolt-cmd.py update`
    - [ ] `python bolt-cmd.py report [list|info|run] <report> <kwargs>`
    - [ ] `python bolt-cmd.py ftp-push <filename>`
    

Reports:
- [ ] Report runner task (`invoke report <report name>`)
- [/] No-Show Report (bi-monthly)
- [ ] Monthly Ridership Report

To-Do:
- [ ] Documentation via Sphinx
- [ ] Leverage `validate` for testing (completeness?)
- [ ] Populated `load` methods?
- [ ] Match RCP and Via NTD schemas
- [ ] Schema (raw and final) dump-to-txt method; or dump to JSON
- [ ] Ditch `invoke` and replace with `click`
- [X] Remove all agency-specific `Datasource` subclasses
- [ ] Remove all agency-specific `Report` subclasses


## v1.0.0 Feature Checklist
- [ ] 95% Test Coverage


## [0.0.3] - 2025-01-09
- [ ] City Nhoods, Wards
- [ ] MUTD Boundaries
- [ ] House/Senate Districts

Added:
- Created 'bolt-cmd.py' using cyclopts
    - Began development of data-file tracking (git-like `status` and `add` commands)
        - Bug: when file added and 'add' is run, status shows all files & add adds all
        - Bug: Remove doesn't work
    - Began development of `update` command
    - Began developemtn of `report` command
        - Options for `run`, `info`, and `list`
- Began development of a DuckDB data warehouse
    - '.sql' files get executed against the warehouse at creation
- Implemented BaseReport ABC
- Logging available to subclasses of `BaseReport` and `Datasource`
    - Added logging to 'County4.py'
    - Refactored and added logging for 'drivershifts.py'
- Mt DOR Cadastral `County4` object
    - Currently requires external "oriondb" code and update procedure
- CR0174 all raw files downloaded; complete history back to 2023-01
- Added 'NTDMonthly' report
- Report-specific SQL files are stored in "/reports/sql" (not the Python module)
- 'funcs' module in utils for misc funcs ('fiscal_year' for SQL)

Changed:
- Cleaner types module
- Refactored 'ParatransitNoShows' report
- Migrating from 'invoke' to 'cyclopts' for cmd scripts (see added functionality)
- Migrated a bunch of code from the `ParatransitNoShow` report to the `RideRequests.transform` method
    - Fixed datetime type issues causing Penalty Points math

Removed:
- init function replaced with standard `__init__` and `super()`


## [0.0.2] - 2025-01-08
Added:
- `log_dir` option in "config.toml" for log folder

Changed:
- Refactored 'metadata' from datasource class definition into (centralized) config.toml
- Codebase ("/datasources", "/reports", and "/utils" modules) are now under "/bolt"
    - e.g. `import bolt` is now a thing
- All `raw` datasource properties are now `list[tuple[str, pd.DataFrame|gpd.GeoDataFrame]]`
- Misc file organization
    - Created (empty) "/tests" folder
    - Moved "DataDefinitions.md" and "ProcessingGuide.md" into "/doc"
    - Moved "data_inventory.json" out of project; added "inv_path" config value
    - Moved `YearMonth` class, and `to_float` and `to_int` funcs to utils

Removed:
- "/logs" directory (unused, better untracked alongside external "/Data" and "/Reports")
- Datasource classes no longer print to `rich.Console`
- Large decrease of codebase!


## [0.0.1] - 2024-12-09

_2025-01-03_
Added:
- "logs" folder for containing (planned) logging functionality
- "ProcessingGuide.md" - moved instructions for processing data from README to own file
- "config.toml"
    - To consolidate datasource metadata and configuration
    - Planned to replace most content in `__init__()` methods
    - Documented in "DataDefinitions.md"
- "LICENSE.txt" (Mozilla Public License); gitignore; "pyproject.toml" - for versioning
- First `git commit`

Changed:
- NoShowReport:
    - Params passed to "tasks.py:no_shows()" now directly passes params to `NoShowReport.process()` which handles date parsing.
- `uvx ruff check` - "All checks passed!"
    - Imports listed explicitly; unused imports removed

Removed:
- "SourceURLs.md" - content moved into new "config.toml"


_2024-12-19_
Added:
- Fixed bugs with loading raw data into `via_s10.py:S10()` (see 2024-12-16)
    - Removed commented-out junk code (day-counts?)
    - Broke-out grouped raw data in what is now "__old_*.xlsx" files into their own monthly files
    - `extract()` now ignores files that begin with "_"
    - Explicitly removed "Closed" service type -- might need to be done with other models

Changed:
- Reverted raw data and cache paths back to using string literals rather than double "\\"


_2024-12-17_
Added:
- Named this project "BoltETL"
- Created "SourceURLs.md" for (raw) MSL data
- `Datasource` baseclass now uses getters/setters for `raw_path` and `cache_path`
    - Allows for future-proof partial paths, while maintaining backwards compat


Changed:
- Flatted the `datasources` file structure and refactored imports


_2024-12-16_
Added:
- Back-populated Ridecheck+ NTD_MONTHLY data (from 2022-07 to present)
- Created CHANGELOG (moved out of README.md into its own file)
- Implemented new 'invoke' functions:
    - _Para No-Show Report_: `invoke no-shows 20241201 20241215`
    - Data Inventory functions to track file changes in '/Data'
        - `invoke inventory` - write "data_inventory.json"
        - `invoke check-inventory` - compare data with "data_inventory.json"
- Created some `datasource`s
    - `S10`
        - BUG: won't process file when contains data for multiple months
    - `NTDMonthly`
- Created a `YearMonth` class

Changed:
- Revised `servicedays.py` to replace the "Normal" service type
- Cleaned up imports (removed `sys.path.append`)

Removed:
- `Datasource.get_yearmonth()` in favor of `YearMonth.from_filepath()`


_2024-12-13_
Added:
- `datasource` objects:
    - `RiderAccounts`
    - `RideRequests`


_2024-12-11_
Added:
- Created DataDefinitions.md (documents file structure)


_2024-12-10_
Added:
- README.md
	- Top material (Basics, Technicalities, etc.)
	- Datasource Processing Guide
	- Changelog
- Back-populated all data (since 2023-02 or so) for
	- Via: Ride Requests
	- Via: Driver Shifts
	- Via: NTD S-10
- Populated calendar year 2024 CR0174 data (2024-01 to 2024-06)
- Added initial "Rider Account.xlsx" file (for overwriting bi-monthly)
- Placeholder files for '/primary'
- Added `datasource` objects:
    - `CR0174` - working
    - `DriverShifts` - working


_2024-12-09_
Added:
- Began outlining and conceiving project in ernest.
- Created file structure
- Copied FY25 data (2024-07 to 2024-10) for
	- CR0174
	- Via S-10
	- Via DriverShifts
	- NTD Monthly
