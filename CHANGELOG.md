# Changelog
[Changelog Reference](https://common-changelog.org/)

## [0.1.0] - 2025-01-30
_Initial Alpha Release Notes_
Stable Features:
- Configuration (TOML) file - `bolt` looks for a path to a .toml file in the "%BOLT-CONFIG%" environment variable
- `bolt-cmd.py` - Git-like command utility for controlling data processes
    - `bolt-cmd.py update` - Updates datasources
    - `bolt-cmd.py report` - Executes reports
- `bolt.datasources`
    - `Datasource` - Abstract class to help users construct standardized datasource objects
- `bolt.reports`
- `bolt.utils`
    - `bolt.utils.types` - contains short-hand idenifiers for PyArrow types
        - (e.g. `pandas.ArrowDtype(pyarrow.string())` == `bolt.utils.types.pyarrow_string`)
        - `bolt.utils.types.conversion` - type cleaning and conversion functions (pandas.Series to PyArrow types)


Unstable Features:
- Logging - Usable, but likely to change
- Loading of SQL files (from as a datasource vs report)
- `bolt.warehouse` - The data warehouse (DuckDB) storage is likely to get refactored


Planned/Non-Functional Features:
- `bolt-cmd.py status` - Buggy; future version will use database instead of JSON
- `bolt-cmd.py add` - see above

Misc:
- Moved from micro-version development versioning (e.g. "0.0.3-dev") to minor release versioning (0.1.0)
- Previous development versions were developed in tandem with now-removed MUTD business-specific logic
    - Seperated out all MUTD business logic (datasources, reports, config, docs)
        - Removed DataDefinitions.md (_TODO_: replace)
        - Removed ProcessingGuide.md
- Heavily edited CHANGELOG file

_v1.0.0 Feature Checklist_
- [/] Schema (raw and final) dump-to-txt method; or dump to JSON
- [ ] Documentation via Sphinx
- [ ] Leverage `validate` for testing (completeness?)
- [ ] Populated `load` methods?
- [ ] 95% Test Coverage
- [/] Command-line utility (using `cycopts`)
    - [/] `python bolt-cmd.py status`
    - [/] `python bolt-cmd.py add [<filename>|"."]`
    - [x] `python bolt-cmd.py update`
    - [x] `python bolt-cmd.py report [list|info|run] <report> <kwargs>`
    - [ ] `python bolt-cmd.py ftp-push <filename>`


## [0.0.3] - 2025-01-09
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
- Report-specific SQL files are stored in "/reports/sql" (not the Python module)
- 'funcs' module in utils for misc funcs ('fiscal_year' for SQL)

Changed:
- Cleaner `bolt.utils.types` module
- Migrating from 'invoke' to 'cyclopts' for cmd scripts (see added functionality)
- `bolt.utils.types.conversion` changes and additions
- How `bolt.datasources` load custom `Datasource` subclasses
- How `bolt.reports` load custom `ReportBase` subclasses

Removed:
- init function replaced with standard `__init__` and `super()`
- Most MUTD business logic


## [0.0.2] - 2025-01-08
Added:
- `log_dir` option in "config.toml" for log folder

Changed:
- Refactored 'metadata' from datasource class definition into (centralized) config.toml
- Codebase ("/datasources", "/reports", and "/utils" modules) are now under "/bolt"
    - e.g. `import bolt` is now a thing
- All `raw` datasource properties are now `list[tuple[str, pd.DataFrame|gpd.GeoDataFrame]]`
- Misc file organization
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
Changed:
- Reverted raw data and cache paths back to using string literals rather than double "\\"


_2024-12-17_
Added:
- Named this project "BoltETL"
- `Datasource` baseclass now uses getters/setters for `raw_path` and `cache_path`
    - Allows for future-proof partial paths, while maintaining backwards compat

Changed:
- Flatted the `datasources` file structure and refactored imports


_2024-12-16_
Added:
- Created CHANGELOG (moved out of README.md into its own file)
- Implemented new 'invoke' functions:
    - Data Inventory functions to track file changes in '/Data'
        - `invoke inventory` - write "data_inventory.json"
        - `invoke check-inventory` - compare data with "data_inventory.json"
- Created some `datasource`s
- Created a `YearMonth` class

Changed:
- Revised `servicedays.py` to replace the "Normal" service type
- Cleaned up imports (removed `sys.path.append`)

Removed:
- `Datasource.get_yearmonth()` in favor of `YearMonth.from_filepath()`


_2024-12-09_
Added:
- Began outlining and conceiving project in ernest.
- Created file structure
