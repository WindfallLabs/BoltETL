# Data Definitions

## Folder Schema and Organization Guidelines

- `/Data` - parent folder; entry point for all data used by the agency
    - `/__datasources__` - A Python module (contains an `__init__.py`) that defines the custom, user-defined subclasses of `bolt.datasources.Datasource`
        - Deletion: Not considered delete-safe. _Should be version-controled with git!_ (or similar)
        - Use: BoltETL loads custom Datasource classes from this location.
    - `/raw` - Contains (raw/unprocessed) data, often downloaded from external sources
        - Deletion: Not considered delete-safe, since getting raw data again might be a pain
        - Use: Primarily for machine-use/automations, Data may and may not be intended for human users
        - e.g. Ridecheck+ reports, Via reports, etc.
        - e.g. City/County data; non-agency / third-party data
        - e.g. DOR data, Data from the Montana State Library, etc.
    - `/primary` - Contains agency-managed data and/or data that is edited manually
        - Deletion: Not delete-safe; it is assumed that data _cannot_ be recreated
        - Intended Use: Both human-use (e.g. in GIS, Excel, etc.) and machine-use (automations/processing)
        - e.g. Routes, Stops, etc.
        - e.g. `FY2024 Statistics.xlsx`
    - `/enriched` - Contains data that is derived from other data; i.e. is the product of processing
        - Deletion: Somewhat delete-safe; deleted files can be recreated (although, possibly not easily), but _will likely cause side effects_
        - Intended Use: Human-readable formats only
        - Alternate name: `exports`, `derived`
        - Unlike the `cached` folder, data here _is_ intended for use by human users
        - e.g. Paratransit Boundary (buffered and unioned Routes)
        - e.g. Human-readable Excel files, etc.
    - `/cached` - Contains processed data in order to potentially skip re-processing
        - Delete-safe; all data here can _quickly and easily_ be recreated; file deletion has no side effects
        - Alternate name: `tmp`; could be used to store temporary outputs
        - Unlike the `derived` folder, data here is _not_ intended for use by human users
        - Contains machine-readable file formats (e.g. feather files, SQLite databases, etc.)
        - e.g. Processed `raw` data (Via reports, Clever reports, etc.)
    - `/scratch` - Temporary workspace for exploratory analysis, manual processing, and just messing about
        - Not delete-safe; but should be emptied often
    - `warehouse.duckdb` - Data Warehouse


- `/Reports` - _WIP_ parent folder for all reports; intended for cross-agency access; "the shelf".
    Report names should follow this format: 'YYYY-MM-DD-ReportName.ext' where the date is the _publication date_
    - `/Ridership`
        - `FY2024-RidershipReport.xlsx` - Fiscal year ridership data
    - `/Electrification`
    - `/AgencyProfile` - "One-Pager"
    - `/AnnualAgencyReport`
    - `/NTD`
    - `/Misc`


## Config
The `config.toml` file is a [TOML](https://toml.io/en/) plain-text file that allows users to define metadata, paths, URLs, etc. for datasources.
The top of the file contains global items, while datasource information should be contained under something like `[data.DatasourceGroupName]` to group datasource information.

Each datasource _must_ have the following [keys](https://toml.io/en/v1.0.0#keyvalue-pair):
- `name` (string) - The name of the datasource.
    - This value determines output filenames, table names, etc; and _must_ match the key (e.g. if the name is "ThisData", the group name should be `[data.ThisData]`)
- `source_dir` (string) - A path to the "source directory"; i.e. the folder that contains the datasource.
- `filename` (string) - The name of a specific file, or, a glob / match string used to open multiple files.


These keys are _optional_:
- `desc` (string; typically a multiline string using triple-double quotes) - Any text to describe the data
- `provider` (string) - The name of the provider of the data
- `use_cache` (boolean) - WIP - If set to 'true', data is loaded from the cache if one exists. If 'false' the `extract()` and `transform()` methods are called when the data is accessed.
