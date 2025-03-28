# Changelog
[Changelog Reference](https://common-changelog.org/)

## [0.3.0] - 2025-03-06
_Third Alpha Release Notes_

Added:
- A `Warehouse` object
- User-defined instances of `Datasource` and `Report` are lazy-loaded into their respective registries
- User-defined objects are lazy-loaded into `bolt.env` which must be imported explicitly
- A graph-based SQL script sorter (who doesn't love a good DAG?)
- `Config` class (supports multiple environments)
- Tests

Changed:
- Complete overhaul of the API; users now use decorated functions instead of classes

Removed:
- No longer depends on user-defined config (`utils._config`)
- Removed all dependence on `utils.servicedays`


## [0.2.0] - 2025-01-31
_Second Alpha Release Notes_
Added:

Changed:
- Now (primarily) leverages `polars` rather than `pandas`
- The `extract` method uses polar's lazy mode (by default) to load raw data (but not for xlsx)
- Cached data uses the `write_icp` and ".arrow" extension
- Began replacing `bolt.utils.servicedays` functions with SQL table "dim_calendar"
