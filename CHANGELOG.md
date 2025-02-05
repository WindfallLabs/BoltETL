# Changelog
[Changelog Reference](https://common-changelog.org/)

## [0.2.0] - 2025-01-31
_Second Alpha Release Notes_
Added:

Changed:
- Now primarily leverages `polars` rather than `pandas`
- The `extract` method uses polar's lazy mode (by default) to load raw data (but not for xlsx)
- Cached data uses the `write_icp` and ".arrow" extension
