# BoltETL
_Author: Garin Wally; 2024-12-09_
_Developed with love and support from the Missoula Urban Transit District_

BoltETL is a data-processing utility and [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) framework intended for solo data analysts and small teams. Currently built with [pandas](https://pandas.pydata.org/) (until I'm more comfortable with [polars](https://pola.rs/)) and Python (one of the world's [most popular](https://spectrum.ieee.org/top-programming-languages-2024) programming languages).

Transform diverse data sources into standardized, Pythonic objects through custom `Datasource` classes and a simple [TOML](https://toml.io/en/) configuration file - whether you're working with Excel reports, CSV files, or spatial data. Define exactly how your data is retrieved, updated, validated, and exported. Leverage high-performance "feather" and DuckDB filetypes for storing and caching both spatial and non-spatial data. Enjoy the simplicity of running your data-tasks using a well-documented-and dead-simple command line utility. No containers, no web servers, no enterprise infrastructure, just Python.

BoltETL is:
- Flexible, single-tool data solution
- Highly customizable
- Scalable for any quantity or complexity
- Free and Open Source
- Supports tabular and spatial data (using [geopandas](https://geopandas.org/en/stable/getting_started/introduction.html))
- Adaptable to your unique data challenges

BoltETL is _not_:
- A replacement for enterprise data pipelining
- An out-of-the-box solution


## Installation and Setup (WIP)
If you are unfamiliar with `git`, download this repository from the "Releases" page.
...


## Getting Started (WIP)
...


## Reports (WIP)

```cmd
invoke no-shows 2024-01-01 2024-01-31

```


## Example and Explanation (WIP)
A `datasource` is a Python class that encapsulates:
- The path to, and the loaded raw data (one or more inputs)
    - Loaded as `pandas.DataFrame` or `geopandas.GeoDataFrame` objects
- The processed data
    - Loaded as `pandas.DataFrame` or `geopandas.GeoDataFrame` (spatial) objects
- The logic for how to process, make fixes, and prepare it
- Writing and loading from "cached" or preprocessed data file for quick loading later
    - e.g. "feather" files
- And other data-specific rules (WIP)

For example, let's look at Missoula County Parcels:
- We first import the `Parcels` datasource, and instantiate it with `parcels = Parcels()`
- We could call `parcels.download()` to update the raw data from the source website
    - _NOTE: that this `download` method hasn't been implemented yet_
- Then call `parcels.extract()` to pull data out of the shapefile and into the `parcels.raw` class attribute
    - which is a `geopandas.GeoDataFrame` for in-memory processing
- Then the `transform()` method is used, which codifies and executes what a human might have to do manually to pre-process the data. For example:
    - Since shapefiles truncate column names to 11 characters, we might want to rename them
        - e.g. `parcels.rename({"PARCELID": "ParcelID"}, axis=1, inplace=True)`
    - We might want to reproject the "geometry" column to use feet rather than meters
        - e.g. `parcels.to_crs("epsg:6515")`
    - We might want to re-cast the data types to something that works better and faster
        - (e.g. string arrays like "PARCELID" use numpy object arrays by default which don't handle null values; so casting to pyarrow strings allows not only an 84% speed increase, but requires less type-casting boilerplate code)
    - Other processing, etc.
    - And at the end of this method, the processed data is returned as the `parcels.data` attribute (so for those following along, both raw data and processed data are available from the object)

In summary, downloading and processing Missoula County Parcels would look like this:

```python
from datasources import Parcels

parcels = Parcels()  # Initialize or instantiate object

parcels.download()  # Get data (file) from source
parcels.extract()  # Load raw data into memory
parcels.transform()  # Do the processing
parcels.write_cache()  # Cache the processed data for later

# Or
#parcels.update()  # Which could handle the calls to download, extract, transform, and cache

```

and loading the processed and cached data would then look like this:

```python
from datasources import Parcels

parcels = Parcels()  # Initialize or instantiate object
parcels.read_cache()

# Then do whatever with it here...

```
