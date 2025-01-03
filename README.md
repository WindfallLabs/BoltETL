# BoltETL
_Author: Garin Wally; 2024-12-09_

## Mission
> _To best optimize our data infrastructure, automate common data manipulation and reporting functions, and set standardized procedures for cataloging, documenting, and stewarding datasets._

## Basics
"BoltETL" (Extract, Transform, Load) is, in essence, a command line utility intended to assist in data processing pipelines and reporting. It does this by using Python classes to wrap data and their [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) procedures which can be imported and manipulated in a modular, standardized, documented, and intuitive manner.


The goals of this project are to:
- Catalog and organize datasets used by MUTD
- Encapsulte ETL processes within modular Datasource objects
    - Automate, when possible, the retrieval of data (from static URLs, etc.)
    - Codify and automate data transformations
- Utilize fast read/write functionality (e.g. PyArrow)
- Cache transformed data (e.g. Feather) to avoid unnecessarily reprocessing datasets
- Encapsulate lists of data used in various reports as Report objects.
- Track when data was last updated


## Python Packages (WIP)
MAYBE BoltETL provides _multiple_ Python modules:
- `datasources`
    - e.g. `from datasources import S10`

- `reports`
- `utils`


## Use (WIP)

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
