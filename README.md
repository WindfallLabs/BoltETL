# BoltETL
_Author: Garin Wally; 2024-12-09_  
_Developed with love and support from the Missoula Urban Transit District_

__NOTICE:__ This project is in active development. However, it is rapidly stabilizing

## Overview

BoltETL is a lightweight, pure-Python data processing framework designed to support locally-executed workflows (both [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) and ELT), reporting, and storage. Its main goal is to simplify the process of moving small- to medium-scale datasets from various sources into a local [DuckDB](https://duckdb.org/) data warehouse. BoltETL is ideal for solo data analysts or small teams who need to work with moderate volumes of data locally and without the complexity or cost of enterprise-grade orchestration systems.

BoltETL is an excellent option for those who want to transition away from doing data processing in Jupyter Notebooks or for those who wish to add more structure and automation to their ad-hoc Python scripts.  

BoltETL may compare to other projects, such as:
- [Luigi](https://luigi.readthedocs.io/en/stable/)
- Apache Airflow
- dbt
- Spark


## Quick Start
### Installation
Currently unavailable on PyPI.  
If you are unfamiliar with `git`, download this repository from the "Releases" page.
...


### Define Your Datasources
The `Datasource` is the key entry point into using the BoltETL framework. A Datasource could be a pile of Excel files, a response from an API, or even a statically defined DataFrame; BoltETL doesn't care. Datasources define how the data is retrieved (Extract), how it is manipulated or prepared (Transform), and how it gets entered into the data warehouse (Load). Each of these steps are executed in order, and are independent and isolated from other Datasources. Thus, there are no dependencies at this stage.  
Let's have a look at an example:  

__s_and_p.py__
```python
import polars as pl  # bring your own dataframe library
from bolt import Datasource, Metadata

metadata = Metadata(description="Just some easily accessible sample data")

s_and_p = Datasource(
    name="S&P500",  # The name of the resulting table
    metadata=metadata  # a simple, yet flexible metadata object
)


@s_and_p.extract_wrapper
def extract(obj, *args, **kwargs):  # 'obj' here is synonomus with 'self' or the 's_and_p' object
    """Define your extraction process here."""
    # Do as little processing here as necessary
    # Data imports, API calls, etc.
    obj.logger.info("Retrieving data")  # Built-in logging object writes to "S&P500.log"
    from great_tables.data import sp500

    return sp500  # return your data (becomes s_and_p.raw_data)


@s_and_p.transform_wrapper
def transform(obj, *args, **kwargs):
    """Define the process to transform raw_data into data."""
    # Do data processing here; accessing 'raw_data' from the 'obj' param
    transformed = pl.from_pandas(obj.raw_data)  # ...

    return transformed  # return your data (becomes s_and_p.data)


@s_and_p.load_wrapper
def load(obj, warehouse, *args, **kwargs):
    """Define the process to load your data into DuckDB."""
    # This step is optional as Datasource's `_default_loader` is the same as below
    df = obj.data
    warehouse.sql("CREATE OR REPLACE TABLE AS SELECT * FROM df")
    return

```

Since the example above is so trivial, we could use the `data_wrapper` decorator to directly set the data instead:

```python
from bolt import Datasource

s_and_p_direct = Datasource(name="S&P500 Direct", source_dir=None, source_filename=None)


@s_and_p_direct.data_wrapper
def set_data(obj, *args, **kwargs):
    # This method is useful when extraction and transformation are trivial/overkill
    # This single function is all that is needed
    from great_tables.data import sp500

    df = pl.from_pandas(sp500)
    return df  # return your data (becomes s_and_p_direct.data)

```

### Call `bolt-cmd.py`


## Reports (WIP)
BoltETL `Report` objects configure how data is exported from the data warehouse to output graphs, tables, and spreadsheets.  

__Call with `bolt-cmd`:__  
```cmd
python bolt-cmd.py report run NoShowReport --start=2024-01-01 --end=2024-01-31
```


## Advanced Usage
### Environment Configuration (WIP)
A BoltETL environment is simply a folder containing user-defined `Datasource`, `Report`, `Warehouse`, and SQL files.  
BoltETL supports multiple environments using a simple [dotenv](https://github.com/theskumar/python-dotenv) file.  

Say you are an independent contractor working for multiple agencies. You would likely wish to separate your projects by client:

__Setup__  
```python
import bolt

bolt.Config.add_env("client1", r"C:\workspace\Client1", activate=True)  # Activates the env
bolt.Config.add_env("client2", r"C:\workspace\Client2")
```

This setup would set `bolt-cmd.py` to use the objects in the "Client1" path each time it's used.  
To change it, use the following. Environments need to be activated each time you wish to swtich them.  

__Set for use with `bolt-cmd`:__  
`python bolt-cmd.py env activate client2`  
