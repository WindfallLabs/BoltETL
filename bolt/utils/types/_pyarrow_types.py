"""PyArrow Types."""

import pandas as pd
import pyarrow as pa

# [reference](https://arrow.apache.org/docs/python/generated/pyarrow.string.html)
# Dates
pyarrow_date = pd.ArrowDtype(pa.date32())
pyarrow_datetime = pd.ArrowDtype(pa.date64())
pyarrow_duration = pd.ArrowDtype(pa.duration("s"))
pyarrow_time = pd.ArrowDtype(pa.time32("ms"))

# String
pyarrow_string = pd.ArrowDtype(pa.string())

# Ints
pyarrow_uint16 = pd.ArrowDtype(pa.uint16())
pyarrow_uint32 = pd.ArrowDtype(pa.uint32())
pyarrow_uint64 = pd.ArrowDtype(pa.uint64())

# Floats
pyarrow_float32 = pd.ArrowDtype(pa.float32())
pyarrow_float64 = pd.ArrowDtype(pa.float64())

__all__ = [
    "pyarrow_date",
    "pyarrow_datetime",
    "pyarrow_duration",
    "pyarrow_string",
    "pyarrow_time",
    "pyarrow_uint16",
    "pyarrow_uint32",
    "pyarrow_uint64",
    "pyarrow_float32",
    "pyarrow_float64",
]
