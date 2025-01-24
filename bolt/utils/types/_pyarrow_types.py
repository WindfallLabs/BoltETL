"""PyArrow Types."""

import pandas as pd
import pyarrow as pa

# [reference](https://arrow.apache.org/docs/python/generated/pyarrow.string.html)
pyarrow_date = pd.ArrowDtype(pa.date32())
pyarrow_datetime = pd.ArrowDtype(pa.date64())
pyarrow_duration = pd.ArrowDtype(pa.duration("s"))
pyarrow_time = pd.ArrowDtype(pa.time32("ms"))

pyarrow_string = pd.ArrowDtype(pa.string())

pyarrow_uint16 = pd.ArrowDtype(pa.uint16())

pyarrow_float32 = pd.ArrowDtype(pa.float32())
