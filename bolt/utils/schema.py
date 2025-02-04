"""Functions related to dataframe schemas."""
import polars as pl
import pandas as pd


def apply_dtypes(
        df: pl.DataFrame,
        schema: pl.Schema | tuple[tuple[str, pl.DataType]],
        sort = True,
        ignore_missing = True,
        #drop_nonetypes = False,
        date_format: str = "%m/%d/%Y"
    ) -> pl.DataFrame:
    """Casts existing columns of a DataFrame to the specified dtypes in a given schema.
    Ignores columns in the schema that don't exist.
    Args:
        df: the dataframe
        schema: a tuple of tuple(column_name, datatype)
        ignore_missing: Whether to throw and error over missing columns or not
        drop_nonetypes: (boolean) Whether or not to drop columns with dtypes of None
          This allows users to specifically drop existing columns by setting a None dtype.
    """
    # Handle pl.Schema objects
    if type(schema) == pl.Schema:
        schema = tuple(zip(schema.names(), schema.dtypes()))
    # Expressions to execute in `df.with_columns`
    expressions = []
    # Final cols to keep (sorted)
    select_columns = []
    for col, dtype in schema:
        # Ignore missing
        if col not in df.columns and ignore_missing:
            continue
        # Get dtype name (str)
        try:
            dtype_name: str = dtype.__name__
        except AttributeError:
            dtype_name: str = dtype.__class__.__name__
        # Keep None dtypes, but ignore casting
        #if col in df.columns and dtype is None and not drop_nonetypes:
        if col in df.columns and dtype is None:
            select_columns.append(col)
            continue
        # Drop: Do not add columns to final selection if dtype is None
        #if col in df.columns and dtype is None and drop_nonetypes:
        #    continue
        # Add column to final dataframe selection order
        select_columns.append(col)

        # Get column dtype
        orig_dtype: pl.DataType = df.dtypes[df.columns.index(col)]
        # Ignore columns that don't need casting
        if dtype == orig_dtype:
            continue

        # Cast types
        if dtype is None:
            raise TypeError("Cannot cast type 'None'")
        # Handle conversions from string
        if orig_dtype == pl.String:
            if dtype_name.startswith("Float") or dtype_name.startswith("Int"):
                #  Remove comma if cast to float|int
                expressions.append(pl.col(col).str.replace(",", "").cast(dtype))
            elif dtype_name.startswith("Duration"):
                # Durations must be parsed using pandas.to_timedelta, and converted back to pl.Series
                dur_expr = pl.Series(
                    # Polars expects 'us', pandas returns 'ns'
                    pd.to_timedelta(df.to_pandas()[col])
                ).cast(dtype)
                expressions.append(dur_expr)
            elif dtype_name.startswith("Date"):
                expressions.append(pl.col(col).str.strptime(pl.Date, date_format))
            # TODO: elifs for other casting corrections here??
        else:
            expressions.append(pl.col(col).cast(dtype))

    # Create returned dataframe
    df = df.with_columns(
            *expressions
    )
    if sort:
        df = df.select(select_columns)
    return df


def apply_sorting(
        df: pl.DataFrame,
        schema: pl.Schema | tuple[tuple[str, pl.DataType]],
        drop_nonetypes = True
) -> pl.DataFrame:
    """Sorts a dataframe using the provided schema."""
    if drop_nonetypes:
        return df.select(*[i[0] for i in schema if i[1] is not None])
    return df.select(*[i[0] for i in schema])


def validate(
        df: pl.DataFrame,
        schema: pl.Schema | tuple[tuple[str, pl.DataType]],
        sort = True,
        drop_nonetypes = True
) -> None:
    """Asserts that dataframe schema and expected schema are equal."""
    if type(schema) == pl.Schema:
        expected: tuple[tuple[str, pl.DataType]] = tuple(zip(schema.names(), schema.dtypes()))
    else:
        expected = schema
    # Drop columns with dtype of None
    if sort:
        df = apply_sorting(df, expected, drop_nonetypes=drop_nonetypes)
    actual = tuple(zip(df.columns, df.dtypes))
    diff = set(expected).difference(set(actual))
    if diff:
        raise pl.exceptions.SchemaError(f"Schemas are not equal: {diff}")
    return df


def check_names(
        df: pl.DataFrame,
        schema: pl.Schema | tuple[tuple[str, pl.DataType]],
        ignore_missing: list[str] | None = None,
        ignore_expected: list[str] | None = None
    ) -> None:
    """Compares actual column names with names provided to schema."""
    if type(schema) == pl.Schema:
        expected: set = set(schema.names())
    else:
        expected: set = {i[0] for i in schema}
    actual = set(df.columns)
    
    # Handle optional ignored values
    if ignore_missing:
        missing = {i for i in expected.difference(actual) if i not in ignore_missing}
    else:
        missing = {i for i in expected.difference(actual)}
    if ignore_expected:
        not_expected = {i for i in actual.difference(expected) if i not in ignore_expected}
    else:
        not_expected = {i for i in actual.difference(expected)}

    # Raise errors (conditionally)
    if missing:
        raise pl.exceptions.ColumnNotFoundError(f"Missing columns: {missing}")
    if not_expected:
        raise pl.exceptions.SchemaError(f"Unexpected columns: {not_expected}")
    return
