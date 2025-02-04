"""Functions related to dataframe schemas."""
import polars as pl
import pandas as pd


def apply_dtypes(
        df: pl.DataFrame,
        schema: tuple[tuple[str, pl.DataType]],
        sort = True,
        ignore_missing = True,
        date_format: str = "%m/%d/%Y"
    ) -> pl.DataFrame:
    """Casts existing columns of a DataFrame to the specified dtypes in a given schema.
    Ignores columns in the schema that don't exist.

    Parameters
    ----------
    df : pl.DataFrame
        The dataframe to apply dtypes.
    schema : tuple[tuple[str, pl.DataType]]
        A tuple of (column_name, datatype) that define the dtypes for the resulting dataframe.
    sort : boolean (default True)
        Whether or not to sort resulting dataframe by column order in schema.
    ignore_missing : boolean (default True)
        Whether to throw an error over missing columns or not.

        Examples
        --------
        >>> df = pl.DataFrame({"a": [1,2,3], "b": ["one", "two", "three"]})
        >>> schema = (("a", pl.String), ("b", pl.String))
        >>> apply_dtypes(df, schema)
        pl.DataFrame({"a": ['1', '2', '3'], "b": ["one", "two", "three"]})

    """
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
        if col in df.columns and dtype is None:
            select_columns.append(col)
            continue
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
        schema: tuple[tuple[str, pl.DataType]],
        drop_nonetypes = True
) -> pl.DataFrame:
    """Sorts a dataframe using the provided schema.
    
    Parameters
    ----------
    df : pl.DataFrame
        DataFrame to sort columns.
    schema : tuple[tuple[str, pl.DataType]]
        Collection that specifies order of columns for resulting df.
    drop_nonetypes : boolean (default True)
        Whether or not to drop columns not specified by the schema.

        Examples
        --------
        >>> df = pl.DataFrame({
            "a": [1,2,3],
            "b": ["one", "two", "three"],
            "y": [0, 0, 0],
            "z": [4,5,6]}
        )
        >>> schema = (("z", pl.Int8), ("a", pl.Int8), ("b", pl.String))
        >>> apply_sorting(df, schema)
        pl.DataFrame({"z": [4,5,6], "a": [1,2,3], "b": ["one", "two", "three"]})
    """
    if drop_nonetypes:
        return df.select(*[i[0] for i in schema if i[1] is not None])
    return df.select(*[i[0] for i in schema])


def validate(
        df: pl.DataFrame,
        schema: tuple[tuple[str, pl.DataType]],
        sort = True,
        drop_nonetypes = True
) -> None:
    """Asserts that dataframe schema and expected schema are equal."""
    expected = set(schema)
    # Drop schema items where the dtype is None
    if drop_nonetypes:
        expected = {i for i in schema if i[1] is not None}
    # Drop columns with dtype of None
    if sort:
        df = apply_sorting(df, schema, drop_nonetypes=drop_nonetypes)
    actual = set(tuple(zip(df.columns, df.dtypes)))
    diff = expected.difference(actual)
    if diff:
        raise pl.exceptions.SchemaError(f"Schemas are not equal: {diff}")
    return df


def check_names(
        df: pl.DataFrame,
        schema: tuple[tuple[str, pl.DataType]],
        ignore_missing: list[str] | None = None
    ) -> None:
    """Compares actual column names in dataframe to column names provided
    in schema.
    
    Parameters
    ----------
    df : pl.DataFrame
        DataFrame to check columns.
    schema : tuple[tuple[str, pl.DataType]]
        Collection used to create a set of column names to compare with df.
    ignore_missing : list[str]
        A list of dataframe column names to if they're not in the schema.
        Useful for ignoring columns that haven't been created yet.

        Examples
        --------
        # Matching dataframe-schema
        >>> df = pl.DataFrame({"a": [1,2,3], "b": ["one", "two", "three"]})
        >>> schema = (("a", pl.Int8), ("b", pl.String))
        >>> check_names(df, schema)
        None

        # Schema expects 'c' column
        >>> df = pl.DataFrame({"a": [1,2,3], "b": ["one", "two", "three"]})
        >>> schema = (("a", pl.Int8), ("b", pl.String), ("c", pl.String))
        >>> check_names(df, schema)
        ColumnNotFoundError: `check_names` found columns missing from dataframe: {'c'}

        # Schema does not expect 'z' column
        >>> df = pl.DataFrame({"a": [1,2,3], "b": ["one", "two", "three"], "z": [4,5,6]})
        >>> schema = (("a", pl.Int8), ("b", pl.String))
        >>> check_names(df, schema)
        SchemaError: `check_names` found columns not expected by schema: {'z'}
    """
    actual: set[str] = set(df.columns)
    expected: set[str] = {i[0] for i in schema}
    # Handle optional ignored schema values
    if ignore_missing:
        expected = {i[0] for i in schema if i[0] not in ignore_missing}
    # Columns names in actual not in expected
    not_expected: set[str] = actual.difference(expected)
    # Handle columns missing in actual columns
    missing: set[str] = expected.difference(actual)

    # Raise errors
    if missing:
        raise pl.exceptions.ColumnNotFoundError(f"`check_names` found columns missing from dataframe: {missing}")
    if not_expected:
        raise pl.exceptions.SchemaError(f"`check_names` found columns not expected by schema: {not_expected}")
    return
