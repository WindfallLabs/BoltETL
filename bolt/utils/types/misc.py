import polars as pl


def schematize(df: pl.DataFrame, schema: list[tuple[str, pl.DataType]]):
    """Casts columns of a DataFrame to those specified in the given schema."""
    # Expressions to execute in `df.with_columns`
    expressions = []
    # Final cols to keep
    select_columns = []
    for col, dtype in schema:
        # Drop columns if dtype is None
        if dtype is None:
            continue
        # Add column to final dataframe selection
        select_columns.append(col)
        # If the column doesn't exist, make it
        if col not in df.columns:
            expressions.append(pl.Series([None] * len(df), dtype=dtype).alias(col))
            continue
        # Get existing column dtype
        orig_dtype: pl.DataType = df.dtypes[df.columns.index(col)]
        if dtype == orig_dtype:
            continue
        # Cast types
        ## Remove comma from string column values if cast to float|int
        if ((dtype.__name__.startswith("Float")
             or dtype.__name__.startswith("Int"))
            and orig_dtype == pl.String):
            expressions.append(pl.col(col).str.replace(",", "").cast(dtype))
        # TODO: elifs for other casting corrections here??
        else:
            expressions.append(pl.col(col).cast(dtype))
    # Create returned dataframe
    return (
        df.with_columns(
            *expressions
        ).select(select_columns)
    )
