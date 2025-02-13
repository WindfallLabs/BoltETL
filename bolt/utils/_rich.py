import pandas as pd
from rich.table import Table


def df_to_table(
    pandas_dataframe: pd.DataFrame,
    rich_table: Table | None = None,
    show_index: bool = True,
    index_name: str | None = None,
) -> Table:
    """Convert a pandas.DataFrame obj into a rich.Table obj.
    Args:
        pandas_dataframe (DataFrame): A Pandas DataFrame to be converted to a rich Table.
        rich_table (Table): A rich Table that should be populated by the DataFrame values.
        show_index (bool): Add a column with a row count to the table. Defaults to True.
        index_name (str, optional): The column name to give to the index column. Defaults to None, showing no value.
    Returns:
        Table: The rich Table instance passed, populated with the DataFrame values.

    [Source](https://gist.github.com/neelabalan/33ab34cf65b43e305c3f12ec6db05938)
    """

    if not rich_table:
        rich_table = Table(show_header=True, header_style="bold blue")

    if show_index:
        index_name = str(index_name) if index_name else ""
        rich_table.add_column(index_name)

    for column in pandas_dataframe.columns:
        rich_table.add_column(str(column))

    for index, value_list in enumerate(pandas_dataframe.values.tolist()):
        row = [str(index)] if show_index else []
        row += [str(x) for x in value_list]
        rich_table.add_row(*row)

    return rich_table
