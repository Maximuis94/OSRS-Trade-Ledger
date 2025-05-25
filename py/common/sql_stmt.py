"""
SQL repository. Contains various template SQL statements for computing certain values.
Can be imported or copied and modified elsewhere, or the associated functions can be imported to modify the SQL into
something more specific.

SQL statements and their associated functions within this module are designed to be modified towards a specific use case
rather than repetitively calling the function from this method.
An alternative approach is to manually copy it and modify it elsewhere.

"""
from typing import Optional

_percentile_score = """
    WITH ranked AS (
      SELECT
        __COLUMN__,
        ROW_NUMBER()   OVER (ORDER BY __COLUMN__) AS rn,
        COUNT(*)       OVER ()             AS total
      FROM (SELECT __COLUMN__ FROM "__TABLE__" __WHERE__ ORDER BY __COLUMN__)
    )
    SELECT __COLUMN__ AS percentile
    FROM ranked
    WHERE rn >= __PERCENTILE_VALUE__ * total
    ORDER BY rn
    LIMIT 1;
"""
"""SQL for computing the percentile score"""


def percentile_score(table: str, column: str, percentile_value: float, where: Optional[str] = None) -> str:
    """Executable SQL for fetching the `percentile_value` percentile score from table `table` with values from `column`
    
    Parameters
    ----------
    table : str
        Name of the table to draw values from
    column : str
        Name of the column to draw values from
    percentile_value : float
        The percentile score to fetch
    where : str
        (optional) WHERE clause to insert into the SQL statement

    Returns
    -------
    str
        Executable SQL statement with the parameters injected into it

    """
    where = "WHERE " + where.lstrip("WHERE ") if where is not None else ""
    
    return (_percentile_score.replace("__COLUMN__", column)
            .replace("__PERCENTILE_VALUE__", str(percentile_value))
            .replace("AS percentile", f"AS percentile{percentile_value*100:.0f}")
            .replace("__TABLE__", table)
            .replace("__WHERE__", where if where else ""))

# print(percentile_score("item00002", "price", .9, """timestamp > (SELECT MAX(timestamp) FROM "item00002" WHERE src < 3)-21600"""))


_closest_n_values = """
    SELECT __COLUMN__ __ALIAS__
    FROM (
      SELECT __COLUMN__
      FROM __TABLE__
      WHERE timestamp <= ? __WHERE__
      ORDER BY timestamp DESC
      LIMIT __N_ROWS__
    )
"""


def avg_closest_n_values(table: str, column: str, n_rows: Optional[int] = None, alias: Optional[str] = None,
                         where_clause: Optional[str] = None) -> str:
    """ SQL statement for fetching `n_rows` values from `column` that are closest to the given timestamp that are as old or older
    than timestamp
    
    Parameters
    ----------
    table : str
        Name of the table to draw values from
    column : str
        Name of the column to draw values from
    n_rows : int, optional, 7 by default
        Amount of rows to include in the statement. By default, insert a ? instead,
    alias : Optional[str], optional, None by default
        If given, select the requested value and assign a particular alias.
    where_clause : Optional[str], optional, None by default
        If given, extend the WHERE clause with this string

    Returns
    -------
    str
        The template SQL statement with the parameters injected into it
    
    Notes
    -----
    The original use case for this SQL was to get the most recent wiki data, given a particular timestamp, as data with
    src=0 was not updated as frequent as the active trade data, for instance. As such, there was need for some statement
    to get the updated data for a particular timestamp, i.e. the most recent data available at that time.
    """
    return (_closest_n_values.replace("__COLUMN__", column)
            .replace("__ALIAS__", f"AS {alias}" if alias is not None else "")
            .replace("__TABLE__", table)
            .replace("__N_ROWS__", str(n_rows) if n_rows is not None else "?")
            .replace("__WHERE__", "AND "+where_clause if where_clause else ""))
    
