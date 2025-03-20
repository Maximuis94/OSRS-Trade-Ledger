"""
Module with various SQL statements to delete rows
"""


def all_rows(table: str) -> str:
    """Returns an SQL statement for `table` that will have all rows of that table deleted"""
    return f"""DELETE FROM "{table}";"""

