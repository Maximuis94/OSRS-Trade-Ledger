"""
Module with various pre-defined SQL statements for the timeseries database


"""
from typing import Literal, Optional

from timeseries.constants import empty_tuple

#region SELECT
select = {
    "000": """SELECT * FROM __TABLE__""",
    "010": """SELECT * FROM __TABLE__ WHERE timestamp >= ?""",
    "001": """SELECT * FROM __TABLE__ WHERE timestamp <= ?""",
    "011": """SELECT * FROM __TABLE__ WHERE timestamp BETWEEN ? AND ?""",
    "100": """SELECT * FROM __TABLE__ WHERE src=?""",
    "110": """SELECT * FROM __TABLE__ WHERE src=? AND timestamp >= ?""",
    "101": """SELECT * FROM __TABLE__ WHERE src=? AND timestamp <= ?""",
    "111": """SELECT * FROM __TABLE__ WHERE src=? AND timestamp BETWEEN ? AND ?""",
    "200": """SELECT * FROM __TABLE__ WHERE src BETWEEN ? AND ?""",
    "210": """SELECT * FROM __TABLE__ WHERE src BETWEEN ? AND ? AND timestamp >= ?""",
    "201": """SELECT * FROM __TABLE__ WHERE src BETWEEN ? AND ? AND timestamp <= ?""",
    "211": """SELECT * FROM __TABLE__ WHERE src BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?""",
    
}

def where(src: Optional[Literal[1, 2]] = None, t0: Optional[bool] = None, t1: Optional[bool] = None) -> str:
    """Generate a WHERE clause based on the parameters passed
    
    
    Parameters
    ----------
    src : Optional[Literal[1, 2]]
        If passed as 1, allows for specifying one src value, if passed as 2, include an src value range,
        i.e. src BETWEEN ? AND ?
    t0 : Optional[bool]
        If True, include a lower bound timestamp
    t1 : Optional[bool]
        If True, include an upper bound timestamp

    Returns
    -------
    str
        WHERE clause that meets all specifications as defined by the parameters that were passed.

    """
    clauses = []
    
    if src == 1:
        clauses.append("src = ?")
    elif src == 2:
        clauses.append("src BETWEEN ? AND ?")

    if t0:
        clauses.append("timestamp >= ?")
    if t1:
        clauses.append("timestamp <= ?")

    if not clauses:
        return ""
    return "WHERE " + " AND ".join(clauses)
    


#endregion