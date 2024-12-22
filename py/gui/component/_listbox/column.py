"""
Module for Listbox Column creation or selection.
Import via "import .component."

This module should be used to acquire ListboxColumn instances
"""
from collections.abc import Callable
from typing import Dict

from gui.util.str_formats import shorten_string
from gui.component._listbox._column import ListboxColumn
from gui.util.str_formats import strf_number, strf_unix


_lbc_make_args = ("column", "width", "format", "header", "button_click", "is_visible", "is_number", "push_left")


# Below are pre-defined ListboxColumns. The get() function should be used if ListboxColumns are needed with custom
# attributes.

ITEM_ID = ListboxColumn.make("item_id", 6)
"""item_id ListboxColumn. Max width is 6, based on the highest item_id being ~30000"""

ITEM_NAME = ListboxColumn.make("item_name", 30, lambda s: shorten_string(s, 30), "item_name", None, True, False, False)
"""item_name ListboxColumn. Max width is 30, longer strings are shortened, by substituting middle chars with '.'"""

PRICE = ListboxColumn.make("price", 9, lambda n: strf_number(n, 2, 9))
"""price ListboxColumn. Max width is 9. Formatting is integer formatting with mkb substitution"""

QUANTITY = ListboxColumn.make("quantity", 9, lambda n: strf_number(n, 2, 9))
"""quantity ListboxColumn. Max width is 9. Formatting is integer formatting with mkb substitution"""

VALUE = ListboxColumn.make("value", 9, lambda n: strf_number(n, 2, 9))
"""value ListboxColumn. Max width is 10. Formatting is integer formatting with mkb substitution"""

TIMESTAMP = ListboxColumn.make("timestamp", 15, lambda t: strf_unix(t, "%d-%m-%y %H:%M", False), "Timestamp")
"""DMY HMS timestamp ListboxColumn. Width is always 15, based on string formatting %d-%m-%Y %H:%M"""

TIMESTAMP_DMY = ListboxColumn.make("timestamp", 9, lambda t: strf_unix(t, "%d-%m-%Y", False), "Date (dmy)")
"""DMY timestamp ListboxColumn. Width is always 9, based on string formatting %d-%m-%Y"""

TIMESTAMP_HMS = ListboxColumn.make("timestamp", 9, lambda t: strf_unix(t, "%H:%M:%S", False), "Time (hms)")
"""HMS timestamp ListboxColumn. Width is always 9, based on string formatting %H:%M:%S"""

TIMESTAMP_DOW = ListboxColumn.make("timestamp", 4, lambda t: strf_unix(t, "%a", False), "DoW")
"""Day of week ListboxColumn. Width is always 4, based on abbreviated weekdays"""

TIMESTAMP_DOW_FULL = ListboxColumn.make("timestamp", 10, lambda t: strf_unix(t, "%A", False), "Weekday")
"""Day of week ListboxColumn. Width is 10, based on string length of "Wednesday", the longest day in char length"""


_hashed_columns: Dict[int, ListboxColumn] = {}


def _is_listbox_column(column: str) -> bool:
    """Return True if this variable is named as is expected of a pre-defined ListboxColumn"""
    return False not in [char.isupper() or char == '_' for char in column]


# listbox_columns = tuple([v for _, v in dict(locals()).items() if isinstance(v, ListboxColumn)])


def get(key, key_var: str, command: Callable = None, header: str = None, column: ListboxColumn = None, **kwargs) \
        -> ListboxColumn:
    """
    Get a ListboxColumn derived from one of the Columns listed above, with slight-massive alterations, depending on
    input. `command` and `header` are listed as separate (kw)args as they are expected to be modified more
    frequently.
 
    Parameters
    ----------
    key
        If passed as string, use this string to fetch an enumerated ListboxColumn. The ListboxColumn will be used as
        template and will have other variables altered
    key_var: str
        The name of the variable that is searched for
    command : Callable, optional, None by default
        If passed, this will be the button command. This method typically does not accept args; nor does it return
        anything.
    header : str, optional, None by default
        If passed, this will be the column header
    column : ListboxColumn, optional, None by default
        The ListboxColumn to use as template
 
    Returns
    -------
    ListboxColumn
        A custom ListboxColumn, based on the given (pre-defined) ListboxColumn, altered via other args.
    
    Raises
    ------
    KeyError
        If no ListboxColumn is found, given `key` and `key_var`, raise a KeyError.
    """
    if column is None:
        for c in ListboxColumn.get_all():
            if not isinstance(c, ListboxColumn):
                print("There is a non-ListboxColumn found within LISTBOX_COLUMNS;")
                for lbc in ListboxColumn.get_all():
                    print(lbc)
            if c.__getattribute__(key_var) == key:
                column = c
                break
    
    if column is None:
        e = f"No ListboxColumns were found for key={key} with value={key_var}"
        raise KeyError(e)
    
    elif len(kwargs) == 0 and command is None and header is None:
        return column
    
    
    # kw = {k: column.__getattribute__(k) for k in column.__dir__() if k[0] != '_'}
    
    
    # Merge the original column values with additionally passed kwargs; the latter will override
    kw = {k: column.__getattribute__(k) if kwargs.get(k) is None else kwargs[k] for k in column.__match_args__
          if k != 'id'}
    
    # If defined, set command and header as well
    if command is not None:
        kw['button_click'] = command
    if header is not None:
        kw['header'] = header
    
    kw_hash = tuple([(k, v) for k, v in kw.items()]).__hash__()
    if _hashed_columns.get(kw_hash) is None:
        lbc = ListboxColumn.make(**{k: kw[k] for k in _lbc_make_args if kw.get(k) is not None})
        _hashed_columns[kw_hash] = lbc
        print(f"Added column {len(_hashed_columns)}:", str(lbc))
        return lbc
    else:
        return _hashed_columns[kw_hash]


if __name__ == "__main__":
    # print(globals().keys())
    # print(locals().keys())
    ...
