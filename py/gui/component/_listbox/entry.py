"""
Module with ListboxEntry class, which is a representation of a single row within a Listbox.

"""
from collections.abc import Callable
from typing import Dict, Iterable, Tuple

from dataclasses import dataclass

import gui.component._listbox.column as listbox_column
from gui.util.filter import Filter


@dataclass(slots=True, order=True)
class ListboxEntry:
    """ Class for a single Listbox Entry """
    
    string: str
    """Default row format, equal to the key order of dict, while including all values"""
    
    ids: Tuple[int, ...]
    """Tuple with the listbox column ids"""
    
    values: Dict[str, any]
    """A dict with a column_id as key and the associated value of that column as entry"""
    
    strings: Dict[int, str]
    """A dict with column_id as key and the associated value of that column, formatted as a string as entry"""
    
    filters: int or None
    """A set of Filter instances applied to this entry"""
    
    is_filtered: bool = False
    """If True, one or more filters are applied, causing this entry to be hidden"""
    
    def __init__(self, values: Dict[int, any]):
        self.ids = tuple(values.keys())
        self.values, self.strings, string = {}, {}, []
        for i, value in values.items():
            lbc = listbox_column.get(i, "id")
            self.values[lbc.column] = value
            s = lbc.get_value(value)
            # print(s)
            self.strings[i] = s
            string.append(s)
        self.string = " " + " ".join(string)
        self.filters = None
    
    def fmt(self, column_order: Iterable[int] = None) -> str:
        """ Return the formatted row, ordered using `column_order` """
        if column_order is None:
            return self.string
        return " ".join([self.strings[i] for i in column_order])
    
    def apply_filters(self, filters: Filter or Tuple[Filter] = None, hashed_filters: int = None) -> bool:
        """
        Applies a set of filters to determine if the current object is filtered, and therefore be present in the Listbox

        Parameters
        ----------
        filters : Filter or Tuple[Filter], optional
            0-N Filters to apply.
        hashed_filters : int, optional
            A hash value derived from `filters`. Used to prevent repetitive calls.
    
        Returns
        -------
        bool
            True if any of the filters fail (-> object is filtered), False otherwise.
    
        Notes
        -----
        - If `filters` is `None`, result is False
        - If `self.filters` matches the `hashed_filters`, the method avoids reapplying
          the filters and directly returns the existing filtering state (`self.is_filtered`).
        - The `filters` parameter can either be a single `Filter` or a tuple
          of such Filters. Each Filter is invoked with this ListboxEntry, and if any of them return `False`, the Entry
          is considered filtered.
        """
        if filters is None:
            self.filters = None
            self.is_filtered = False
            return False
        
        if self.filters is not None and self.filters == hashed_filters:
            return self.is_filtered
        
        self.is_filtered = False
        for f in ([filters] if isinstance(filters, Filter) else filters):
            if not f(self):
                self.is_filtered = True
        self.filters = hashed_filters
        return self.is_filtered
    
    @staticmethod
    def generate(entry_values: dict):
        """ Returns a ListboxEntry, generated from `entry_values` """
        return ListboxEntry(entry_values)
    
    def __getitem__(self, item):
        try:
            return self.values[item]
        except KeyError as e:
            print(f"KeyError in ListboxEntry.__getitem__ with key={item}")
            raise e
    