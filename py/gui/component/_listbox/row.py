"""
Module with ListboxEntry class, which is a representation of a single row within a Listbox.
"""
from dataclasses import dataclass
from typing import Dict, Iterable, Tuple

import gui.component._listbox.column as listbox_column
from gui.component.interface.row import IRow
from gui.component.filter.filter import Filter


@dataclass(slots=True, order=True, init=False, repr=False)
class ListboxRow(IRow):
    """
    A single ListboxEntry. Its individual values can be accessed via ListboxEntry[ListboxColumn.column].
    
    
    """
    _string: str
    """Default row format, equal to the key order of dict, while including all values"""
    
    columns: Tuple[int, ...]
    """Tuple with the listbox column ids"""
    
    _values: Dict[str, any]
    """A dict with a column_id as key and the associated value of that column as entry"""
    
    _strings: Dict[int, str]
    """A dict with column_id as key and the associated value of that column, formatted as a string as entry"""
    
    filters: int or None
    """A set of Filter instances applied to this entry"""
    
    is_filtered: bool
    """If True, one or more filters are applied, causing this entry to be hidden"""
    
    def __init__(self, values: Dict[int, any]):
        self.columns = tuple(values.keys())
        self._values, self._strings, string = {}, {}, []
        for i, value in values.items():
            lbc = listbox_column.get(i, "id")
            self._values[lbc.column] = value
            s = lbc.get_value(value)
            self._strings[i] = s
            string.append(s)
        self._string = " " + " ".join(string)
        self.filters = None
        self.is_filtered = False
    
    def strf(self, column_order: Iterable[int] = None) -> str:
        if column_order is None:
            return self._string
        return " ".join([self._strings[i] for i in column_order])
    
    def apply_filters(self, filters: Filter or Tuple[Filter] = None, hashed_filters: int = None) -> bool:
        """
        Applies a set of filters to determine if the current object is filtered, and therefore be present in the Listbox

        Parameters
        ----------
        filters : Filter or Iterable[Filter], optional
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
        
        # Filter was previously applied; return that outcome rather than re-applying filters.
        if self.filters is not None and self.filters == hashed_filters:
            return self.is_filtered
        
        self.is_filtered = False
        for f in filters:
            if not f(self):
                self.is_filtered = True
        self.filters = hashed_filters
        return self.is_filtered
    
    @staticmethod
    def generate(entry_values: dict):
        """ Returns a ListboxEntry, generated from `entry_values` """
        return ListboxRow(entry_values)
    
    def __getitem__(self, item: str | int) -> any:
        """
        Get a specific value from the values this ListboxEntry holds. `item` can be a column attribute from the
        ListboxColumn, or the `id` attribute. Recommended usage is via Listbox.column, however.
        
        Parameters
        ----------
        item : str | int
            ListboxColumn.column attribute (str) or ListboxColumn.id (int)

        Returns
        -------
        any
            Will return the corresponding value, if it exists.
        """
        try:
            # print(listbox_column.get("id" if isinstance(item, int) else item).column)
            return self._values.get(item)#, self._values[listbox_column.get("id", item).column])
        except KeyError:
            msg = (f"KeyError in ListboxEntry.__getitem__ with key={item} of type={type(item)} "
                   f"The following keys are valid; "
                   f"{', '.join([str(c) for c in self.columns] + list(self._values.keys()))}")
            raise KeyError(msg)
    
    def __iter__(self):
        """
        Iterate over the values of the _values dictionary.

        Yields
        ------
        Any
            The values from the _values dictionary.
        """
        return iter(self._values.values())
    
    def __len__(self) -> int:
        return len(self._values)
    
    def __repr__(self):
        return self._string
    
    def __str__(self):
        return self._string
