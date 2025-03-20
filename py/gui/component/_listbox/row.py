"""
Module: listbox_entry.py
========================
This module implements the ListboxRow class, which represents a single row within a Listbox.
Each row aggregates formatted values from different columns and supports filtering and
iterable access.

Classes
-------
ListboxRow
    Immutable representation of a single row, with methods to format values,
    apply filters, and access individual cell values.
"""

from dataclasses import dataclass
from typing import Dict, Iterable, Tuple, Any, Union, Optional
from gui.component._listbox.column import ListboxColumn
from gui.component.interface.row import IRow
from gui.component._listbox.filter import Filter


@dataclass(slots=True, order=True, init=False, repr=False)
class ListboxRow(IRow):
    """
    Represents a single row in a Listbox.

    The row stores its values in two forms:
      - `_values`: A dictionary mapping the ListboxColumn's `column` (str) to the raw value.
      - `_strings`: A dictionary mapping a column ID (int) to the formatted string value.
    The concatenated string representation (_string) is built from the formatted values,
    preserving the order of keys in the row.

    Attributes
    ----------
    _string : str
        The concatenated, formatted string for the row.
    columns : Tuple[int, ...]
        A tuple of column IDs present in the row.
    _values : Dict[str, Any]
        A mapping from ListboxColumn.column (str) to the corresponding cell value.
    _strings : Dict[int, str]
        A mapping from column ID (int) to the formatted string value for that column.
    filters : Optional[int]
        A hash representing the set of filters last applied.
    is_filtered : bool
        Indicates whether the row is filtered (and therefore hidden).

    Examples
    --------
    >>> values = {0: 42, 1: "example"}
    >>> row = ListboxRow(values)
    >>> print(row)
    " 42    example "
    """
    _string: str
    columns: Tuple[int, ...]
    _values: Dict[str, Any]
    _strings: Dict[int, str]
    filters: Optional[int]
    is_filtered: bool

    def __init__(self, values: Dict[int, Any]) -> None:
        """
        Construct a ListboxRow from a dictionary of values.

        Parameters
        ----------
        values : dict
            A dictionary where keys are column IDs (int) and values are the raw cell values.

        Notes
        -----
        For each key in `values`, the corresponding ListboxColumn is retrieved (via `ListboxColumn.get_by_id`),
        and the raw value is stored under the column's identifier in `_values`. The value is also formatted
        using the column's `get_value` method and stored in `_strings`. Finally, all formatted values are concatenated
        into `_string`.
        """
        self.columns = tuple(values.keys())
        self._values, self._strings, string_parts = {}, {}, []
        for col_id, value in values.items():
            lbc = ListboxColumn.get_by_id(col_id)
            # Use the column's identifier (a string) as the key.
            self._values[lbc.column] = value
            formatted = lbc.get_value(value)
            self._strings[col_id] = formatted
            string_parts.append(formatted)
        self._string = " " + " ".join(string_parts)
        self.filters = None
        self.is_filtered = False

    def strf(self, column_order: Optional[Iterable[int]] = None) -> str:
        """
        Format the row as a string.

        Parameters
        ----------
        column_order : iterable of int, optional
            An optional sequence of column IDs specifying the order in which to join formatted values.
            If None, the default order (based on self.columns) is used.

        Returns
        -------
        str
            The formatted row string.
        """
        if column_order is None:
            return self._string
        return " ".join([self._strings[i] for i in column_order])

    def apply_filters(self, filters: Union[Filter, Tuple[Filter, ...]] = None, hashed_filters: int = None) -> bool:
        """
        Apply a set of filters to the row.

        The method invokes each filter on the row. If any filter returns False,
        the row is marked as filtered.

        Parameters
        ----------
        filters : Filter or tuple of Filter, optional
            One or more filters to apply.
        hashed_filters : int, optional
            A hash value representing the filters. If this matches the previously applied filters,
            the filtering is skipped.

        Returns
        -------
        bool
            True if the row is filtered (i.e., at least one filter failed), False otherwise.

        Notes
        -----
        - If `filters` is None, the row is not filtered.
        - If the current filters match the provided `hashed_filters`, the existing `is_filtered`
          value is returned.
        """
        if filters is None:
            self.filters = None
            self.is_filtered = False
            return False

        if self.filters is not None and self.filters == hashed_filters:
            return self.is_filtered

        self.is_filtered = False
        # If filters is a single Filter, convert it to a tuple.
        filter_tuple = filters if isinstance(filters, tuple) else (filters,)
        for f in filter_tuple:
            if not f(self):
                self.is_filtered = True
        self.filters = hashed_filters
        return self.is_filtered

    @staticmethod
    def generate(entry_values: Dict[int, Any]) -> 'ListboxRow':
        """
        Generate a ListboxRow from a dictionary of entry values.

        Parameters
        ----------
        entry_values : dict
            A dictionary with column IDs as keys and cell values as values.

        Returns
        -------
        ListboxRow
            The generated ListboxRow instance.
        """
        return ListboxRow(entry_values)

    def __getitem__(self, item: Union[str, int]) -> Any:
        """
        Retrieve a cell value from the row.

        Parameters
        ----------
        item : str or int
            If a string, it should match a ListboxColumn.column attribute.
            If an integer, it represents the ListboxColumn.id.

        Returns
        -------
        any
            The corresponding cell value if found.

        Raises
        ------
        KeyError
            If the key is not found in the row.
        """
        try:
            return self._values.get(item)
        except KeyError:
            valid_keys = ", ".join([str(k) for k in self.columns] + list(self._values.keys()))
            raise KeyError(f"Key '{item}' not found in ListboxRow. Valid keys: {valid_keys}")

    def __iter__(self):
        """
        Iterate over the cell values in the row.

        Yields
        ------
        any
            Each cell value stored in the row.
        """
        return iter(self._values.values())

    def __len__(self) -> int:
        """
        Return the number of cells in the row.

        Returns
        -------
        int
            The number of key-value pairs in the row.
        """
        return len(self._values)

    def __repr__(self) -> str:
        """
        Return a string representation of the row.

        Returns
        -------
        str
            The formatted string for the row.
        """
        return self._string

    def __str__(self) -> str:
        """
        Return the formatted row as a string.

        Returns
        -------
        str
            The formatted string for the row.
        """
        return self._string
