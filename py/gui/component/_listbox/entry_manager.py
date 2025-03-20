"""
Module: listbox_entries.py
==========================
This module implements the ListboxEntries class, which manages a collection of listbox entries,
providing sorting, filtering, and easy access to a subset of formatted entries. The entries
can be accessed as if ListboxEntries were a list.

Classes
-------
ListboxEntries
    Manages sorting, filtering, and accessing a set of entries.
"""

from collections.abc import Callable
from typing import List, Tuple, Dict, Optional, Iterable, Any, Union

from gui.component._listbox.column import ListboxColumn
from gui.component._listbox.interfaces import SortLike
from gui.component._listbox.sort import Sort, Sorts
from gui.component.interface.filter import IFilterable, IFilter
from gui.component.interface.sort import ISortSequence, ISortable, ISortBy
from gui.util.font import Font, FontFamily
from gui.util.generic import IRow


class ListboxEntries(ISortable, IFilterable):
    """
    Manages a collection of listbox entries, supporting sorting, filtering, and formatting.

    This class maintains a full list of entries (self.all) and a filtered/sorted subset (self.subset)
    that can be accessed as if it were a list. It also maintains a collection of ListboxColumn instances
    and a mapping from column identifiers (both numeric and string) to column indices.

    Parameters
    ----------
    entries : List[IRow]
        The initial list of entries.
    listbox_columns : Iterable[ListboxColumn]
        The collection of ListboxColumn instances representing the columns.
    insert_subset : Callable
        A callback function responsible for filling the listbox with entries.
    initial_sort : Optional[ISortSequence], optional
        The default sort sequence to be applied, by default None.
    **kwargs : dict
        Additional keyword arguments.

    Attributes
    ----------
    all : List[IRow]
        All entries present in the underlying data structure.
    subset : Tuple[IRow, ...]
        The filtered and sorted subset of entries.
    filters : Optional[IFilter]
        The filter(s) currently applied.
    applied_filters : Optional[int]
        A hash value representing the set of filters currently applied.
    default_sort_sequence : Optional[Sorts]
        The sort sequence that is applied by default.
    sort_sequence : Optional[Sorts]
        The current sorting sequence.
    last_sorted : Optional[ISortBy]
        The header button sort that was most recently applied.
    fill_listbox : Callable
        Callback to populate the listbox with entries.
    columns : Tuple[ListboxColumn, ...]
        A tuple of ListboxColumn instances.
    column_mapping : Dict[Union[int, str], int]
        Mapping from column ID or DataFrame header (str) to column index.
    font : Font
        The font used for the entries.
    """

    __slots__ = ("all", "subset", "filters", "applied_filters", "default_sort_sequence",
                 "sort_sequence", "last_sorted", "fill_listbox", "columns",
                 "column_mapping", "font")

    all: List[IRow]
    subset: Tuple[IRow, ...]
    filters: Optional[IFilter]
    applied_filters: Optional[int]
    default_sort_sequence: Optional[Sorts]
    sort_sequence: Optional[Sorts]
    last_sorted: Optional[ISortBy]
    fill_listbox: Callable
    columns: Tuple[ListboxColumn, ...]
    column_mapping: Dict[Union[int, str], int]
    font: Font

    def __init__(self,
                 entries: List[IRow],
                 listbox_columns: Iterable[ListboxColumn],
                 insert_subset: Callable,
                 initial_sort: Optional[ISortSequence] = None,
                 **kwargs):
        """
        Initialize the ListboxEntries instance.

        Parameters
        ----------
        entries : List[IRow]
            The initial list of entries.
        listbox_columns : Iterable[ListboxColumn]
            The columns to be displayed.
        insert_subset : Callable
            A callback to insert the subset of entries into the listbox.
        initial_sort : Optional[ISortSequence], optional
            The initial sort sequence to apply, by default None.
        **kwargs : dict
            Additional keyword arguments.
        """
        self.all = list(entries)
        self.default_sort_sequence = initial_sort
        self.sort_sequence = None
        self.last_sorted = None
        self.filters = None
        self.applied_filters = None
        self.fill_listbox = insert_subset
        if initial_sort is not None:
            self.initial_sort(initial_sort)

        self.columns = tuple(listbox_columns)
        self.column_mapping = {}
        for idx, c in enumerate(self.columns):
            self.column_mapping[c.id] = idx
            self.column_mapping[c.column] = idx
        self.font = kwargs.get('font', Font(9, FontFamily.CONSOLAS))

    def get_column(self, column: Union[int, str]) -> ListboxColumn:
        """
        Fetch a ListboxColumn by its DataFrame header (str) or column ID (int).

        Parameters
        ----------
        column : int or str
            The identifier for the column.

        Returns
        -------
        ListboxColumn
            The corresponding ListboxColumn instance.
        """
        return self.columns[self.column_mapping[column]]

    def header_button_sort(self, sort: ISortBy, **kwargs) -> Tuple[IRow, ...]:
        """
        Callback for header button clicks to sort the entries.

        If the same sort is applied consecutively, its direction is flipped.

        Parameters
        ----------
        sort : ISortBy
            The sort configuration.
        **kwargs : dict
            Additional keyword arguments to pass to apply_configurations.

        Returns
        -------
        tuple of IRow
            The sorted (and filtered) subset of entries.
        """
        if self.last_sorted is not None and self.last_sorted == sort:
            sort.flip()
        self.last_sorted = sort
        return self.apply_configurations(sort, header_callback=True, **kwargs)

    def initial_sort(self, initial_sort: Optional[SortLike] = None, **kwargs) -> None:
        """
        Perform an initial sort on the full list of entries.

        Parameters
        ----------
        initial_sort : Optional[SortLike], optional
            The sort sequence to apply initially.
        **kwargs : dict
            Additional keyword arguments.
        """
        self.default_sort_sequence = initial_sort
        if initial_sort is not None:
            entries = self.all
            entries = self.sort(initial_sort, entries)
            self.all = list(entries)

    def apply_configurations(self,
                             sort_by: Optional[Sorts] = None,
                             filters: Optional[IFilter] = None,
                             column_order: Any = None,
                             header_callback: bool = False,
                             **kwargs) -> Tuple[IRow, ...]:
        """
        Sequentially apply filters and sorting to the entries, then format each row.

        Parameters
        ----------
        sort_by : Optional[Sorts], optional
            The sort sequence to apply, by default None.
        filters : Optional[IFilter], optional
            The filter(s) to apply, by default None.
        column_order : any, optional
            An optional parameter for reordering columns.
        header_callback : bool, optional
            Whether the sort is triggered by a header button callback. Default is False.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        tuple of IRow
            The resulting subset of entries after filtering and sorting.
        """
        if filters is None:
            subset = self.all
        else:
            self.filters, filters_hashed = filters, hash(filters)
            subset = self.filter(filters, filters_hashed)

        if sort_by is not None:
            self.sort_sequence = sort_by

        if header_callback:
            if self.last_sorted is not None and self.last_sorted == sort_by:
                self.last_sorted.flip()
                self.sort_sequence = self.last_sorted
            else:
                self.last_sorted = sort_by
            self.subset = self.sort_sequence(subset)
        else:
            if self.sort_sequence is None:
                self.subset = tuple(subset)
            else:
                # Create a new Sort instance from the current sort sequence.
                self.subset = tuple(self.sort(Sort(self.sort_sequence.column,
                                                   self.sort_sequence.reverse), subset))
        return self.subset

    @property
    def fetch(self) -> Tuple[IRow, ...]:
        """
        Retrieve the filtered, sorted subset of formatted entries.

        Returns
        -------
        tuple of IRow
            The subset of entries.
        """
        return tuple(self.subset)

    def filter(self, filters: Union[IFilter, Iterable[IFilter]], filters_hashed: int = None) -> Tuple[IRow, ...]:
        """
        Apply filtering to the full list of entries.

        Parameters
        ----------
        filters : IFilter or Iterable[IFilter]
            The filter(s) to apply.
        filters_hashed : int, optional
            A hash representing the applied filters, by default None.

        Returns
        -------
        tuple of IRow
            The filtered subset of entries.

        Raises
        ------
        Exception
            If filtering fails.
        """
        subset = []
        if filters_hashed is not None and self.applied_filters == filters_hashed:
            return self.subset

        for row in self.all:
            if not filters.apply_filters(row):
                subset.append(row)
        self.applied_filters = filters_hashed
        self.subset = tuple(subset)
        return self.subset

    def sort(self, sort_by: Sorts, rows: Optional[Iterable[IRow]] = None) -> Tuple[IRow, ...]:
        """
        Sort the given entries using the specified sort sequence.

        Parameters
        ----------
        sort_by : Sorts
            The sort sequence to apply.
        rows : Optional[Iterable[IRow]], optional
            The entries to sort. If None, sorts self.all.

        Returns
        -------
        tuple of IRow
            The sorted entries.
        """
        return sort_by.apply_sorts(self.all if rows is None else rows)

    def __len__(self) -> int:
        """
        Return the number of entries in the current subset.

        Returns
        -------
        int
            The length of the subset.
        """
        return len(self.subset)

    def __getitem__(self, index: int) -> IRow:
        """
        Retrieve the entry at the specified index from the subset.

        Parameters
        ----------
        index : int
            The index of the entry.

        Returns
        -------
        IRow
            The entry at the given index.
        """
        return self.subset[index]
