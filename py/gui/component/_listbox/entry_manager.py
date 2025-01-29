from collections.abc import Callable, Iterable
from typing import List, Tuple, Dict, Optional

from gui.component._listbox._column import ListboxColumn
from gui.component._listbox.interfaces import SortLike
from gui.component.interface.column import IListboxColumn
from gui.component.interface.filter import IFilterable, IFilter
from gui.component.interface.sort import ISortSequence, ISortable, ISortBy
from gui.component.sort.sort import Sort, Sorts
from gui.util.font import Font, FontFamily
from gui.util.generic import IRow


class ListboxEntries(ISortable, IFilterable):
    """
    Class for sorting, filtering and accessing a set of entries. Entries of the subset can be accessed as if it
    were a list of IListboxEntries
    """
    __slots__ = ("all", "sort_sequence", "last_sorted", "filters", "fill_listbox", "columns", "column_mapping",
                 "subset", "default_sort_sequence")
    
    all: List[IRow]
    """All ListboxEntries present in the underlying data structure"""
    
    subset: Tuple[IRow, ...]
    """A filtered, sorted subset of ListboxEntries"""
    
    filters: Optional[IFilter]
    """List of Filter instances that are to be applied"""
    
    applied_filters: Optional[int]
    """Hashed set of Filters that is currently applied to the subset, if any"""
    
    default_sort_sequence: Optional[SortLike]
    """The sort sequence that is applied by default"""
    
    sort_sequence: Optional[SortLike]
    """The configured sorting sequence"""
    
    last_sorted: Optional[ISortBy]
    """The header button sort that was most recently applied"""
    
    fill_listbox: Callable
    """Callback that fills the listbox with entries"""
    
    columns: tuple[ListboxColumn, ...]
    """List of ListboxColumns"""
    
    column_mapping: Dict[int | str, int]
    """Mapping of column id and df header to each index in the columns tuple"""
    
    font: Font = Font(9, FontFamily.CONSOLAS)
    """Font of the entries"""
    
    def __init__(self, entries: List[IRow], listbox_columns: Iterable[IListboxColumn], insert_subset: Callable,
                 initial_sort: ISortSequence = None, **kwargs):
        
        self.all = list(entries)
        
        self.default_sort_sequence = initial_sort
        self.sort_sequence = None
        self.last_sorted: Sort or None = None
        
        self.filters = None
        self.applied_filters = None
        self.fill_listbox = insert_subset
        
        if initial_sort is not None:
            self.initial_sort(initial_sort)
        
        self.columns = tuple(listbox_columns)
        self.column_mapping = {}
        for a, b in [({c.id: idx}, {c.column: idx}) for idx, c in enumerate(listbox_columns)]:
            self.column_mapping.update(a)
            self.column_mapping.update(b)
    
    def get_column(self, column: int or str):
        """Fetch a ListboxColumn by its df header (str) or column id (int)"""
        return self.columns[self.column_mapping[column]]
    
    def header_button_sort(self, sort: ISortBy, **kwargs):
        """Callback for Listbox header buttons"""
        if self.last_sorted is not None and self.last_sorted == sort:
            sort.flip()
        self.last_sorted = sort
        return self.apply_configurations(sort, header_callback=True, **kwargs)
    
    def initial_sort(self, initial_sort: Optional[SortLike] = None, **kwargs):
        """Sort the full list of entries. This implies the entry list is always pre-sorted like this."""
        self.default_sort_sequence = initial_sort
        
        if initial_sort is not None:
            entries = self.all
            self.default_sort_sequence = initial_sort
            entries = self.sort(initial_sort, entries)
            self.all = list(entries)
    
    def apply_configurations(self, sort_by: Optional[Sorts] = None, filters: Optional[IFilter] = None,
                             column_order=None, header_callback: bool = False) -> Tuple[IRow, ...]:
        """Sequentially apply filters+sorting, then format individual rows"""
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
            self.subset = tuple(subset if self.sort_sequence is None else self.sort(Sort(self.sort_sequence.column, self.sort_sequence.reverse), subset))
        return self.subset
    
    @property
    def fetch(self) -> Tuple[IRow, ...]:
        """fetch the filtered, sorted subset of formatted entries."""
        return tuple(self.subset)
    
    def filter(self, filters: IFilter | Iterable[IFilter], filters_hashed: int = None) -> Tuple[IRow, ...]:
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
        return sort_by.apply_sorts(self.all if rows is None else rows)
    
    def __len__(self) -> int:
        return len(self.subset)
    
    def __getitem__(self, index: int) -> IRow:
        return self.subset[index]
