"""
Module with Sort class

"""
from collections.abc import Iterable
from dataclasses import dataclass
from typing import List, Optional

from gui.component.interface.sort import ISortBy, ISortSequence
from gui.util.generic import SupportsGetItem


@dataclass(slots=True, order=True)
class Sort(ISortBy):
    """
    A sort element that ensures Sort sequences are provided in a streamlined fashion.
    Sort instances can be called, which will apply the sorting parameters to the given list of ListboxEntries
    
    
    """
    column: str
    """The ListboxColumn that is to be sorted"""

    reverse: bool = False
    """Whether or not to reverse the sort"""
    
    def __iter__(self):
        yield self
    
    def __call__(self, to_sort: Iterable[SupportsGetItem]) -> List[SupportsGetItem]:
        """Apply this Sort to the given ListboxEntries `to_sort` and return the sorted list of ListboxEntries"""
        return list(sorted(to_sort, key=lambda e: e._values[self.column], reverse=self.reverse))
    
    def __eq__(self, other):
        return self.column == other.column and self.reverse == other.reverse
    
    def flip(self):
        """Flip this Sort by setting its reverse flag to the opposite value"""
        self.reverse = not self.reverse
    
    def apply_sorts(self, entries: Iterable[SupportsGetItem]) -> List[SupportsGetItem]:
        return self(entries)


@dataclass(slots=True)
class SortSequence(List, ISortSequence):
    """Class with a sequence of Sorts. Can be called to apply all of its sorts sequentially to the given list."""
    def __init__(self, *sorts):
        super().__init__()
        for sort in sorts:
            self + sort
    
    def __add__(self, sort: ISortBy | Iterable[ISortBy]) -> List[ISortBy]:
        if isinstance(sort, Iterable):
            for s in sort:
                self + s
        else:
            if isinstance(sort, str):
                sort = Sort(sort)
            elif isinstance(sort, tuple):
                sort = Sort(*sort)
            self.append(sort)
            return self
    
    def __sub__(self, el: Sort | str):
        """Remove `el` from this SortSequence"""
        self.remove(Sort(el) if isinstance(el, str) else el)
        return self
        
    def __call__(self, to_sort: Iterable[SupportsGetItem]) -> List[SupportsGetItem]:
        """Sequentially apply the sequence of sorts to `to_sort`"""
        for _sort in self:
            to_sort = _sort(to_sort)
        return to_sort
    
    def apply_sorts(self, entries: Iterable[SupportsGetItem]) -> Iterable[SupportsGetItem]:
        return self(entries)


Sorts = Optional[Sort | SortSequence]
"""Type that describes 0-N Sort instances"""
