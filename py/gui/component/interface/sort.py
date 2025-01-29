"""
Module with an interface for extending a data structure with filter-like capabilities

"""
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Tuple, Protocol

from gui.util.generic import SupportsGetItem


class ISortBy(ABC):
    """Interface for a single sort command"""
    @abstractmethod
    def apply_sorts(self, entries: Iterable[SupportsGetItem]) -> Tuple[SupportsGetItem, ...]:
        """Apply this sort and return `entries` sorted"""
        raise NotImplementedError
    
    @abstractmethod
    def flip(self):
        """Reverse the sort order"""
        raise NotImplementedError
    
    @abstractmethod
    def __iter__(self):
        raise NotImplementedError


class ISortSequence(ABC):
    """Interface for a SortSequence that is applicable to a data structure"""
    @abstractmethod
    def apply_sorts(self, entries: Iterable[SupportsGetItem]) -> Tuple[SupportsGetItem, ...]:
        """Sequentially apply all sorts and return the sorted data structure"""
        raise NotImplementedError
    
    @abstractmethod
    def __iter__(self):
        """Iterate over this SortSequence"""
        raise NotImplementedError
    

class ISortable(ABC):
    """Interface for a data structure that can be sorted"""
    @abstractmethod
    def sort(self, sort_by: ISortSequence) -> Tuple[SupportsGetItem, ...]:
        """Sort this datastructure"""
        raise NotImplementedError
