"""
Listbox-related inferfaces
"""
from abc import abstractmethod, ABC, ABCMeta
from collections.abc import Sequence, Iterable
from typing import Protocol, Tuple, Callable

from gui.component.interface.filter import FilterMeta
from gui.component.interface.row import IRow
from gui.util.generic import SupportsGetItem

FilterFunction = Callable[[Sequence[any]], bool]
SortScoreFunction = Callable[[Sequence[any]], int | float]
StringFormatFunction = Callable[[Sequence[any]], str]


class IFilter(metaclass=FilterMeta):
    """
    Filter class that used for filtering Entries. Instances are to be applied via IFilter(entry, threshold)

    Examples
    --------

    # Define Filter my_filter, which will filter something if the price is smaller than the threshold given
    my_filter = Filter("price", lambda p, t: p < t)

    if my_filter():


    """
    @abstractmethod
    def __call__(self, entry: SupportsGetItem) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    def __iter__(self):
        raise NotImplementedError


class IListbox(ABC):
    """Interface for a Listbox with limited additional capabilities"""


class IListboxConfig(Protocol):
    """
    Protocol for Listbox Configurations.
    A Listbox Configuration is an Interface that provides means to filter, sort and to represent a row as a string.
    Additionally, it provides the means to change the sort/filter/printing properties
    """
    @abstractmethod
    def load_data(self) -> IRow:
        """Load all rows relevant for this Listbox configuration."""
        raise NotImplementedError

    @abstractmethod
    def sort_score_row(self, row: IRow) -> int | float:
        """Compute the sort rank score for `row`"""
        raise NotImplementedError

    @abstractmethod
    def filter_row(self, row: IRow) -> bool:
        """Determine if `row` should be filtered, given its values"""
        raise NotImplementedError
    
    @abstractmethod
    def strf_row(self, row: IRow) -> str:
        """Format a row as a string that can be placed within a listbox"""
        raise NotImplementedError


class SortLike(Protocol):
    """Protocol for an object that has an underlying sorting task"""
    @abstractmethod
    def apply_sorts(self, entries: Iterable[SupportsGetItem]) -> Tuple[SupportsGetItem, ...]: ...
    
    @abstractmethod
    def __iter__(self): ...
