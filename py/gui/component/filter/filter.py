"""
Module with Filter logic

"""
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import List, Optional

from overrides import override

from gui.component.interface.row import IRow
from gui.component.interface.filter import IFilter
from gui.util.generic import SupportsGetItem, Number


FilterLike = Callable[[Number | str, Number | str], bool]


@dataclass(slots=True, match_args=True, frozen=True, eq=True, order=True)
class UndefinedFilter:
    """Filter without a threshold value"""
    
    attribute_name: str
    """The name of the attribute that is to be filtered"""
    
    filter: FilterLike
    """Name of the filter function to apply"""
    
    def apply_filters(self, row: SupportsGetItem, threshold) -> bool:
        """Apply this filter to `attribute_name` attribute of `row`, given `threshold`."""
        return self.filter(threshold, row if isinstance(row, Number) else row[self.attribute_name])
    
    def __call__(self, entry: SupportsGetItem, threshold) -> bool:
        """Apply this Filter to `entry`; return whether it should be filtered."""
        return self.apply_filters(threshold, entry)
    
    def __iter__(self):
        """Iterate over this class as if it were an iterable with only this instance"""
        yield self
    
    @property
    def function(self) -> FilterLike:
        """Filter function that corresponds to `_f_name`"""
        return FilterSet[self._f_name]
    

@dataclass(slots=True, match_args=True, frozen=True, eq=True, order=True)
class Filter(IFilter, UndefinedFilter):
    """
    Filter dataclass used for filtering Entries. The filter can be called with an entry passed, which will apply the
    filter to the given entry.
    
    Examples
    --------
    # Define Filter my_filter, which will filter something if the price is smaller than the threshold given
    my_filter = Filter("price", lambda p, t: p < t)
    
    if my_filter():
    
    
    """
    threshold: Number | str
    """Threshold value used in the filter function"""
    
    def apply_filters(self, row: SupportsGetItem, *args, **kwargs) -> bool:
        return self.filter(self.threshold, row[self.attribute_name])
    
    @override(check_signature=False)
    def __call__(self, entry: SupportsGetItem, *args, **kwargs) -> bool:
        """Apply this Filter to `entry`; return whether it should be filtered."""
        return self.apply_filters(entry)
    
    def __iter__(self):
        """Iterate over this class as if it were an iterable with only this instance"""
        yield self
    
    @property
    def function(self) -> FilterLike:
        """Filter function that corresponds to `_f_name`"""
        return FilterSet[self._f_name]


@dataclass(slots=True)
class FilterSequence(List, IFilter):
    """Class with a sequence of Filters. Can be called to apply all of its Filters to the given list of entries."""
    def __init__(self, *filters):
        super().__init__()
        for filter in filters:
            self + filter
    
    def __add__(self, filters: Filter | Iterable[Filter]) -> List[Filter]:
        for f in filters:
            if isinstance(f, IFilter):
                self.append(f)
        
        return self
    
    def __sub__(self, *filters: Filter | Iterable[Filter]):
        """Remove `el` from this FilterSequence"""
        for f in filters:
            self.remove(f)
        return self
    
    def __call__(self, row: IRow) -> bool:
        """Sequentially apply the sequence of sorts to `to_sort`"""
        return self.apply_filters(row)
    
    def append(self, filter: IFilter):
        """Add an IFilter instance to this FilterSequence"""
        if isinstance(filter, IFilter):
            super().append(filter)
    
    def apply_filters(self, row: SupportsGetItem) -> bool:
        for f in self:
            if f(row):
                return True
        return False


FilterSet = Optional[Filter | FilterSequence]
"""Type that describes 0-N Filter instances"""
