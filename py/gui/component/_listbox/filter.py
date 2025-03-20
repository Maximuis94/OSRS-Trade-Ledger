"""
Module with Filter logic and the means to access them.
The filter functions listed below are basic arithmetic and string comparison operators.
The list can be extended via FilterFunction, which will automatically register the function in the dict.

Examples
--------
print(STR_FILTERS.EQUAL("123", "123"))
print(STR_FILTERS.NOT_EQUAL("123", "123"))
print(Filters["EQUAL"](123, 123))
print(Filters.EQUAL(12, 123))
print(Filters["EQUAL"](12, 123))
print(Filters.STRING_IN("aaaaaaaaaaaaaaaaa", "aaaaaa"))
print(Filters.STRING_IN("aaaaaaaaaaaaaaaaa", "aaabaa"))
print(Filters["HAS"]("aaaaaaaaaaaaaaaaa", "aaaaaa"))
print(Filters["HAS_NOT"]("aaaaaaaaaaaaaaaaa", "aaabaa"))
"""
from enum import Enum, EnumMeta

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import List, Optional, Dict

from overrides import override

from gui.component.interface.row import IRow
from gui.component.interface.filter import IFilter
from gui.util.generic import SupportsGetItem, Number


FilterLike = Callable[[Number | str, Number | str], bool]

Number = int | float
FilterFunctionProtocol = Callable[[Number | str, Number | str], bool] | Callable[[str, str], bool]


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


def _eq(n: Number | str, v: Number | str) -> bool:
    """True if `n` is equal to `v`."""
    return n == v


def _ne(n: Number | str, v: Number | str) -> bool:
    """True if `n` is not equal to `v`."""
    return n != v


def _gt(n: Number | str, v: Number | str) -> bool:
    """True if `n` is greater than `v`."""
    return n > v


def _ge(n: Number | str, v: Number | str) -> bool:
    """True if `n` is greater than or equal to `v`."""
    return n >= v


def _lt(n: Number | str, v: Number | str) -> bool:
    """True if `n` is less than `v`."""
    return n < v


def _le(n: Number | str, v: Number | str) -> bool:
    """True if `n` is less than or equal to `v`."""
    return n <= v


def s_in(s: str, t: str) -> bool:
    """True if `s` contains `t`."""
    return t in s


def s_nin(s: str, t: str) -> bool:
    """True if `s` does not contain `t`."""
    return t not in s


_alias_func_mapping: Dict[str, str] = {

}


class FilterFunction:
    """Filter Function; if a new class instance is made, it is registered in the dict under every alias provided."""
    func: Callable[[Number | str, Number | str], bool] | Callable[[str, str], bool]
    
    def __new__(cls, func: Callable[[Number | str, Number | str], bool] | Callable[[str, str], bool], name: str,
                *aliases: str):
        global _alias_func_mapping
        _alias_func_mapping.update({alias: name for alias in aliases})
        return UndefinedFilter(name, func)


class FiltersMeta(EnumMeta):
    def __getitem__(self, function_name: str) -> FilterFunctionProtocol:
        """Get a specific FilterFunction via its name or alias and return it"""
        global _alias_func_mapping
        try:
            return Filters.__getattribute__(Filters, _alias_func_mapping.get(function_name, function_name).upper())
        except AttributeError:
            print(f"Unable to identify a function named '{function_name}'. The following names are registered;")
            print("\n".join(Filters._member_names_ + list(_alias_func_mapping.keys())))
            
            raise KeyError("Failed to identify a function for the given name. See the list above for valid names.")
    
    def add(cls, func: Callable[[Number | str, Number | str], bool] | Callable[[str, str], bool], name: str,
            *aliases: str):
        return FilterFunction(func, name, *aliases)


class Filters(Enum, metaclass=FiltersMeta):
    """Enumeration with arithmetic and string filter operators. S_ prefix indicates a string-only operation."""
    EQUAL = FilterFunction(_eq, "EQUAL", "S_EQ", "N_EQ", "_EQ")
    """True if `v` is equal to `t`."""
    
    NOT_EQUAL = FilterFunction(_ne, "NOT_EQUAL", "S_NE", "N_NE", "_NE")
    """True if `v` is not equal to `t`."""
    
    GREATER_THAN = FilterFunction(_gt, "GREATER_THAN", "S_GT", "N_GT", "_GT")
    """True if `v` is greater than `t`."""
    
    GREATER_EQUAL_THAN = FilterFunction(_ge, "GREATER_EQUAL_THAN", "S_GE", "N_GE", "_GE")
    """True if `v` is greater than or equal to `t`."""
    
    LESS_THAN = FilterFunction(_lt, "LESS_THAN", "S_LT", "N_LT", "_LT")
    """True if `v` is less than `t`."""
    
    LESS_EQUAL_THAN = FilterFunction(_le, "LESS_EQUAL_THAN", "S_LE", "N_LE", "_LE")
    """True if `v` is less than or equal to `t`."""
    
    STRING_IN = FilterFunction(s_in, "STRING_IN", "S_IN", "HAS")
    """True if `s` contains `t`."""
    
    STRING_NOT_IN = FilterFunction(s_nin, "STRING_NOT_IN", "S_NIN", "HAS_NOT")
    """True if `s` does not contain `t`."""
    
    def __call__(self, v: Number | str, t: Number | str) -> bool:
        """Apply the filter function with `v` and `t` as args. """
        return self.value(v, t)
    
    def apply_filters(self, v: Number | str, t: Number | str) -> bool:
        """Apply the filter function with `v` and `t` as args. """
        return self.value(v, t)


class STR_FILTERS(Enum):
    """All predefined Filters applicable to strings. Greater/lesser than are not included."""
    EQUAL = Filters.EQUAL
    NOT_EQUAL = Filters.NOT_EQUAL
    STRING_IN = Filters.STRING_IN
    STRING_NOT_IN = Filters.STRING_NOT_IN
    
    def apply_filters(self, row: SupportsGetItem, threshold, attribute_name) -> bool:
        return self.value(row[attribute_name], threshold)
    
    def __call__(self, v: str, t: str) -> bool:
        return self.value(v, t)


class NUM_FILTERS(Enum):
    """All predefined Filters applicable to numbers."""
    
    EQUAL = Filters.EQUAL
    NOT_EQUAL = Filters.NOT_EQUAL
    GREATER_THAN = Filters.GREATER_THAN
    GREATER_EQUAL_THAN = Filters.GREATER_EQUAL_THAN
    LESS_THAN = Filters.LESS_THAN
    LESS_EQUAL_THAN = Filters.LESS_EQUAL_THAN
    
    def apply_filters(self, row: SupportsGetItem, threshold: Number, attribute_name: str) -> bool:
        return self.value(row[attribute_name], threshold)
    
    def __iter__(self):
        yield self
    
    def __call__(self, v: Number, t: Number) -> bool:
        return self.value(v, t)
    
    def get(self, attribute_name: str, threshold: Number) -> Filter:
        return Filter(attribute_name, self.value.value, threshold)


__all__ = "Filters", "FilterFunction", "STR_FILTERS", "NUM_FILTERS"
