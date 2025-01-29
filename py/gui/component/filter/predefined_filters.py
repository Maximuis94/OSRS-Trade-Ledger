"""
Module with filter functions and the means to access them.
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
from collections.abc import Callable
from enum import Enum, EnumMeta
from typing import Dict

from gui.component.filter.filter import Filter, UndefinedFilter
from gui.util.generic import SupportsGetItem

Number = int | float
FilterFunctionProtocol = Callable[[Number | str, Number | str], bool] | Callable[[str, str], bool]


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
    
    def __new__(cls, func: Callable[[Number | str, Number | str], bool] | Callable[[str, str], bool], name: str, *aliases: str):
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
    
    def add(cls, func: Callable[[Number | str, Number | str], bool] | Callable[[str, str], bool], name: str, *aliases: str):
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

    LESS_THAN = FilterFunction(_lt, "LESS_THAN",  "S_LT", "N_LT", "_LT")
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
    
    def apply_filters(self, row: SupportsGetItem, threshold, attribute_name )-> bool:
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


# if __name__ == "__main__":
#     print(STR_FILTERS.EQUAL("aaaaaaaaaaaaaaaaa", "aaaaaa"))
#     print(STR_FILTERS.NOT_EQUAL("aaaaaaaaaaaaaaaaa", "aaabaa"))
#     print(Filters["HAS"]("aaaaaaaaaaaaaaaaa", "aaaaaa"))
#     print(Filters["HAS_NOT"]("aaaaaaaaaaaaaaaaa", "aaabaa"))
