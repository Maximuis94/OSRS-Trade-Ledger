"""
Module with generic/abstract classes that are constructed from builtins
"""
from abc import ABC, abstractmethod
from typing import *
from collections.abc import *


def check_callable_return_type(func: Callable, expected_return_type: type) -> bool:
    """
    Checks if the return type of a given Callable matches the expected type.

    Parameters
    ----------
    func : Callable
        The function to check.
    expected_return_type : type
        The expected return type.

    Returns
    -------
    bool
        True if the function's return type matches the expected type, otherwise False.
    """
    try:
        return get_type_hints(func)['return'] == expected_return_type
    except KeyError:
        return False
    
    
class IRow:
    ...


class IFilter:
    ...


class IFilterable(ABC):
    """Interface class for a set of rows to which a FilterSequence can be applied"""
    @abstractmethod
    def filter(self, filters: IFilter) -> Tuple[IRow, ...]:
        """Apply all Filters to `entries` and return the filtered subset"""
        raise NotImplementedError


# T_n = TypeVar('T_n', bound='Number')
#
#
# class Number(Protocol):
#     """A number, or an entity that can be used as one."""
#     def __add__(self, other: Union[int, float]) -> T_n: ...
#     def __sub__(self, other: Union[int, float]) -> T_n: ...
#     def __mul__(self, other: Union[int, float]) -> T_n: ...
#     def __truediv__(self, other: Union[int, float]) -> T_n: ...
#     def __floordiv__(self, other: Union[int, float]) -> T_n: ...
#     def __mod__(self, other: Union[int, float]) -> T_n: ...
#     def __pow__(self, exponent: Union[int, float], modulo: int = ...) -> T_n: ...
#     def __neg__(self) -> T_n: ...
#     def __pos__(self) -> T_n: ...
#     def __abs__(self) -> T_n: ...
#     def __eq__(self, other: object) -> bool: ...
#     def __lt__(self, other: Union[int, float]) -> bool: ...
#     def __le__(self, other: Union[int, float]) -> bool: ...
#     def __gt__(self, other: Union[int, float]) -> bool: ...
#     def __ge__(self, other: Union[int, float]) -> bool: ...
#     def __int__(self) -> int: ...
#     def __float__(self) -> float: ...
Number = int | float

T = TypeVar('T')


class SupportsGetItem(Protocol[T]):
    """
    Protocol for objects that support __getitem__ and are iterable.
    """
    def __getitem__(self, item: any) -> T: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[T]: ...


if __name__ == "__main__":
    print(get_type_hints(IFilterable.filter)['return'] == Tuple[IRow, ...])
    ...
