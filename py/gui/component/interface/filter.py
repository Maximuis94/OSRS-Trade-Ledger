"""
Module with an interface for extending a data structure with filter-like capabilities

"""
from abc import ABC, abstractmethod, ABCMeta
from typing import Tuple, Callable

from gui.component.interface.row import IRow
from gui.util.generic import check_callable_return_type


class FilterMeta(ABCMeta):
    """Filter meta-class"""
    def __instancecheck__(self, instance):
        try:
            return check_callable_return_type(instance.apply_filters, bool) and hasattr(instance, '__iter__')
        except AttributeError:
            return False
        
    def __subclasscheck__(cls, subclass):
        return super().__subclasscheck__(subclass)


class IFilter(ABC, metaclass=FilterMeta):
    """Interface class for a single Filter. If a filter evaluates to True, the entity will be filtered out."""
    @abstractmethod
    def apply_filters(self, row: IRow) -> bool:
        """Apply this Filter to the given row"""
        raise NotImplementedError
    
    @abstractmethod
    def __iter__(self):
        """Iterate over this (set of) filter(s). In case of a single filter, yield just that filter"""
        raise NotImplementedError


class FilterableMeta(ABCMeta):
    """Filter meta-class"""
    def __instancecheck__(self, instance):
        return check_callable_return_type(instance.apply_filters, tuple) and hasattr(instance, '__iter__')
        
    def __subclasscheck__(cls, subclass):
        return super().__subclasscheck__(subclass)


class IFilterable(ABC):
    """Interface class for a set of rows to which a FilterSequence can be applied"""
    @abstractmethod
    def filter(self, filters: IFilter) -> Tuple[IRow, ...]:
        """Apply all Filters to `entries` and return the filtered subset"""
        raise NotImplementedError
