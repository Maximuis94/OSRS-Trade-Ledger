"""
Module with Row metaclass and interface
"""
from abc import ABC, abstractmethod, ABCMeta
from typing import Iterable


class RowMeta(ABCMeta):
    """Listbox Metaclass for defining custom isinstance and issubclass logic"""
    def __instancecheck__(self, instance):
        try:
            instance.strf
            instance.__getitem__
            instance.__iter__
            instance.__len__
            return True
        except AttributeError:
            return False
    
    # def __subclasscheck__(self, subclass):
    #     try:
    #         subclass.strf
    #         subclass.__getitem__
    #         subclass.__iter__
    #         subclass.__len__
    #         return True
    #     except AttributeError:
    #         return False


class IRow(ABC, metaclass=RowMeta):
    """
    ListboxEntry. It represents a single row found within a listbox.
    """
    
    @abstractmethod
    def strf(self, column_order: Iterable[int] = None) -> str:
        """
        Formatted string representation of this Row, optionally with a custom column order.
        
        Parameters
        ----------
        column_order : Optional[Iterable[int]], None by default
            If passed, apply the format to a custom order of elements.

        Returns
        -------
        str
            Formatted string representation of this Row.
        """
        raise NotImplementedError
        
    @abstractmethod
    def __getitem__(self, item: int | str):
        """Returns the value specified by `item`"""
        raise NotImplementedError
    
    @abstractmethod
    def __iter__(self):
        """Iterate over values of this Row. Each value represents the value of a listbox column"""
        raise NotImplementedError
    
    @abstractmethod
    def __len__(self) -> int:
        """Returns the number of values in the row"""
        raise NotImplementedError
