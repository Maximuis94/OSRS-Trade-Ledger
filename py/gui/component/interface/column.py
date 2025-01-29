"""
Module with ListboxColumn interface

"""
from abc import ABC, abstractmethod, ABCMeta
from typing import Callable, Any


class ListboxColumnMeta(ABCMeta):
    """Metaclass for ListboxColumn interface."""
    def __instancecheck__(self, instance):
        return (
            hasattr(instance, 'get_value') and
            hasattr(instance, '__str__') and
            hasattr(instance, 'to_string') and
            hasattr(instance, 'make') and
            hasattr(instance, 'get_by_id') and
            hasattr(instance, 'get_all')
        )


class IListboxColumn(ABC, metaclass=ListboxColumnMeta):
    """
    Interface for the ListboxColumn class, defining the structure and methods.
    """

    @staticmethod
    @abstractmethod
    def make(column: str, width: int, format: Callable[[Any], str] = ..., header: str = None,
             button_click: Callable = None, is_visible: bool = True, is_number: bool = True,
             push_left: bool = True):
        """
        Creates a new ListboxColumn instance.

        Parameters
        ----------
        column : str
            Identifier for the column.
        width : int
            Display width of the column.
        format : Callable[[Any], str], optional
            Callable to format the cell value, by default `lambda s: str(s)`.
        header : str, optional
            Display name of the column header, by default derived from `column`.
        button_click : Callable, optional
            Callable for button click events, by default `None`.
        is_visible : bool, optional
            Indicates if the column is visible, by default `True`.
        is_number : bool, optional
            Indicates if the column contains numerical values, by default `True`.
        push_left : bool, optional
            Alignment of the column, by default `True` (left).

        Returns
        -------
        IListboxColumn
            A new instance of the ListboxColumn.
        """
        raise NotImplementedError

    @abstractmethod
    def get_value(self, x: Any, print_warning: bool = True) -> str:
        """
        Formats a value for display in the column.

        Parameters
        ----------
        x : Any
            The value to format.
        print_warning : bool, optional
            Whether to print a warning if the formatted value exceeds the column width.

        Returns
        -------
        str
            The formatted value.
        """
        raise NotImplementedError

    @abstractmethod
    def __str__(self) -> str:
        """
        Returns the string representation of the column identifier.
        """
        raise NotImplementedError

    @abstractmethod
    def to_string(self) -> str:
        """
        Returns the string representation of all keys and values in the column.
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def get_by_id(listbox_column_id: int):
        """
        Retrieves the ListboxColumn corresponding to the given ID.

        Parameters
        ----------
        listbox_column_id : int
            The ID of the ListboxColumn.

        Returns
        -------
        ListboxColumn
            The corresponding ListboxColumn instance.
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def get_all() -> list:
        """
        Retrieves all ListboxColumn instances.

        Returns
        -------
        list
            A list of all ListboxColumn instances.
        """
        raise NotImplementedError
    
    @property
    @abstractmethod
    def width(self) -> int:
        """The maximum amount of characters any value of this column is expected to need."""
        raise NotImplementedError
    
    @property
    @abstractmethod
    def is_visible(self) -> bool:
        """Flag that indicates whether this ListboxColumn is visible"""
        raise NotImplementedError
    
    @property
    @abstractmethod
    def header(self) -> str:
        """The text as displayed on the header button"""
        raise NotImplementedError
    
    @property
    @abstractmethod
    def column(self) -> str:
        """The name of the column, as used within the database"""
        raise NotImplementedError
