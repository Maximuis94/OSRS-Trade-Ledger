""""""
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Type, Any, List

from gui.util.str_formats import strf_number, strf_unix
from util.str_formats import shorten_string

LISTBOX_COLUMNS: list = []

@dataclass(frozen=True, slots=True, match_args=True)
class ListboxColumn:
    """
    Represents a column in a tkinter Listbox with formatting, sorting, and filtering functionality.
    """
    
    column: str
    """The identifier of the associated pandas DataFrame column."""
    
    width: int
    """The display width of the column in characters."""
    
    dtype: Type[int | float | str | bool]
    """The data type of the values in the column."""
    
    format: Callable[[Any], str]
    """A callable that formats a cell value into a string."""
    
    button_click: Callable
    """A callable to handle button click events associated with the column."""
    
    header: str = None
    """The display name of the column header."""
    
    df_dtype: str = None
    """The data type in the DataFrame or data source, expressed as a string."""
    
    visible: bool = True
    """Determines whether the column is visible in the Listbox."""
    
    push_left: bool = True
    """Determines the alignment of the column; True for left, False for right."""
    
    id: int = None
    """A unique identifier assigned to this ListboxColumn"""
    
    @staticmethod
    def make(column: str, width: int, format: Callable[[Any], str] = lambda s: str(s), header: str = None,
             button_click: Callable = None, is_visible: bool = True, is_number: bool = True,
             push_left: bool = True):
        """
        Creates a new immutable `ListboxColumn` instance.

        Parameters
        ----------
        column : str
            The identifier for the column, typically referencing a field in the data source.
        width : int
            The display width of the column in characters.
        format : Callable[[Any], str], optional
            A callable to format the cell value into a string, by default `lambda s: str(s)`.
        header : str, optional
            The display name of the column header. If `None`, derived from `column`, by default `None`.
        button_click : Callable, optional
            A callable to handle button click events associated with the column, by default `None`.
        is_visible : bool, optional
            Determines whether the column is visible in the Listbox, by default `True`.
        is_number : bool, optional
            Indicates whether the column contains numerical values, by default `True`.
        push_left : bool, optional
            Determines the alignment of the column; True for left, False for right, by default `True`.
        id : int
            A unique identifier assigned to this ListboxColumn; incremental and based on the size of the LISTBOX_COLUMNS
            tuple.

        Returns
        -------
        ListboxColumn
            A new instance of the `ListboxColumn` class.
        """
        global LISTBOX_COLUMNS
        lbc = ListboxColumn(
            column=column,
            width=width,
            dtype=int if is_number else str,
            format=format,
            button_click=button_click,
            header=column.replace('_', ' ').capitalize() if header is None else header,
            df_dtype="number" if is_number else "string",
            visible=is_visible,
            push_left=push_left,
            id=len(LISTBOX_COLUMNS)
        )
        LISTBOX_COLUMNS.append(lbc)
        return lbc
    
    def get_value(self, x: Any, print_warning: bool = True) -> str:
        """
        Formats a value for display in the column. The result is designed to be part of a ListboxEntry.

        Parameters
        ----------
        x : Any
            The value to format.
        print_warning : bool, optional
            Whether to print a warning if the formatted value exceeds the column width, by default `True`.

        Returns
        -------
        str
            The formatted value, truncated to the column's width.

        Raises
        ------
        TypeError
            If `self.format` is not callable or `x` cannot be formatted.
        """
        try:
            # print(x)
            f = (
                f"{self.format(x): {'<' if self.push_left else '>'}{self.width - 1}} "
                if self.visible
                else ""
            )
            if len(f) > self.width and print_warning:
                print(
                    f"Formatted value for column {self.header} value={f} exceeded configured "
                    f"width {self.width} (width={len(f)})"
                )
            return f
        except TypeError as e:
            print(f"Error formatting value: {x}")
            raise e
    
    def __str__(self):
        return self.column
    
    def to_string(self) -> str:
        """String representation of all keys and values found within this ListboxColumn"""
        return f"""({", ".join([f"{k}={self.__getattribute__(k)}" for k in self.__dir__() if k in self.__match_args__])})"""
        
    @staticmethod
    def get_by_id(listbox_column_id: int):
        """ Returns the ListboxColumn that corresponds to `id`. Only applies to ListboxColumns defined via 'make'. """
        global LISTBOX_COLUMNS
        return LISTBOX_COLUMNS[listbox_column_id]

    @staticmethod
    def get_all() -> list:
        """Return a list of all ListboxColumns"""
        global LISTBOX_COLUMNS
        return LISTBOX_COLUMNS
