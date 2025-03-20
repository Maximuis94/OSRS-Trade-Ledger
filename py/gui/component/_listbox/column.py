from typing import Type, Callable, Any, List, Optional
from dataclasses import dataclass
from gui.component.interface.column import IListboxColumn

# Global list for storing created ListboxColumn instances.
LISTBOX_COLUMNS: List['ListboxColumn'] = []


@dataclass(frozen=True, slots=True, match_args=True, kw_only=True)
class ListboxColumn(IListboxColumn):
    """
    Represents a column in a tkinter Listbox with formatting, sorting, and filtering functionality.

    Parameters
    ----------
    id : int
        A unique identifier assigned to this ListboxColumn.
    column : str
        The identifier of the associated pandas DataFrame column.
    width : int
        The display width of the column in characters.
    dtype : Type[int | float | str | bool]
        The data type of the values in the column.
    format : Callable[[Any], str]
        A callable that formats a cell value into a string.
    button_click : Callable
        A callable to handle button click events associated with the column.
    header : str
        The display name of the column header.
    df_dtype : str
        The data type in the DataFrame or data source, expressed as a string.
    visible : bool
        Determines whether the column is visible in the Listbox.
    push_left : bool
        Determines the alignment of the column; True for left, False for right.

    Attributes
    ----------
    is_visible : bool
        Property that returns the visibility of the column.
    """

    id: int
    column: str
    width: int
    dtype: Type[int | float | str | bool]
    format: Callable[[Any], str]
    button_click: Callable
    header: str
    df_dtype: str
    visible: bool
    push_left: bool

    @property
    def is_visible(self) -> bool:
        """
        Determine if the column is visible.

        Returns
        -------
        bool
            True if the column is visible, False otherwise.
        """
        return self.visible

    @staticmethod
    def make(column: str,
             width: int,
             format: Callable[[Any], str] = lambda s: str(s),
             header: Optional[str] = None,
             button_click: Optional[Callable] = None,
             is_visible: bool = True,
             is_number: bool = True,
             push_left: bool = True) -> 'ListboxColumn':
        """
        Create a new immutable ListboxColumn instance.

        Parameters
        ----------
        column : str
            The identifier for the column, typically referencing a field in the data source.
        width : int
            The display width of the column in characters.
        format : Callable[[Any], str], optional
            A callable to format the cell value into a string. Defaults to converting the value to a string.
        header : str, optional
            The display name of the column header. If None, it is derived from `column`.
        button_click : Callable, optional
            A callable to handle button click events associated with the column.
        is_visible : bool, optional
            Determines whether the column is visible in the Listbox. Defaults to True.
        is_number : bool, optional
            Indicates whether the column contains numerical values. Defaults to True.
        push_left : bool, optional
            Determines the alignment of the column; True for left, False for right. Defaults to True.

        Returns
        -------
        ListboxColumn
            A new instance of the ListboxColumn class.
        """
        global LISTBOX_COLUMNS
        lbc = ListboxColumn(
            id=len(LISTBOX_COLUMNS),
            column=column,
            width=width,
            dtype=int if is_number else str,
            format=format,
            button_click=button_click,
            header=column.replace('_', ' ').capitalize() if header is None else header,
            df_dtype="number" if is_number else "string",
            visible=is_visible,
            push_left=push_left
        )
        LISTBOX_COLUMNS.append(lbc)
        return lbc

    def get_value(self, x: Any, print_warning: bool = True) -> str:
        """
        Format a value for display in the column.

        The result is designed to be part of a ListboxEntry and is truncated to fit the column's width.

        Parameters
        ----------
        x : Any
            The value to format.
        print_warning : bool, optional
            Whether to print a warning if the formatted value exceeds the column width.
            Defaults to True.

        Returns
        -------
        str
            The formatted value, truncated to the column's width.

        Raises
        ------
        TypeError
            If `self.format` is not callable or if `x` cannot be formatted.
        """
        try:
            formatted_value = self.format(x)
            alignment = "<" if self.push_left else ">"
            # Format with width-1 characters and add a trailing space.
            result = f"{formatted_value:{alignment}{self.width - 1}} " if self.visible else ""
            if len(result) > self.width and print_warning:
                print(
                    f"Formatted value for column {self.header} exceeded configured width {self.width} "
                    f"(actual length: {len(result)})"
                )
            return result
        except TypeError as e:
            print(f"Error formatting value: {x}")
            raise e

    def __str__(self) -> str:
        """
        Return the column identifier.

        Returns
        -------
        str
            The column attribute.
        """
        return self.column

    def to_string(self) -> str:
        """
        Return a string representation of key attributes of this ListboxColumn.

        Returns
        -------
        str
            A string containing key names and values.
        """
        keys = [k for k in self.__dir__() if k in self.__match_args__]
        return f"({', '.join(f'{k}={self.__getattribute__(k)}' for k in keys)})"

    @staticmethod
    def get_by_id(listbox_column_id: int) -> 'ListboxColumn':
        """
        Retrieve the ListboxColumn corresponding to the given ID.

        Parameters
        ----------
        listbox_column_id : int
            The unique identifier for the ListboxColumn.

        Returns
        -------
        ListboxColumn
            The ListboxColumn instance with the specified ID.
        """
        global LISTBOX_COLUMNS
        return LISTBOX_COLUMNS[listbox_column_id]

    @staticmethod
    def get_all() -> List['ListboxColumn']:
        """
        Return a list of all ListboxColumn instances created via make().

        Returns
        -------
        list of ListboxColumn
            A list of all ListboxColumn instances.
        """
        global LISTBOX_COLUMNS
        return LISTBOX_COLUMNS
