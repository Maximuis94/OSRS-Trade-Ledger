"""
Module: gui_listbox.py
======================
This module implements the GuiListbox class, which provides a Listbox with additional
features (button header, sorting, filtering, and a bottom label). The complexity of the
Listbox is managed externally via controller classes. This class primarily serves as a
container for the Listbox and its associated controls.

Classes
-------
GuiListbox
    A composite widget that integrates a button header, listbox, scrollbar, and bottom label,
    along with sorting and filtering capabilities.
"""

import tkinter as tk
import tkinter.ttk as ttk
from collections.abc import Callable, Sequence, Iterable
from typing import List, Tuple, Optional, Union, Dict, Any

from gui.base.frame import GuiFrame
from gui.component._listbox.button_header import ButtonHeader
from gui.component._listbox.entry_manager import ListboxEntries
from gui.component._listbox.interfaces import SortLike
from gui.component._listbox.row import ListboxRow
from gui.component.interface.button_header import IButtonHeader
from gui.component.interface.column import IListboxColumn
from gui.component.interface.filter import IFilter, IFilterable
from gui.component.interface.listbox import IListbox
from gui.component.interface.row import IRow
from gui.component.interface.sort import ISortSequence, ISortable
from gui.component.label import GuiLabel
from gui.component._listbox.sort import Sorts
from gui.util.colors import Rgba
from gui.util.decorators import DocstringInheritor
from gui.util.generic import SupportsGetItem


_STYLE_ID = "style.GuiListbox"


def _make_button_header(gui_listbox: "GuiListbox", tag: str) -> IButtonHeader:
    """
    Create a button header for the given GuiListbox.

    Parameters
    ----------
    gui_listbox : GuiListbox
        The listbox for which to create the button header.
    tag : str
        The tag to use for grid placement of the header.

    Returns
    -------
    IButtonHeader
        The created button header.
    """
    if gui_listbox._button_header is not None:
        gui_listbox._button_header.destroy()
    
    button_header = ButtonHeader(gui_listbox, tag, gui_listbox.header_button_sort, gui_listbox._columns)
    return button_header


def _remove_entry_slice(rows: Iterable[IRow], start: Optional[int] = None, end: Optional[int] = None) -> List:
    """
    Remove a slice of entries from the given iterable.

    Parameters
    ----------
    rows : Iterable[IRow]
        The collection of entries.
    start : int, optional
        The starting index of the slice to remove.
    end : int, optional
        The ending index of the slice to remove.

    Returns
    -------
    list
        The list of remaining entries after the specified slice is removed.
    """
    if start is None and end is None:
        return []
    rows_list = list(rows)
    if start is None:
        return rows_list[end:]
    elif end is None:
        return rows_list[:start]
    else:
        return rows_list[:start] + rows_list[end:]


@DocstringInheritor.inherit_docstrings
class GuiListbox(GuiFrame, IListbox, ISortable, IFilterable):
    """
    Composite Listbox widget that integrates sorting, filtering, and additional controls.

    This class creates a Listbox with a button header, a vertical scrollbar, and a bottom label.
    It leverages an external controller for additional complexity and delegates most formatting,
    sorting, and filtering responsibilities to associated manager classes.

    Parameters
    ----------
    frame : GuiFrame
        The parent GuiFrame on which the Listbox is built.
    tag : str
        A tag identifier for the Listbox.
    entries : List[dict or tuple]
        The initial entries. If entries are dictionaries, keys should correspond to ListboxColumn.column.
    entry_width : int, optional
        The width of the entry area (default is 20).
    listbox_height : int, optional
        The height of the listbox (default is 10).
    sticky : str, optional
        The sticky value for grid placement (default is 'N').
    event_bindings : iterable, optional
        An iterable of event binding pairs to be applied to the Listbox.
    select_mode : any, optional
        The selection mode for the listbox (default is tk.SINGLE).
    columns : Sequence[IListboxColumn], optional
        The columns that dictate the row formatting.
    onclick_row : Callable[[SupportsGetItem], Any], optional
        Callback to execute when a row is clicked.
    **kwargs : dict
        Additional keyword arguments.

    Attributes
    ----------
    _columns : tuple of IListboxColumn
        The columns used for formatting the rows.
    _button_header : Optional[IButtonHeader]
        The header of buttons corresponding to columns.
    _listbox : tk.Listbox
        The underlying Tkinter Listbox.
    _scrollbar : ttk.Scrollbar
        The vertical scrollbar associated with the listbox.
    _bottom_label : GuiLabel
        A label displayed at the bottom of the listbox.
    _entries : ListboxEntries
        Manager for the listbox entries.
    _bottom_label_text : Optional[tk.StringVar]
        The StringVar holding the bottom label text.
    _active_filters : Optional[IFilter]
        The filters currently applied.
    _active_sorts : Optional[ISortSequence]
        The sort sequence currently applied.
    _color_scheme : Optional[Callable[[IRow], Rgba]]
        Function to determine the background color of a row.
    _set_row_bgc : bool
        Flag indicating whether to set row background colors.
    _submitted_entries : List[IRow]
        List of entries that have been inserted into the listbox.
    _onclick_row : Callable
        Callback invoked when a row is clicked.
    _style : ttk.Style
        The ttk style applied to the Listbox.
    """

    __slots__ = ("_columns", "_button_header", "_listbox", "_scrollbar", "_bottom_label",
                 "_entries", "_bottom_label_text", "_active_filters", "_active_sorts", "_color_scheme",
                 "_set_row_bgc", "_submitted_entries", "_onclick_row", "_style")

    _columns: Tuple[IListboxColumn, ...]
    _button_header: Optional[IButtonHeader]
    _listbox: tk.Listbox
    _scrollbar: ttk.Scrollbar
    _bottom_label: GuiLabel
    _entries: ListboxEntries
    _bottom_label_text: Optional[tk.StringVar]
    _active_filters: Optional[IFilter]
    _active_sorts: Optional[ISortSequence]
    _color_scheme: Optional[Callable[[IRow], Rgba]]
    _set_row_bgc: bool
    _submitted_entries: List[IRow]
    _onclick_row: Callable
    _style: ttk.Style

    def __init__(self,
                 frame: GuiFrame,
                 tag: str,
                 entries: List[Union[dict, tuple]],
                 entry_width: int = 20,
                 listbox_height: int = 10,
                 sticky: str = 'N',
                 event_bindings: Optional[Iterable[Tuple[str, Callable]]] = None,
                 select_mode: any = tk.SINGLE,
                 columns: Sequence[IListboxColumn] = None,
                 onclick_row: Optional[Callable[[SupportsGetItem], Any]] = None,
                 **kwargs) -> None:
        """
        Initialize the GuiListbox instance.

        This constructor sets up the widget hierarchy:
        a button header, a listbox with a scrollbar, and a bottom label.
        It also creates the ListboxEntries manager to handle sorting and filtering.

        Parameters
        ----------
        frame : GuiFrame
            The parent GuiFrame.
        tag : str
            The identifier tag for the listbox.
        entries : List[dict or tuple]
            The initial entries. If each entry is a dict, keys must correspond to ListboxColumn.column.
        entry_width : int, optional
            The width of the entry area (default is 20).
        listbox_height : int, optional
            The height of the listbox (default is 10).
        sticky : str, optional
            The sticky configuration for the bottom label (default is 'N').
        event_bindings : Iterable[tuple], optional
            Additional event bindings for the listbox.
        select_mode : any, optional
            The selection mode for the listbox (default is tk.SINGLE).
        columns : Sequence[IListboxColumn], optional
            The columns for formatting the entries.
        onclick_row : Callable[[SupportsGetItem], Any], optional
            Callback executed when a row is clicked.
        **kwargs : dict
            Additional keyword arguments.
        """
        # Initialize the base frame.
        self.init_widget_start(frame=frame, tag=tag, **{k: v for k, v in kwargs.items() if k != 'text'})
        self._columns = tuple(columns)
        width = entry_width + 1

        # Create the container frame with a grid layout.
        super().__init__(frame, [("A" * entry_width) + "*", ("B" * entry_width) + "C", "D" * width],
                         relief="ridge")
        
        # Create entries manager.
        if isinstance(entries[0], dict):
            self._entries = ListboxEntries(
                [ListboxRow({c.id: entry[c.column] for c in self._columns}) for entry in entries],
                listbox_columns=self._columns,
                insert_subset=self.fill_listbox,
                **kwargs)
        else:
            self._entries = ListboxEntries(
                [ListboxRow(e) for e in entries],
                listbox_columns=columns,
                insert_subset=self.fill_listbox,
                **kwargs)
        
        # Create button header.
        self._make_button_header()

        # Create scrollbar.
        self._scrollbar = ttk.Scrollbar(self.frame, orient="vertical")
        self._scrollbar.grid(row=1, column=entry_width, sticky="NS")

        # Create listbox.
        self._listbox = tk.Listbox(self.frame,
                                   width=entry_width,
                                   height=listbox_height,
                                   selectmode=select_mode,
                                   yscrollcommand=self._scrollbar.set,
                                   font=self._entries.font.tk)
        self._listbox.grid(row=1, column=0, columnspan=entry_width, sticky="NWE")
        self._scrollbar.config(command=self._listbox.yview)

        # Create bottom label.
        self._bottom_label_text = tk.StringVar(self.frame)
        self._bottom_label = GuiLabel(self, text_variable=self._bottom_label_text, tag='D',
                                      padxy=(0, 0), sticky=sticky)
        self._bottom_label.grid(row=2, column=0, columnspan=width, sticky='WE')

        self._submitted_entries = []
        self.fill_listbox()

        if event_bindings is not None:
            for binding in event_bindings:
                self._listbox.bind(binding[0], binding[1])
        self.row_click = onclick_row

    def insert(self, *rows: IRow, index: Optional[int] = None) -> None:
        """
        Insert one or more rows into the listbox at the specified index.

        Parameters
        ----------
        *rows : IRow
            One or more entries to insert.
        index : int, optional
            The index at which to insert the entries. If None, entries are appended.
        """
        if index is None:
            index = len(self._submitted_entries)
        for idx, row in enumerate(rows):
            idx += index
            try:
                r = str(row)
                self._listbox.insert(idx, r)
                self._submitted_entries.append(row)
                if self._set_row_bgc:
                    self._listbox.itemconfig(idx, bg=self.get_bgc(row))
            except AttributeError as e:
                print('Attribute Error', idx, row)
                raise e

    def add(self, rows: Iterable[IRow], extend: bool = True) -> None:
        """
        Add new rows to the list of entries.

        Parameters
        ----------
        rows : Iterable[IRow]
            The entries to add.
        extend : bool, optional
            If True, append to the existing list; otherwise, replace the current entries.
        """
        if extend:
            self._entries.all = list(self._entries.all) + list(rows)
        else:
            self._entries.all = list(rows)

    def fill_listbox(self,
                     rows: Optional[Iterable[IRow]] = None,
                     filters: Optional[IFilter] = None,
                     extend: bool = True,
                     sorts: Optional[Sorts] = None,
                     is_header_button_callback: bool = False) -> None:
        """
        Populate the listbox with entries after applying sorting and filtering.

        Parameters
        ----------
        rows : Iterable[IRow], optional
            The entries to fill the listbox with. If None, uses the full list.
        filters : IFilter, optional
            The filters to apply.
        extend : bool, optional
            If True, append new entries; if False, clear existing entries before filling.
        sorts : Sorts, optional
            Sort criteria to apply.
        is_header_button_callback : bool, optional
            If True, indicates the fill operation is triggered by a header button callback.
        """
        if not extend:
            self.clear_listbox()
        if rows is not None:
            if not extend:
                self._entries.all = list(rows)
            else:
                self._entries.all += list(rows)

        configured_rows = self._entries.apply_configurations(
            sort_by=self._active_sorts if sorts is None else sorts,
            filters=self._active_filters if filters is None else filters,
            header_callback=is_header_button_callback)
        for r in configured_rows:
            self.insert(r)

    def refresh_listbox(self) -> None:
        """
        Clear and refill the listbox using the current configuration of entries.
        """
        self.clear_listbox()
        for row in self._entries.apply_configurations():
            self.insert(row)

    def clear_listbox(self, start: Optional[int] = None, end: Optional[int] = None) -> None:
        """
        Clear entries from the listbox.

        Parameters
        ----------
        start : int, optional
            The starting index to clear.
        end : int, optional
            The ending index to clear. If None, clears to the end.
        """
        self._listbox.delete(0 if start is None else start, tk.END if end is None else end)
        self._entries.subset = _remove_entry_slice(self._entries.subset, start, end)

    def get_bgc(self, entry: IRow) -> Optional[str]:
        """
        Determine the background color for an entry based on the color scheme.

        Parameters
        ----------
        entry : IRow
            The row entry to evaluate.

        Returns
        -------
        str or None
            The hexadecimal color code if a color scheme is set; otherwise, None.
        """
        if self._color_scheme is None:
            return None
        return self.color_scheme(entry).hexadecimal

    @property
    def row_click(self) -> Callable[[IRow], Any]:
        """
        Get the callback function to execute when a row is clicked.

        Returns
        -------
        Callable[[IRow], Any]
            The row click callback.
        """
        return self._onclick_row

    @row_click.setter
    def row_click(self, onclick_row: Callable[[IRow], Any]) -> None:
        """
        Set the callback function to execute when a row is clicked.

        Parameters
        ----------
        onclick_row : Callable[[IRow], Any]
            The callback function.
        """
        self._listbox.unbind("<<ListboxSelect>>")
        self._onclick_row = onclick_row
        self._listbox.bind("<<ListboxSelect>>", self._row_click)

    @property
    def frame(self) -> ttk.Frame:
        """
        Get the underlying ttk.Frame that contains the listbox.

        Returns
        -------
        ttk.Frame
            The underlying frame.
        """
        return self._frame

    @property
    def color_scheme(self) -> Optional[Callable[[IRow], Rgba]]:
        """
        Get the color scheme function for determining row background colors.

        Returns
        -------
        Callable[[IRow], Rgba] or None
            The color scheme function, or None if not set.
        """
        return self._color_scheme

    @color_scheme.setter
    def color_scheme(self, color_scheme: Callable[[IRow], Rgba]) -> None:
        """
        Set the color scheme function for determining row background colors.

        Parameters
        ----------
        color_scheme : Callable[[IRow], Rgba]
            A function that receives an IRow and returns an Rgba instance.
        """
        self._color_scheme = color_scheme

    @property
    def bottom_label(self) -> str:
        """
        Get the text of the bottom label.

        Returns
        -------
        str
            The text from the bottom label's StringVar.
        """
        return self._bottom_label_text.get()

    @bottom_label.setter
    def bottom_label(self, string: str) -> None:
        """
        Set the text for the bottom label.

        Parameters
        ----------
        string : str
            The new text for the bottom label.
        """
        self._bottom_label_text.set(string)

    @property
    def index(self) -> int:
        """
        Get the index of the currently selected row in the listbox.

        Returns
        -------
        int
            The index of the selected row.
        """
        idx = self._listbox.curselection()[0]
        return idx if isinstance(idx, int) else idx[0]

    def header_button_sort(self, sort: Sorts) -> None:
        """
        Sort the entries using the given sort criteria via a header button callback
        and refill the listbox accordingly.

        Parameters
        ----------
        sort : Sorts
            The sort criteria to apply.
        """
        self.fill_listbox(sorts=sort, is_header_button_callback=True, extend=False)

    def _make_button_header(self) -> None:
        """
        Create the button header for the listbox.

        Each visible column is assigned a button, and the header is displayed at the top.
        """
        self._button_header = _make_button_header(self, "A")

    def _row_click(self, event: tk.Event) -> None:
        """
        Handle row click events by executing the row click callback.

        Parameters
        ----------
        event : tk.Event
            The event object from the listbox selection.
        """
        if self._onclick_row is not None:
            idx = self.index
            entry = self._entries.subset[idx]
            self._onclick_row(index=idx, entry=entry, event=event)

    @property
    def default_sort(self) -> Sorts:
        """
        Get the default sort sequence applied to the listbox entries.

        Returns
        -------
        Sorts
            The default sort sequence.
        """
        return self._entries.default_sort_sequence

    @default_sort.setter
    def default_sort(self, sort_by: Optional[SortLike] = None) -> None:
        """
        Set the default sort sequence for the listbox entries.

        Parameters
        ----------
        sort_by : SortLike, optional
            The new default sort sequence.
        """
        self._entries.initial_sort(sort_by)

    @property
    def n_entries(self) -> int:
        """
        Get the number of entries in the listbox.

        Returns
        -------
        int
            The count of entries.
        """
        return len(self._entries)

    def sort(self, sort_by: ISortSequence) -> Tuple[IRow, ...]:
        """
        Sort the listbox entries using the specified sort sequence.

        Parameters
        ----------
        sort_by : ISortSequence
            The sort sequence to apply.

        Returns
        -------
        tuple of IRow
            The sorted entries.
        """
        return self._entries.sort(sort_by)

    def filter(self, *filters: IFilter) -> Tuple[IRow, ...]:
        """
        Filter the listbox entries using the provided filters.

        Parameters
        ----------
        *filters : IFilter
            One or more filters to apply.

        Returns
        -------
        tuple of IRow
            The filtered entries.
        """
        return self._entries.filter(filters)

    def __getitem__(self, index: int) -> IRow:
        """
        Retrieve the entry at the specified index from the submitted entries.

        Parameters
        ----------
        index : int
            The index of the entry to retrieve.

        Returns
        -------
        IRow
            The entry at the given index.
        """
        return self._submitted_entries[index]

    def __len__(self) -> int:
        """
        Return the number of entries in the listbox.

        Returns
        -------
        int
            The number of entries.
        """
        return len(self._entries)
