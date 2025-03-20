"""
Module: button_header.py
========================
This module implements the ButtonHeader class, which represents an array of GuiButtons.
ButtonHeader is responsible for creating and managing a header of buttons that correspond
to columns in an associated listbox (or similar widget).

It integrates with GuiFrame for layout and uses an internal grid (defined by letters) to place buttons.

Classes
-------
ButtonHeader
    An array-like container for GuiButtons used as column headers.
"""

from collections.abc import Iterable, Callable
from typing import Tuple, Dict, Any, Optional, List
from multipledispatch import dispatch

from gui.base.frame import GuiFrame
from gui.component._listbox.column import ListboxColumn
from gui.component.button import GuiButton
from gui.component.interface.button_header import IButtonHeader
from gui.component.interface.column import IListboxColumn
from gui.component._listbox.sort import Sort
from gui.util.constants import letters, empty_tuple
from gui.util.generic import Number

_ENTRY_SPACING: int = 1
"""Amount of spaces between two columns (used in width calculation)"""


class ButtonHeader(IButtonHeader):
    """
    Represents an array of GuiButtons to be used as headers for columns.

    Parameters
    ----------
    frame : GuiFrame
        The parent frame in which the header is placed.
    tag : str
        The tag identifier for the header.
    header_button_callback : Callable
        The callback to be invoked when any header button is clicked.
    columns : tuple of IListboxColumn
        A tuple of IListboxColumn instances representing the columns for which headers should be generated.
        Only visible columns (c.is_visible True) are considered.

    Attributes
    ----------
    _parent : GuiFrame
        The parent frame.
    _tag : str
        The header's tag.
    _button_panel_frame : GuiFrame
        The frame in which the header buttons are placed.
    _buttons : tuple of GuiButton
        A tuple of generated header buttons.
    _n_entry_chars : int
        The estimated number of characters across the entry above which these headers are placed.
    width_scaling : Number
        Scaling factor used in computing button widths.
    _on_click : Callable
        The callback invoked when a header button is clicked.
    _kws : tuple of dict
        A tuple of keyword argument dictionaries for generating each button.
    column_indices : dict
        A mapping from column identifier to its index in the header.
    header : str
        A string representing the combined header text.
    button_ids : dict
        A mapping from an internal button identifier (derived from widget info) to the corresponding column.
    """

    __slots__ = ("_parent", "_tag", "_button_panel_frame", "_buttons", "_n_entry_chars",
                 "width_scaling", "_on_click", "_kws", "column_indices", "header", "button_ids")

    _parent: GuiFrame
    tag: str
    _button_panel_frame: GuiFrame
    _buttons: Tuple[GuiButton, ...]
    _n_entry_chars: Optional[int]
    width_scaling: Number
    _on_click: Callable
    _kws: Iterable[Dict[str, Any]]

    def __init__(self, frame: GuiFrame, tag: str, header_button_callback: Callable, columns: Tuple[ListboxColumn, ...]):
        """
        Initialize a ButtonHeader.

        Filters visible columns, sets up internal state, and generates header buttons.

        Parameters
        ----------
        frame : GuiFrame
            The parent frame.
        tag : str
            The header tag.
        header_button_callback : Callable
            The callback to be invoked when a header button is clicked.
        columns : tuple of IListboxColumn
            The columns for which header buttons should be created.
        """
        # Filter columns to only those visible.
        columns = [c for c in columns if c.is_visible]
        if not all(isinstance(c, IListboxColumn) for c in columns):
            raise TypeError("Error initializing button header; columns should be a tuple of properly initialized "
                            "IListboxColumn instances.")
        self._parent = frame
        self._tag = tag
        self._on_click = header_button_callback
        self.width_scaling = 1.15  # scaling factor for width computation
        # _n_entry_chars will be computed later.
        self._n_entry_chars = None
        self.header = " "
        self.button_ids: Dict[str, Any] = {}
        self.column_indices: Dict[Any, int] = {}

        # Initialize _kws with existing settings if any.
        self._kws = empty_tuple

        # If columns exist, add them and then generate buttons.
        if len(columns) > 0:
            self.add(*columns)
        self.generate_buttons()

    def add(self, *columns: IListboxColumn) -> None:
        """
        Add new columns to the header.

        Updates internal keyword argument dictionaries (_kws) for generating buttons
        and recomputes the estimated number of entry characters.

        Parameters
        ----------
        *columns : IListboxColumn
            One or more columns to add.
        """
        columns = [c for c in columns if c.is_visible]
        self.n_entry_chars(*columns)
        entry_width = self._n_entry_chars  # entry width is set to estimated number of chars
        kws = list(self._kws) if self._kws != empty_tuple else []
        for idx, c in enumerate(columns):
            idx += len(self._buttons)
            # Calculate button width scaled to the entry width.
            w = int(c.width / self._n_entry_chars * entry_width * self.width_scaling)
            # Append the column header to the header string.
            self.header += f"{c.header: ^{c.width}} "
            kws.append({
                "command": self.on_click,
                "column": c,
                "command_kwargs": {"sort": Sort(c.column)},
                "button_text": c.header,
                "width": w,
                "sticky": "WE",
                "tag": letters[idx]
            })
            self.column_indices[c.column] = columns.index(c)
        self._kws = tuple(kws)

    def on_click(self, **kwargs) -> None:
        """
        Invoke the header button callback with provided keyword arguments.

        Parameters
        ----------
        **kwargs : dict
            Keyword arguments to pass to the callback.
        """
        self._on_click(**kwargs)

    def generate_buttons(self, *columns: IListboxColumn, button_command: Optional[Callable] = None) -> None:
        """
        Generate and place header buttons based on the current _kws.

        If any buttons already exist, they are destroyed before generating new ones.
        Optionally, a new button command can be provided.

        Parameters
        ----------
        *columns : IListboxColumn, optional
            Additional columns to add (will call add()).
        button_command : Callable, optional
            A new callback to set as _on_click.
        """
        # Destroy any existing buttons.
        for b in self._buttons:
            b.destroy()

        if button_command is not None:
            self._on_click = button_command

        if len(columns) > 0:
            self.add(*columns)

        num_buttons = len(tuple(self._kws))
        self._button_panel_frame = GuiFrame(self._parent.frame, [letters[:num_buttons]])
        buttons: List[GuiButton] = []
        for kw in self._kws:
            # If a new button command is provided, update the keyword dict.
            if button_command is not None:
                kw["command"] = self._on_click
            b = GuiButton(self._button_panel_frame, **kw)
            column = kw["column"]
            # Extract an identifier from the widget info.
            key = str(b.info).split("!")[-1].replace(">", "")
            self.button_ids[key] = column.column
            buttons.append(b)
        self._buttons = tuple(buttons)
        self._button_panel_frame.grid(**self.grid_kwargs)

    def n_entry_chars(self, *columns: IListboxColumn) -> int:
        """
        Estimate and update the number of characters present in the associated entry.

        This value is used to calculate the width of header buttons.

        Parameters
        ----------
        *columns : IListboxColumn, optional
            Additional columns to consider in the character count.

        Returns
        -------
        int
            The estimated number of characters.
        """
        # Compute from current _kws.
        if self._kws != empty_tuple:
            self._n_entry_chars = sum([kw["column"].width + _ENTRY_SPACING for kw in self._kws]) - _ENTRY_SPACING
        else:
            self._n_entry_chars = 0
        if columns:
            self._n_entry_chars += sum([c.width + _ENTRY_SPACING for c in columns])
        return self._n_entry_chars

    def get_button(self, attribute_name: str, attribute_value: Any) -> Optional[Tuple[GuiButton, Dict[str, Any]]]:
        """
        Retrieve a header button matching a given attribute and value.

        Parameters
        ----------
        attribute_name : str
            The name of the attribute to match.
        attribute_value : any
            The expected value of the attribute.

        Returns
        -------
        tuple of (GuiButton, dict) or None
            A tuple containing the matching button and its keyword argument dictionary,
            or None if no match is found.

        Raises
        ------
        RuntimeError
            If no buttons have been generated.
        """
        if len(self._buttons) == 0:
            raise RuntimeError("Unable to get a specific button if no buttons have been generated so far")

        for b, kw in zip(self._buttons, self._kws):
            try:
                column = kw["column"]
                if getattr(column, attribute_name) == attribute_value:
                    return b, kw
            except AttributeError:
                pass

            try:
                if kw[attribute_name] == attribute_value:
                    return b, kw
            except KeyError:
                pass
        return None

    def destroy(self) -> None:
        """
        Destroy all header buttons and the button panel frame.
        """
        for b in self._buttons:
            b.destroy()
        if self._button_panel_frame is not None:
            self._button_panel_frame.destroy()

    @property
    def grid_kwargs(self) -> Dict[str, Any]:
        """
        Retrieve grid keyword arguments for placing the header's button panel frame.

        Returns
        -------
        dict
            A dictionary of grid parameters obtained from the parent frame.
        """
        return self._parent.get_grid_kwargs(self._tag)

    def __iter__(self):
        """
        Return an iterator over the header buttons.

        Returns
        -------
        iterator
            An iterator over the tuple of GuiButtons.
        """
        return iter(self._buttons)

    @dispatch(int)
    def __getitem__(self, idx: int) -> GuiButton:
        """
        Retrieve a header button by its index.

        Parameters
        ----------
        idx : int
            The index of the desired button.

        Returns
        -------
        GuiButton
            The header button at the specified index.
        """
        return self._buttons[idx]
