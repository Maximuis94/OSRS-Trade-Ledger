"""
Module: gui_base_frame.py
===========================
This module provides the fundamental classes for managing GUI frames and layouts.

Classes
-------
TkGrid : Grid layout manager that parses a sequence of strings into widget placement dimensions.
LayoutManager : Dedicated manager that wraps TkGrid and exposes a simplified API for grid operations.
GuiFrame : Wrapper around ttk.Frame that integrates layout management, event binding, and Tk variable handling.
"""
from collections.abc import Callable

import tkinter as tk
import tkinter.ttk as ttk
from collections import namedtuple

from multipledispatch import dispatch
from typing import Sequence, Any, Dict, Tuple, List

from gui.util.event_binding import EventBinding

# Define a namedtuple for widget dimensions.
WidgetDimensions = namedtuple('WidgetDimensions', ['column', 'row', 'columnspan', 'rowspan'])

# Define any illegal characters for tag processing.
ILLEGAL_CHARACTERS = ""


class TkGrid:
    """
    Grid layout manager that parses a sequence of strings into widget placement dimensions.

    Parameters
    ----------
    grid : sequence of str
        A sequence of strings where each string represents one row of the layout.
        Each character corresponds to a widget's tag.

    Attributes
    ----------
    grid : sequence of str
        The original grid layout.
    grid_1d : str
        The flattened grid (all rows concatenated).
    widgets : dict
        Mapping from widget tag (str) to its dimensions (WidgetDimensions).
    width : int
        The number of columns in the grid (inferred from the first row).
    height : int
        The number of rows in the grid.
    unique_tags : str
        A string of unique tags found in the grid, ordered left-to-right, top-to-bottom.

    Raises
    ------
    ValueError
        If a tag is not found or rows are inconsistent in length.

    Examples
    --------
    >>> grid_layout = ["AAABBB", "AAABBB", "CCCDDD"]
    >>> tg = TkGrid(grid_layout)
    >>> tg.widgets['A']
    WidgetDimensions(column=0, row=0, columnspan=3, rowspan=2)
    """
    __slots__ = ('grid', 'grid_1d', 'widgets', 'width', 'height', '_row_length', '_unique_tags')

    def __init__(self, grid: Sequence[str]):
        self.grid = grid
        self._unique_tags = ""
        self.flatten_grid()
        self.widgets = {}
        self.width = len(grid[0]) if grid else 0
        self.height = len(grid)
        for tag in self.unique_tags:
            self.parse_grid(tag)

    @property
    def unique_tags(self) -> str:
        """
        Unique widget tags in the grid.

        Returns
        -------
        str
            A string of unique tags (ordered left-to-right, top-to-bottom).
        """
        return self._unique_tags

    @unique_tags.setter
    def unique_tags(self, grid_str: str):
        """
        Set the unique tags after stripping illegal characters.

        Parameters
        ----------
        grid_str : str
            The flattened grid string.

        Raises
        ------
        TypeError
            If grid_str is not a string.
        """
        if not isinstance(grid_str, str):
            raise TypeError(f"unique_tags must be a string, got {type(grid_str)}")
        self._unique_tags = self._strip_illegal_tags(grid_str)

    def flatten_grid(self) -> None:
        """
        Flatten the 2D grid into a single 1D string while preserving order.
        """
        self.grid_1d = "".join(self.grid)
        self._row_length = len(self.grid_1d) // len(self.grid) if self.grid else 0
        self.unique_tags = self.grid_1d

    def _strip_illegal_tags(self, grid_str: str) -> str:
        """
        Remove illegal characters from the grid string.

        Parameters
        ----------
        grid_str : str
            The grid string to process.

        Returns
        -------
        str
            The processed string with illegal characters removed.
        """
        seen = set()
        return ''.join(ch for ch in grid_str if ch not in ILLEGAL_CHARACTERS and ch not in seen and not seen.add(ch))

    def parse_grid(self, tag: str) -> None:
        """
        Parse the grid to determine the dimensions of a widget with the given tag.

        Parameters
        ----------
        tag : str
            The widget tag to locate in the grid.

        Raises
        ------
        ValueError
            If the tag is not found in the grid.

        Notes
        -----
        The method calculates the starting row and column, as well as the column span and row span,
        and stores the dimensions as a WidgetDimensions tuple in the widgets attribute.
        """
        start_row, start_col, colspan, rowspan = None, None, 0, 0
        for row_index, row in enumerate(self.grid):
            if tag in row:
                if start_row is None:
                    start_row = row_index
                    start_col = row.index(tag)
                    colspan = row.count(tag)
                    rowspan = 1
                else:
                    rowspan += 1
            elif start_row is not None:
                break
        if start_row is None:
            raise ValueError(f"Tag '{tag}' not found in grid.")
        self.widgets[tag] = WidgetDimensions(column=start_col, row=start_row,
                                               columnspan=colspan, rowspan=rowspan)

    def get_dims(self, tag: str, **kwargs) -> Dict[str, Any]:
        """
        Get grid keyword arguments for a widget with the given tag.

        Parameters
        ----------
        tag : str
            The widget tag.
        **kwargs : dict
            Additional grid options (e.g., padx, pady, sticky).

        Returns
        -------
        dict
            A dictionary of grid parameters suitable for the grid() method.

        Raises
        ------
        ValueError
            If the tag is not found in the grid.
        """
        if tag not in self.widgets:
            valid = ", ".join(self.widgets.keys())
            raise ValueError(f"Tag '{tag}' not found. Valid tags: {valid}")
        dims = self.widgets[tag]._asdict()
        dims.update(kwargs)
        return dims

    def has_tag(self, tag: str) -> bool:
        """
        Check if a given tag exists in the grid.

        Parameters
        ----------
        tag : str
            The widget tag to check.

        Returns
        -------
        bool
            True if the tag exists, False otherwise.
        """
        return tag in self.widgets

    @staticmethod
    def generic_layout(n_widgets: int, alignment: str) -> Tuple[str, ...]:
        """
        Generate a generic layout for a given number of widgets.

        Parameters
        ----------
        n_widgets : int
            The number of widgets.
        alignment : str
            The layout orientation; must be 'vertical' or 'horizontal'.

        Returns
        -------
        tuple of str
            A tuple representing the grid layout.

        Raises
        ------
        ValueError
            If an invalid alignment is provided.

        Examples
        --------
        >>> TkGrid.generic_layout(3, 'horizontal')
        ('abc',)
        >>> TkGrid.generic_layout(3, 'vertical')
        ('a', 'b', 'c')
        """
        from gui.util.constants import letters  # Assumes letters is defined externally.
        if alignment.lower() == 'vertical':
            return tuple(letters[:n_widgets])
        elif alignment.lower() == 'horizontal':
            return (letters[:n_widgets],)
        else:
            raise ValueError("Invalid alignment. Use 'vertical' or 'horizontal'.")


class LayoutManager:
    """
    Dedicated layout manager that encapsulates grid operations.

    Parameters
    ----------
    grid : TkGrid
        An instance of TkGrid defining the layout.

    Attributes
    ----------
    unique_tags : str
        Unique tags from the underlying grid.
    width : int
        The grid's width (number of columns).
    height : int
        The grid's height (number of rows).
    """
    __slots__ = ("_grid",)

    def __init__(self, grid: TkGrid):
        self._grid = grid

    @property
    def unique_tags(self) -> str:
        """
        Unique tags from the underlying grid.

        Returns
        -------
        str
            A string of unique widget tags.
        """
        return self._grid.unique_tags

    @property
    def width(self) -> int:
        """
        Grid width (number of columns).

        Returns
        -------
        int
            The width of the grid.
        """
        return self._grid.width

    @property
    def height(self) -> int:
        """
        Grid height (number of rows).

        Returns
        -------
        int
            The height of the grid.
        """
        return self._grid.height

    def get_grid_kwargs(self, tag: str, **kwargs) -> Dict[str, Any]:
        """
        Get grid keyword arguments for placing a widget with a given tag.

        Parameters
        ----------
        tag : str
            The widget tag.
        **kwargs : dict
            Additional grid options.

        Returns
        -------
        dict
            A dictionary of grid parameters.
        """
        return self._grid.get_dims(tag, **kwargs)


class GuiFrame:
    """
    Wrapper around ttk.Frame that integrates layout management and event bindings.

    Parameters
    ----------
    parent : tk.Widget
        The parent widget.
    grid_layout : sequence of str or TkGrid
        The layout definition as a sequence of strings or a TkGrid instance.
    **kwargs
        Additional options passed to ttk.Frame.

    Attributes
    ----------
    _frame : ttk.Frame
        The underlying ttk.Frame.
    _layout_manager : LayoutManager
        The layout manager used for grid operations.
    event_bindings : list of tuple
        A list of event binding pairs (event, callback).
    tag : str
        An optional tag identifier for the frame.
    """
    __slots__ = ("_frame", "_layout_manager", "_tk_vars", "event_bindings", "tag")

    def __init__(self, parent: tk.Widget, grid_layout: Sequence[str] or TkGrid, **kwargs):
        self.tag = kwargs.pop('tag', '')
        self._frame = ttk.Frame(parent._frame if hasattr(parent, "_frame") else parent, **kwargs)
        self.event_bindings: List[EventBinding] = []
        self._tk_vars = {}
        if isinstance(grid_layout, TkGrid):
            grid = grid_layout
        else:
            grid = TkGrid(grid_layout)
        self._layout_manager = LayoutManager(grid)

    @property
    def frame(self) -> ttk.Frame:
        """
        Public accessor for the underlying ttk.Frame.

        Returns
        -------
        ttk.Frame
            The underlying frame.
        """
        return self._frame

    @property
    def layout_manager(self) -> LayoutManager:
        """
        Public accessor for the layout manager.

        Returns
        -------
        LayoutManager
            The layout manager instance.
        """
        return self._layout_manager

    def init_widget_start(self, tag: str, text: str = None,
                          text_variable: tk.StringVar = None, **kwargs) -> tk.StringVar:
        """
        Prepare widget initialization by setting a tag and initializing a text variable.

        Parameters
        ----------
        tag : str
            The widget tag.
        text : str, optional
            The initial text value.
        text_variable : tk.StringVar, optional
            A pre-existing StringVar to use.
        **kwargs : dict
            Additional options (e.g., padding).

        Returns
        -------
        tk.StringVar
            The text variable to be used by the widget.
        """
        self.tag = tag
        text_var = tk.StringVar(self._frame, value=text) if text_variable is None else text_variable
        self._set_padding(**kwargs)
        return text_var

    def get_widget_dimensions(self, tag: str) -> Tuple[int, int]:
        """
        Compute the pixel dimensions of a widget based on the grid layout.

        Parameters
        ----------
        tag : str
            The widget tag.

        Returns
        -------
        tuple of int
            A tuple (width, height) representing the widget's dimensions in pixels.

        Raises
        ------
        ValueError
            If the tag is not found in the grid.
        """
        dims = self._layout_manager._grid.widgets.get(tag)
        if dims is None:
            raise ValueError(f"Tag '{tag}' not found in grid:\n" +
                             "\n".join(self._layout_manager._grid.grid))
        width = int((dims.columnspan / self.layout_manager.width) * self._frame.winfo_width())
        height = int((dims.rowspan / self.layout_manager.height) * self._frame.winfo_height())
        return width, height

    def get_grid_kwargs(self, widget_tag: str, **kwargs) -> Dict[str, Any]:
        """
        Retrieve grid keyword arguments for placing a widget based on its tag.

        Parameters
        ----------
        widget_tag : str
            The widget tag.
        **kwargs : dict
            Additional grid options (e.g., padding).

        Returns
        -------
        dict
            A dictionary of grid parameters.
        """
        # Process shorthand for padding.
        if 'pad' in kwargs:
            pad_val = kwargs.pop('pad')
            if isinstance(pad_val, (int, float)):
                kwargs['padx'] = kwargs['pady'] = pad_val
            elif isinstance(pad_val, (tuple, list)) and len(pad_val) >= 2:
                kwargs['padx'], kwargs['pady'] = pad_val[:2]
        if 'ipad' in kwargs:
            ipad_val = kwargs.pop('ipad')
            if isinstance(ipad_val, (int, float)):
                kwargs['ipadx'] = kwargs['ipady'] = ipad_val
            elif isinstance(ipad_val, (tuple, list)) and len(ipad_val) >= 2:
                kwargs['ipadx'], kwargs['ipady'] = ipad_val[:2]
        return self._layout_manager.get_grid_kwargs(widget_tag, **kwargs)

    def _set_padding(self, **kwargs) -> None:
        """
        Set padding options on the underlying frame.

        Parameters
        ----------
        **kwargs : dict
            Padding options such as 'padxy', 'padx', and 'pady'.
        """
        for key in ('padxy', 'padx', 'pady'):
            if key in kwargs:
                setattr(self._frame, key, kwargs[key])

    def set_event_bindings(self, event_bindings: Sequence[Tuple[str, Callable]] = None, replace: bool = True) -> None:
        """
        Set or replace event bindings on the underlying frame.

        Parameters
        ----------
        event_bindings : sequence of tuple, optional
            A sequence of (event, callback) pairs.
        replace : bool, optional
            If True, replace existing bindings; otherwise, add to them.
        """
        if event_bindings is not None:
            self.event_bindings = list(event_bindings) if replace else self.event_bindings + list(event_bindings)
        for trigger, callback in self.event_bindings:
            self._frame.bind(trigger, callback)

    def add_tk_var(self, var: tk.Variable, key: str, value: Any = None) -> None:
        """
        Register a Tkinter variable in the frame's internal dictionary.

        Parameters
        ----------
        var : tk.Variable
            The Tkinter variable to register.
        key : str
            The key under which to register the variable.
        value : any, optional
            An optional value to set for the variable.
        """
        if value is not None:
            var.set(value)
        self._tk_vars[key] = var
    
    def grid(self, *args, **kwargs):
        """Method that calls the grid() method of the Frame"""
        self._frame.grid(*args, **kwargs)
    
    # @dispatch(GuiFrame, str, any, str)
    def bind_event(self, event: str, callback: Callable, description: str = ""):
        """
        Bind an event to the underlying Tk widget and record the binding.

        Parameters
        ----------
        event : str
            The event sequence to bind (e.g., "<Button-1>", "<Enter>", etc.).
        callback : Callable
            The callback function to execute when the event occurs.
        description : str, optional, "" by default
            The description that will be added to the event binding
        """
        
        self._frame.bind(event, callback)
        self.event_bindings.append(EventBinding(event, callback, description))
    
    # @dispatch(EventBinding)
    def bind_event(self, event_binding: EventBinding):
        """
        Bind an event to the underlying Tk widget and record the binding.

        Parameters
        ----------
        event_binding : EventBinding
            EventBinding with all information needed
        """
        self._frame.bind(*event_binding.bind_args)
        self.event_bindings.append(event_binding)
    
    # @dispatch(str)
    def unbind_event(self, event: str):
        """
        Unbind all callbacks associated with the given event from the underlying Tk widget.

        Parameters
        ----------
        event : str
            The event sequence to unbind (e.g., "<Button-1>").
        """
        self._frame.unbind(event)
        self.event_bindings = [b for b in self.event_bindings if b[0] != event]
    
    # @dispatch(EventBinding)
    def unbind_event(self, event: EventBinding):
        """
        Unbind all callbacks associated with the given event from the underlying Tk widget.

        Parameters
        ----------
        event : str
            The event sequence to unbind (e.g., "<Button-1>").
        """
        self._frame.unbind(event.event)
        self.event_bindings.remove(event)
    
    def destroy(self):
        """Call to the destroy method of the underlying Frame"""
        self._frame.destroy()
