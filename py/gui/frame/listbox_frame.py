"""
This module contains the implementation of the GuiListbox

"""
import tkinter as tk
import tkinter.ttk as ttk
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Sequence
from typing import List, Tuple, Optional, final

from multipledispatch import dispatch
from typing_extensions import NamedTuple

from gui.base.frame import GuiFrame
from gui.component.interface.column import IListboxColumn
from gui.component.interface.filter import IFilter
from gui.component.interface.row import IRow

from gui.component.button import GuiButton
from gui.base.frame import TkGrid
from gui.component.filter.predefined_filters import Filters, NUM_FILTERS
from gui.component.label import GuiLabel
from gui.component._listbox._column import ListboxColumn
from gui.component._listbox.entry_manager import ListboxEntries
from gui.component.listbox import GuiListbox
from gui.util.colors import Color, Rgba
from gui.util.constants import letters, empty_tuple
from gui.component.filter.filter import Filter
from gui.component._listbox.row import ListboxRow
from gui.component.sort.sort import Sort, Sorts
from gui.util.font import Font
from gui.util.generic import SupportsGetItem

_GRID_LAYOUT = "A" * 10, "B" * 7 + "C" * 3
"""Grid layout to apply to a GuiListboxFrame"""


class ListboxArgs(NamedTuple):
    """Container class with Listbox Configurations. Can be pre-defined and passed to a Listbox."""
    
    listbox_columns: Sequence[IListboxColumn]
    """Columns the Listbox will have"""
    
    entries: Iterable[IRow] = empty_tuple
    """Rows to fill the listbox with. Can also be added later."""
    
    row_click: Optional[Callable[IRow]] = None
    """Method to invoke when clicking a row"""
    
    row_color_scheme: Optional[Callable[[IRow], Rgba]] = None
    """Method to apply for coloring rows. It should accept an IRow instance and return an Rgba instance"""
    
    row_font: Font = Font()
    """Font to use for the rows"""
    
    default_sort: Sorts = None
    """The sort to apply by default. This sort is applied to the list of rows if no additional sorts are active."""
    
    default_filter: IFilter = None
    """The filter to apply by default. This filter is always active."""
    
    width: int = 20
    """The width of the listbox in characters."""
    
    height: int = 10
    """The height of the listbox in rows."""


class GuiListboxFrame(GuiFrame, ABC):
    """
    An extendable, abstract GuiListboxFrame class that is used to generate a pair of listboxes and a panel to modify
    its parameters.
    
    This class is to be overridden, which the listbox and control panel setups to be filled in by the subclass.
    The general behaviour is expected to be more or less the same.
    """
    __slots__ = "primary_listbox", "secondary_listbox", "settings_panel"
    primary_listbox: GuiListbox
    secondary_listbox: GuiListbox
    configuration_panel: GuiFrame
    
    quick_sorts: Sequence[Sorts]
    quick_filters: IFilter
    color_schemes: Sequence[Rgba]
    
    def __init__(self, frame: GuiFrame, tag: str, **kwargs):
        super().__init__(frame, grid_layout=kwargs.get('grid_layout', _GRID_LAYOUT), tag=tag,
                         relief=kwargs.get('relief', tk.SUNKEN))
    
    @final
    def __post_init__(self):
        """Executed after __init__()"""
        ...
    
    @abstractmethod
    def setup_primary_listbox(self, **kwargs) -> GuiListbox:
        """Initializes the primary listbox"""
        raise NotImplementedError
    
    def primary_listbox_row_clicked(self, e, *args, **kwargs):
        """Callback from the primary Listbox if a row is clicked. Override this method to implement behaviour."""
        ...
    
    @abstractmethod
    def setup_secondary_listbox(self, **kwargs) -> GuiListbox:
        """Initializes the secondary listbox"""
        raise NotImplementedError
    
    def secondary_listbox_row_clicked(self, e, *args, **kwargs):
        """Callback from the secondary Listbox if a row is clicked. Override this method to implement behaviour."""
        ...
    
    def setup_config_panel(self):
        """
        Initializes the configuration panel. By default, it can be used to alter sorts, color schemes and filters
        currently applied to the listboxes.
        Alternatively, this method can be overridden
        
        Returns
        -------

        """
        # TODO
        ...
        
    @abstractmethod
    def implement_configurations(self):
        """Parse the fields from the configurations panel and implement them"""
        ...
    