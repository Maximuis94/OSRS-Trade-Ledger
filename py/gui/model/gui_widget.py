"""
This module contains a baseclass used as template for gui objects.

GuiWidgets are placed on a GuiFrame,

"""
import tkinter as tk
import tkinter.ttk as ttk
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import List, Tuple

import util.gui as ug
from gui.model.grid import TkGrid


class GuiWidget(ABC):
    """
    Wrapper class for GUI widgets that defines basic widget behaviour
    Each GUI widget has a master frame, 0-n event bindings, a set of tk vars and a font that is
    applied.
    
    Notes
    -----
    This class should be inherited alongside another tk widget
    """
    
    def __init__(self, frame: (tk.Frame, ttk.Frame), grid_tag: str, grid: TkGrid = None,
                 event_bindings: List[Tuple[str, Callable]] = (), padx: int = 0, pady: int = 0, sticky: str = 'N', **kwargs):
        # super().__init__(master=frame, widgetName='Button', **kwargs)
        self.frame = frame
        self.grid_tag = grid_tag
        self.tk_grid = grid
        self.tk_vars = {}
        self.font = ()
        self.event_bindings: List[Tuple[str, Callable]] = event_bindings
        if len(self.event_bindings) > 0:
            self.set_event_bindings()
        self._set_bindings()
        if kwargs.get('padxy') is not None:
            self.padx, self.pady = kwargs.get('padxy')
        else:
            self.padx = padx
            self.pady = pady
        self.sticky = sticky
        self.apply_grid()
        
    def apply_grid(self, grid: TkGrid = None, padx: int = None, pady: int = None, sticky: str = None):
        """
        Apply grid configurations from `grid` or the previously configured grid. If optional parameters are omitted,
        the currently active configuration will be used instead
        
        Parameters
        ----------
        grid : TkGrid, optional, default is None
            If passed, override the currently active grid with `grid`.
        padx : int, optional, None by default
            Horizontal padding that is to be applied
        pady : int, optional, None by default
            Vertical padding that is to be applied
        sticky : str, optional, None by default
            The edge to which the parameter will be positioned against.
        """
        if grid is not None:
            self.tk_grid = grid
        if padx is not None:
            self.padx = padx
        if pady is not None:
            self.pady = pady
        if sticky is not None:
            self.sticky = sticky
        
        self._grid(**self.tk_grid.get_dims(self.grid_tag, pady=self.pady, padx=self.padx, sticky=self.sticky))
    
    def _grid(self, **kwargs):
        self.grid(**kwargs)
    
    def _set_bindings(self, event_bindings: List[Tuple[str, Callable]] = None):
        if event_bindings is not None:
            self.event_bindings += event_bindings
        
        for trigger, command in self.event_bindings:
            self.bind(trigger, command)
    
    def add_tk_var(self, var: tk.Variable, key: str, value: any = None):
        if value is not None:
            var.set(value)
        self.tk_vars[key] = var
        
    # def set_event_bindings(self, bindings: List[tuple] = None, replace_bindings: bool = True):
    def set_event_bindings(self, event_bindings: List[Tuple[str, Callable]] = None, replace_bindings: bool = True):
        """
        Iterate over the configured event bindings list and bind them. If additional bindings are supplied, append them
        to the existing list or replace the existing bindings with them.
        
        Parameters
        ----------
        event_bindings : List[tuple], optional, None by default
            An Iterable with tuples that specify an event listener and the command to execute if the event triggers.
        replace_bindings : bool, optional, True by default
            Flag that dictates whether `bindings` will replace self.event_bindings, or whether it will be added to it.

        Returns
        -------

        """
        if event_bindings is not None:
            self.event_bindings += event_bindings
        
        for trigger, command in self.event_bindings:
            self.bind(trigger, command)
        if isinstance(event_bindings, list) and len(event_bindings[0]) == 2:
            self.event_bindings = event_bindings if replace_bindings else self.event_bindings + event_bindings
            
        for event, command in self.event_bindings:
            self.bind(event, command)
    
    def set_value(self, value: (int, float, bool, str), var_key: str):
        """ Set `value` to the tk variable from the var dict specified by key `var_key` """
        # This works fine if there is a very limited amount of tk vars in this object. If not, specify a key!!!!
        tk_var = self.tk_vars.get(var_key)
        if tk_var is None:
            tk_var = ug.tk_var(value=value, name=var_key)
        else:
            tk_var.set(value)
        self.tk_vars[var_key] = tk_var

    # def grid(self, param):
    #     pass


class InputWidget(GuiWidget):
    """
    Abstract class for GUI input widgets, i.e. widgets used for prompting user input.
    An input widget has 3 custom methods to implement;
    reset_input_field() for resetting the value in the widget to its default state
    parse_input() for extracting and formatting the input
    verify_input() to check if the input is properly formatted.
    
    """
    
    def __init__(self, default_value: str = '', **kwargs):
        super().__init__(**kwargs)
        self.default_value, self.input_key = default_value, 'input_value'
        self.reset_input_field()
    
    def reset_input_field(self):
        """ Reset the input field to its default state. """
        self.set_value(value=self.default_value, var_key=self.input_key)
    
    @abstractmethod
    def parse_input(self):
        """ Parse the input from the input field and return it """
        ...
    
    @abstractmethod
    def verify_input(self) -> bool:
        """ Evaluate whether the input from the input field meets expected requirements """
        ...
    
    
