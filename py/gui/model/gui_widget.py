"""
This module contains a baseclass used as template for gui objects.

GuiWidgets are placed on a GuiFrame,

"""
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import List, Tuple

import tkinter as tk
import tkinter.ttk as ttk

import util.gui as ug
from gui.model.grid import Grid, WidgetDimensions, TkGrid


class GuiWidget(ABC):
    """
    Wrapper class for GUI widgets that defines basic widget behaviour
    Each GUI widget has a master frame, 0-n event bindings, a set of tk vars and a font that is
    applied.
    
    """
    def __init__(self, frame: (tk.Frame, ttk.Frame), grid_tag: str, grid: TkGrid = None,
                 event_bindings: Iterable[tuple] = (), padx: int = 0, pady: int = 0, sticky: str = 'N', **kwargs):
        # super().__init__(master=frame, widgetName='Button', **kwargs)
        self.frame = frame
        self.grid_tag = grid_tag
        self.tk_grid = grid
        self.event_bindings = [] if kwargs.get('event_bindings') is None else kwargs.get('event_bindings')
        self.tk_vars = {}
        self.font = ()
        self.event_bindings = event_bindings
        self._set_bindings()
        self.padx = padx
        self.pady = pady
        self.sticky = sticky
        
    def apply_grid(self, grid: TkGrid = None, padx: int = 0, pady: int = 0, sticky: str = 'N'):
        """
        Apply grid configurations from `grid` or the previously configured grid, if `grid` is None, along with the other
        parameters passed.
        
        Parameters
        ----------
        grid : TkGrid, optional, default is None
            If passed, override the currently active grid with `grid`.
        padx : int, optional, 0 by default
            Horizontal padding that is to be applied
        pady : int, optional, 0 by default
            Vertical padding that is to be applied
        sticky : str, optional, 'N' by default
            The edge to which the parameter will be positioned against.
        """
        if grid is not None:
            self.tk_grid = grid
        
        self._grid(**self.tk_grid.get_dims(self.grid_tag, pady=self.pady, padx=self.padx, sticky=self.sticky))
    
    def _grid(self, **kwargs):
        self.grid(**kwargs)
    
    def _set_bindings(self):

        for trigger, command in self.event_bindings:
            # print(binding)
            self.bind(trigger, command)
    
    def add_tk_var(self, var: tk.Variable, key: str, value: any = None):
        if value is not None:
            var.set(value)
        self.tk_vars[key] = var
    
    def set_value(self, var_key: str, value: any):
        tk_var = self.tk_vars.get(var_key)
        if tk_var is None:
            raise KeyError(f"Unable to set value to non-existent Variable (key={var_key})")
        else:
            tk_var.set(value)
            self.tk_vars[var_key] = tk_var
        
    # def grid_args(self):
    #     return {
    #         'row': self.y,
    #         'rowspan': self.h,
    #         'column': self.x,
    #         'columnspan': self.w,
    #         'padx': self.padx,
    #         'pady': self.pady,
    #         'sticky': self.sticky
    #     }
        
    def set_event_bindings(self, bindings: List[tuple] = None, replace_bindings: bool = True):
        """
        Iterate over the configured event bindings list and bind them. If additional bindings are supplied, append them
        to the existing list or replace the existing bindings with them.
        
        Parameters
        ----------
        bindings : List[tuple], optional, None by default
            An Iterable with tuples that specify an event listener and the command to execute if the event triggers.
        replace_bindings : bool, optional, True by default
            Flag that dictates whether `bindings` will replace self.event_bindings, or whether it will be added to it.

        Returns
        -------

        """
        if isinstance(bindings, list) and len(bindings[0]) == 2:
            self.event_bindings = bindings if replace_bindings else self.event_bindings + bindings
            
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


class InputWidget(ABC):
    """
    Abstract class for GUI input widgets, i.e. widgets used for prompting user input.
    An input widget has 3 custom methods to implement;
    reset_input_field() for resetting the value in the widget to its default state
    parse_input() for extracting and formatting the input
    verify_input() to check if the input is properly formatted.
    
    """
    @abstractmethod
    def __init__(self):
        ...
    
    @abstractmethod
    def reset_input_field(self):
        """ Reset the input field to its default state. """
        ...
    
    @abstractmethod
    def parse_input(self):
        """ Parse the input from the input field and return it """
        ...
    
    @abstractmethod
    def verify_input(self) -> bool:
        """ Evaluate whether the input from the input field meets expected requirements """
        ...
    
    
