"""
This module contains a baseclass used as template for gui objects.

GuiWidgets are placed on a GuiFrame,

"""
import tkinter as tk
import tkinter.ttk as ttk
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import List, Tuple

import util.gui as ug
from gui.model.grid import TkGrid
from util.data_structures import remove_dict_entries


class GuiWidget:
    """
    Wrapper class for GUI widgets that defines basic widget behaviour
    Each GUI widget has a master frame, 0-n event bindings, a set of tk vars and a font that is
    applied.
    
    Notes
    -----
    This class should be inherited alongside another tk widget
    """
    frame: tk.Frame or ttk.Frame
    grid_tag: str
    tk_grid: TkGrid
    event_bindings: List[Tuple[str, Callable]] = []
    padx: int = 0
    pady: int = 0
    sticky: str = 'N'
    tk_vars: dict = {}
    font: tuple = ()
        
    def apply_grid(self, grid: TkGrid = None, grid_tag: str = None, padx: int = None, pady: int = None, sticky: str = None, **kwargs):
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
        
        self.verify_attribute('tk_grid', attribute_value=grid, failed_method='apply_grid')
        self.verify_attribute('grid_tag', attribute_value=grid_tag, failed_method='apply_grid')
        if padx is not None:
            self.padx = padx
        if pady is not None:
            self.pady = pady
        if sticky is not None:
            self.sticky = sticky
        
        self._grid(**self.tk_grid.get_dims(self.grid_tag, pady=self.pady, padx=self.padx, sticky=self.sticky))
    
    def _grid(self, **kwargs):
        """ Calls the grid method of the tkinter object to place it. This  """
        try:
            self.grid(**kwargs)
        except AttributeError:
            raise AttributeError("GuiWidget does not appear to have a grid method. This method should be inherited via"
                                 " an existing tkinter widget class like ttk.Button.")
    
    def _set_padding(self, **kwargs):
        """ Set x- and y-padding for this Widget, either via padxy as a tuple, or padx and pady separately """
        keys = frozenset(('padxy', 'padx', 'pady')).intersection(kwargs)
        if len(keys) > 0:
            for next_pad in frozenset(('padxy', 'padx', 'pady')).intersection(kwargs):
                value = kwargs.get(next_pad)
                if value is not None:
                    if next_pad == 'padxy':
                        self.padx, self.pady = value
                        break
                    else:
                        self.__setattr__(next_pad, value)
            kwargs = remove_dict_entries(_dict=kwargs, keys=keys)
        return kwargs
        
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
    def verify_attribute(self, attribute_name: str, attribute_value: any = None, failed_method: str = None):
        """ Set an attribute value or verify if an attribute is already set. If not, raise a descriptive error """
        if attribute_value is not None:
            self.__setattr__(attribute_name, attribute_value)
            return
        try:
            if self.__getattribute__(attribute_name) is None:
                raise AttributeError()
        except AttributeError:
            s = f"Verification for attribute {attribute_name} failed."
            if failed_method is not None:
                s += f" Make sure it is already set while/before calling {failed_method}()"
            raise AttributeError(s)


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
    
    
@dataclass
class A:
    a: int = 1
    b: int = 2
    c: int = 4

if __name__ == '__main__':
    aaa = A(2, 3, 4)
    
    aaa.__dict__.update({'a': 2, 'd': 5})
    print(aaa.__dict__)
