import tkinter as tk
from abc import ABCMeta
from collections.abc import Sized
from tkinter import ttk as ttk
from typing import List, Tuple, Callable, Iterable

from gui.base.frame import GuiFrame
from gui.component.event_bindings import EventBinding
from util import gui as ug
from util.data_structures import remove_dict_entries


class GuiWidget(ttk.Widget):
    """
    Wrapper class for GUI widgets that defines basic widget behaviour
    Each GUI widget has a master frame, 0-n event bindings, a set of tk vars and a font that is
    applied.
    
    Notes
    -----
    This class should be inherited alongside another tk widget
    """
    __slots__ = "frame", "tag", "_text"

    frame: GuiFrame
    tag: str
    event_bindings: List[Tuple[str, Callable]] = []
    padx: int = 0
    pady: int = 0
    sticky: str = 'N'
    _text: tk.StringVar
    _tkinter_variable_dict: dict = {}
    font: tuple = ()
    
    def apply_grid(self, **kwargs):
        """ Calls the grid method of the tkinter object to place it. kwargs will be passed along to the grid() call """
        try:
            # print('grid', kwargs)
            super().grid(**self.frame.get_grid_kwargs(self.tag, **kwargs))
        except ZeroDivisionError:
            raise AttributeError("GuiWidget does not appear to have a grid method. This method should be inherited via"
                                 " an existing tkinter widget class like ttk.Button.")
        
    def init_widget_start(self, frame: GuiFrame, tag: str, text: str = None, text_variable: tk.StringVar = None, **kwargs):
        """ Method that is to be called before initializing the superclass in the subclass __init__() """
        self.frame = frame
        self.tag = tag
        
        self._text = tk.StringVar() if text_variable is None else text_variable
        if text is not None:
            self._text.set(text)
            
        kwargs = self._set_padding(**kwargs)
    
    def init_widget_end(self, event_bindings: Iterable[Tuple[str, Callable]] and Sized = (), apply_grid: bool = True,
                        **kwargs):
        """ Method that is to be called after initializing the superclass in the subclass __init__() """
        self._set_bindings(event_bindings)
        if apply_grid:
            # print(kwargs)
            self.apply_grid(**kwargs)
    
    @property
    def text(self) -> str:
        """ Main text variable of the Widget. Its specific usage depends on the specific widget subclass. """
        return self._text.get()
    
    @text.setter
    def text(self, text: str):
        self._text.set(text)
        
    def set_text_variable(self, string_var: tk.StringVar):
        """ Set the current text tk.StringVar to `string_var` """
        self._text = string_var
    
    def _set_padding(self, **kwargs):
        """ Set x- and y-padding for this Widget, either via padxy as a tuple, or padx and pady separately """
        keys = frozenset(('padxy', 'padx', 'pady')).intersection(tuple(kwargs.keys()))
        # print(kwargs, keys)
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
        self._tkinter_variable_dict[key] = var
        
    # def set_event_bindings(self, bindings: List[tuple] = None, replace_bindings: bool = True):
    def set_event_bindings(self, event_bindings: List[Tuple[str, Callable] or EventBinding] = None,
                           replace_bindings: bool = True):
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
            
        for next_binding in self.event_bindings:
            try:
                if isinstance(next_binding, EventBinding):
                    self.bind(next_binding.tag.value(), next_binding.callback)
            except AttributeError:
                self.bind(next_binding[0], next_binding[1])
    
    def set_tk_var(self, name: str, value: (int, float, bool, str), set_new_var: bool = True):
        """
        Set the tkinter variable listen in the tkinter variable dict with key `name` to `value`. If there is no variable
        listed under `name`, register a new one.
        
        Parameters
        ----------
        name : str
            The key the tkinter var should be set to
        value : int or float or bool or str
            The value to set the tkinter var to
        set_new_var : bool, optional, True by default
            If True, allow a new variable to be registered.

        Returns
        -------

        """
        try:
            tk_var = self._tkinter_variable_dict[name]
        except KeyError:
            if not set_new_var:
                raise AttributeError(f"No tkinter variable was registered under `{name}`. If you wish to register this "
                                     f"as a new variable, pass `set_new_var=True` as well")
            else:
                tk_var = ug.tk_var(value=value, name=name)
        else:
            tk_var.set(value)
        
        self._tkinter_variable_dict[name] = tk_var

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


