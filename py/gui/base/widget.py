"""
Module: gui_base_widget.py
===========================
Defines a mixin base class for GUI widgets that encapsulates common behavior such as
grid placement, text handling, event binding, and Tk variable management.

Classes
-------
GuiWidget
    Base mixin for GUI widgets that provides common functionality to be used with
    concrete Tkinter widget classes.

Notes
-----
This class is intended to be used as a mixin together with a concrete widget class
(e.g., ttk.Button, ttk.Label). It relies on a parent GuiFrame for grid placement and
other layout-related operations.
"""
import tkinter as tk
from multipledispatch import dispatch
from typing import Any, Dict, List, Tuple, Callable, Optional
from gui.base.frame import GuiFrame
from gui.util.event_binding import EventBinding
from gui.util.font import Font
from gui.util.str_formats import shorten_string
from gui.util.constants import grid_args as _GRID_ARGS, default_font as _DEFAULT_FONT


class GuiWidget:
    """
    Base mixin for GUI widgets that provides common functionality:
    
    - Automatic grid placement using the parent GuiFrame.
    - Management of a Tkinter text variable.
    - Event binding.
    - Tkinter variable registration.
    
    This class is designed to be used in combination with a concrete Tkinter widget class.
    
    Attributes
    ----------
    frame : GuiFrame
        The parent frame in which the widget is placed.
    tag : str
        The tag used to associate the widget with a specific position in the grid layout.
    event_bindings : list of tuple
        A list of event binding pairs (event, callback) to be applied to the widget.
    padx : int
        Horizontal padding for grid placement.
    pady : int
        Vertical padding for grid placement.
    sticky : str
        Sticky property for grid placement.
    _text : tk.StringVar
        The Tkinter StringVar used to manage the widget's text.
    _tk_vars : dict
        A dictionary for registered Tkinter variables.
    font : Font
        The font to be used by the widget.
    _max_length : int
        An optional maximum text length. If set, text longer than this will be shortened.
    tk_widget: tk.Widget
        The associated Widget
    """

    __slots__ = ("frame", "tag", "event_bindings", "padx", "pady", "sticky",
                 "_text", "_tk_vars", "font", "_max_length")
    tk_widget: tk.Widget

    def __init__(self):
        """
        Initialize the GuiWidget with default values.
        
        Notes
        -----
        This method initializes internal attributes with default values. It does not create the actual
        Tkinter widget instance; that is the responsibility of the concrete subclass.
        """
        self.frame: Optional[GuiFrame] = None
        self.tag: str = ""
        self.event_bindings: List[EventBinding] = []
        self.padx: int = 0
        self.pady: int = 0
        self.sticky: str = 'N'
        self._text: Optional[tk.StringVar] = None
        self._tk_vars: Dict[str, tk.Variable] = {}
        self.font: Font = _DEFAULT_FONT
        self._max_length: Optional[int] = None

    def apply_grid(self, **kwargs):
        """
        Place the widget using the grid geometry manager, merging default grid options with those provided.
        
        Expects that the concrete widget instance is stored in the attribute 'tk_widget'.
        
        Parameters
        ----------
        **kwargs : dict
            Additional grid options (e.g., padx, pady, sticky) to be merged with default settings.
        
        Raises
        ------
        AttributeError
            If the widget is not associated with a valid GuiFrame or if the underlying widget does not have a grid method.
        """
        for key in _GRID_ARGS:
            if key not in kwargs and hasattr(self, key):
                kwargs[key] = getattr(self, key)
        if self.frame is None or not hasattr(self.frame, "get_grid_kwargs"):
            raise AttributeError("GuiWidget must be associated with a valid GuiFrame instance.")
        grid_kwargs = self.frame.get_grid_kwargs(self.tag, **kwargs)
        self.tk_widget.grid(**grid_kwargs)

    def init_widget_start(self, frame: GuiFrame, tag: str, text: str = None,
                          text_variable: tk.StringVar = None, **kwargs):
        """
        Initialize common widget properties before creating the concrete widget.
        
        This method sets the parent frame, widget tag, and text variable. It also processes any padding parameters.
        
        Parameters
        ----------
        frame : GuiFrame
            The parent GuiFrame.
        tag : str
            The tag for grid placement.
        text : str, optional
            The initial text for the widget.
        text_variable : tk.StringVar, optional
            A pre-existing StringVar to use for the widget's text.
        **kwargs : dict
            Additional keyword arguments (e.g., padding options).
        """
        self.frame = frame
        self.tag = tag
        self._max_length = kwargs.pop("max_text_length", None)
        if "font" in kwargs:
            self.font = kwargs.pop("font")
        if text_variable is None:
            self._text = tk.StringVar(frame.frame, value=text)
        else:
            if text is not None:
                text_variable.set(text)
            self._text = text_variable
        self._set_padding(**kwargs)

    def init_widget_end(self, event_bindings: List[Tuple[str, Callable]] = None,
                        apply_grid_flag: bool = True, **kwargs):
        """
        Finalize widget initialization.
        
        This method sets event bindings and applies grid placement if requested.
        
        Parameters
        ----------
        event_bindings : list of tuple, optional
            A list of event binding pairs (event, callback) to attach to the widget.
        apply_grid_flag : bool, optional
            If True, calls apply_grid() to place the widget using the grid geometry manager.
        **kwargs : dict
            Additional grid options for apply_grid.
        """
        if event_bindings:
            self._set_bindings(event_bindings)
        if apply_grid_flag:
            self.apply_grid(**kwargs)

    @property
    def text(self) -> str:
        """
        Get the current text of the widget.
        
        Returns
        -------
        str
            The text stored in the widget's StringVar.
        """
        return self._text.get() if self._text else ""

    @text.setter
    def text(self, new_text: str):
        """
        Set the widget's text, shortening it if it exceeds _max_length.
        
        Parameters
        ----------
        new_text : str
            The new text to set.
        """
        if self._max_length is not None and len(new_text) > self._max_length:
            new_text = shorten_string(new_text, self._max_length)
        if self._text is None:
            self._text = tk.StringVar(value=new_text)
        else:
            self._text.set(new_text)

    def set_text_variable(self, text_var: tk.StringVar):
        """
        Assign a new tk.StringVar as the widget's text variable.
        
        Parameters
        ----------
        text_var : tk.StringVar
            The new text variable.
        """
        self._text = text_var

    def _set_padding(self, **kwargs):
        """
        Process and set padding parameters from kwargs.
        
        Supports 'padxy', 'padx', and 'pady'.
        
        Parameters
        ----------
        **kwargs : dict
            Padding options.
        """
        pad_keys = {"padxy", "padx", "pady"}
        for key in pad_keys.intersection(kwargs.keys()):
            value = kwargs[key]
            if key == "padxy":
                self.padx, self.pady = (value, value) if not isinstance(value, (tuple, list)) else value[:2]
                break
            else:
                setattr(self, key, value)
        for key in pad_keys:
            kwargs.pop(key, None)

    def _set_bindings(self, event_bindings: List[Tuple[str, Callable]]):
        """
        Set event bindings on the underlying widget.
        
        Parameters
        ----------
        event_bindings : list of tuple
            A list of (event, callback) pairs to be bound to the widget.
        """
        self.event_bindings.extend(event_bindings)
        for trigger, callback in self.event_bindings:
            self.tk_widget.bind(trigger, callback)

    def add_tk_var(self, var: tk.Variable, key: str, value: Any = None):
        """
        Register a Tkinter variable with an associated key.
        
        Parameters
        ----------
        var : tk.Variable
            The Tkinter variable to register.
        key : str
            The key under which the variable will be stored.
        value : any, optional
            If provided, the variable will be set to this value.
        """
        if value is not None:
            var.set(value)
        self._tk_vars[key] = var

    def set_tk_var(self, name: str, value: Any, create_new: bool = True):
        """
        Update a Tkinter variable by name. If not found and creation is allowed, a new variable is created.
        
        Parameters
        ----------
        name : str
            The key associated with the Tk variable.
        value : any
            The new value to assign.
        create_new : bool, optional
            If True, a new Tk variable is created if one does not exist. Otherwise, raises KeyError.
        
        Raises
        ------
        KeyError
            If the variable is not found and create_new is False.
        """
        if name in self._tk_vars:
            self._tk_vars[name].set(value)
        elif create_new:
            new_var = tk.Variable(value=value, name=name)
            self._tk_vars[name] = new_var
        else:
            raise KeyError(f"Tk variable '{name}' not found and creation is disabled.")
    
    # @dispatch(str, callable, str)
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
        
        self.tk_widget.bind(event, callback)
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
        self.tk_widget.bind(*event_binding.bind_args)
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
        self.tk_widget.unbind(event)
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
        self.tk_widget.unbind(event.event)
        self.event_bindings.remove(event)
    
    @staticmethod
    def tk_var(value: Any, name: str, master: Optional[tk.Frame | ttk.Frame | tk.Toplevel] = None) -> tk.Variable:
        if isinstance(value, int):
            return tk.IntVar(value=value, name=name, master=master)
        elif isinstance(value, str):
            return tk.StringVar(value=value, name=name, master=master)
        elif isinstance(value, float):
            return tk.DoubleVar(value=value, name=name, master=master)
        elif isinstance(value, bool):
            return tk.BooleanVar(value=value, name=name, master=master)
        else:
            return tk.Variable(value=value, name=name, master=master)
