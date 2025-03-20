"""
Module: gui_checkbutton.py
==========================
Refactored GuiCheckbutton class.

This class extends ttk.Checkbutton and integrates GuiWidget's common features such as
grid placement, text management, event binding, and Tk variable registration.

It supports an optional style parameter that, if not provided, defaults to a pre-configured
style based on the default font and color.
"""

import tkinter as tk
import tkinter.ttk as ttk
from typing import Tuple, List, Callable
from gui.base.widget import GuiWidget
from gui.base.frame import GuiFrame
from gui.util.colors import Color
from gui.util.font import Font


class GuiCheckbutton(ttk.Checkbutton, GuiWidget):
    """
    Base class for a GUI Checkbutton that combines ttk.Checkbutton with common widget
    functionality provided by GuiWidget.

    Attributes
    ----------
    _status : tk.BooleanVar
        The variable that tracks the checkbutton's state.
    """
    __slots__ = ("_status",)

    def __init__(self,
                 gui_frame: GuiFrame,
                 tag: str,
                 initial_state: bool = None,
                 text: str = None,
                 text_variable: tk.StringVar = None,
                 status_variable: tk.BooleanVar = None,
                 event_bindings: List[Tuple[str, Callable]] = (),
                 style: str = None,
                 **kwargs):
        """
        Initialize a GuiCheckbutton.

        Parameters
        ----------
        gui_frame : GuiFrame
            The parent frame in which the checkbutton is placed.
        tag : str
            Tag used for grid placement.
        initial_state : bool, optional
            The initial checked state. If provided, sets the checkbutton state.
        text : str, optional
            The label text of the checkbutton.
        text_variable : tk.StringVar, optional
            A pre-existing StringVar for the button's text. If not provided, one is created.
        status_variable : tk.BooleanVar, optional
            A pre-existing BooleanVar for the checkbutton's state. If not provided, one is created.
        event_bindings : list of tuple, optional
            Additional event bindings (event, callback) to be set on the widget.
        style : str, optional
            ttk style to apply to the checkbutton. If not provided, a default style ("GuiCheckbutton.TCheckbutton")
            is configured using the default font and Color.BLACK.
        **kwargs : dict
            Additional options (e.g., padding) passed to grid layout.
        """
        # Create or assign the status variable.
        if status_variable is None:
            self._status = tk.BooleanVar()
        else:
            self._status = status_variable

        if initial_state is not None:
            self._status.set(initial_state)

        # Initialize common widget properties via GuiWidget.
        GuiWidget.__init__(self)
        self.frame = gui_frame  # Use public frame property from GuiFrame.
        self.tag = tag

        # Initialize text variable via GuiFrame helper.
        text_var = gui_frame.init_widget_start(tag, text=text, text_variable=text_variable, **kwargs)
        self._text = text_var

        # Determine style. If not provided, configure a default style.
        if style is None:
            style = "GuiCheckbutton.TCheckbutton"
            s = ttk.Style()
            # Configure default style using the widget's font and default foreground color.
            s.configure(style, font=self.font.tk, foreground=Color.BLACK.value.hexadecimal)
        kwargs.setdefault('style', style)

        # Create the underlying ttk.Checkbutton; assign to tk_widget.
        self.tk_widget = ttk.Checkbutton(gui_frame.frame,
                                         variable=self._status,
                                         textvariable=self._text,
                                         **kwargs)

        # Finalize widget initialization: event bindings and grid placement.
        self.init_widget_end(event_bindings, **kwargs)

    @property
    def status(self) -> bool:
        """
        Get the current state of the checkbutton.

        Returns
        -------
        bool
            True if the checkbutton is checked, False otherwise.
        """
        return self._status.get()

    @status.setter
    def status(self, new_status: bool) -> None:
        """
        Set the checkbutton's state.

        Parameters
        ----------
        new_status : bool
            The new state to set; True for checked, False for unchecked.
        """
        self._status.set(new_status)
