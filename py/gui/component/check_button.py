"""
Module the implementation of a checkbutton for the GUI.

"""


import tkinter as tk
import tkinter.ttk as ttk
from collections.abc import Callable
from typing import Tuple, List

from gui.base.widget import GuiWidget
from gui.base.frame import TkGrid


class GuiCheckbutton(ttk.Checkbutton, GuiWidget):
    __slots__ = ("_status",)
    
    def __init__(self, frame, tag: str, initial_state: bool = None, text_variable: tk.StringVar = None,
                 status_variable: tk.BooleanVar = None, event_bindings: List[Tuple[str, Callable]] = (), text: str = None, **kwargs):
        if status_variable is None:
            self._status = tk.BooleanVar()
        else:
            self._status = status_variable
            
        if initial_state is not None:
            self._status.set(initial_state)
            
        self.init_widget_start(frame, tag, text=text, text_variable=text_variable, **kwargs)
        super().__init__(self.frame, textvariable=self._text)
        self.init_widget_end(event_bindings=event_bindings, **kwargs)
    
    @property
    def status(self):
        """ The status of the checkbutton; interacts with the underlying tk.BooleanVar """
        return self._status.get()
    
    @status.setter
    def status(self, new_status: bool):
        self._status.set(new_status)
