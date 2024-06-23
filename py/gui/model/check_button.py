"""
Module the implementation of a checkbutton for the GUI.

"""


import tkinter as tk
import tkinter.ttk as ttk
from collections.abc import Callable
from typing import Tuple, List

from gui.model.grid import TkGrid
from gui.model.gui_widget import GuiWidget


class GuiCheckbutton(ttk.Checkbutton, GuiWidget):
    def __init__(self, frame, grid: TkGrid, grid_tag: str, initial_state: bool = False, textvariable: tk.StringVar = None,
                 variable: tk.Variable = None, event_bindings: List[Tuple[str, Callable]] = (), text: str = None, **kwargs):
        if variable is None:
            self.status = tk.BooleanVar()
        else:
            self.status = variable
        self.status.set(initial_state)
        
        self.text = tk.StringVar(self.frame) if textvariable is None else textvariable
        self.text.set(text)
        
        super().__init__(textvariable=self.text, variable=self.status, onvalue=True, offvalue=False, **kwargs)
        super(GuiWidget, self).__init__(frame, grid_tag, grid, event_bindings=event_bindings)

    
    def get(self):
        return self.status.get()
    
    def set(self, new_status: bool):
        self.status.set(new_status)
