"""
Module with the GuiEntry implementation


"""

import tkinter as tk
from tkinter import ttk

from gui.base.widget import GuiWidget
from gui.util.constants import Side


class GuiEntry(tk.Entry, GuiWidget):
    __slots__ = "label_text", "label", "_entry_text"
    label_text: tk.StringVar
    label: tk.Label
    _entry_text: tk.StringVar
    _default_value: str = ""
    
    def __init__(self, label_text: str or tk.StringVar = None, label_side: Side = Side.LEFT,
                 entry_variable: tk.StringVar = None, value: str = None, default_value: str = None, **kwargs):
        if label_side == Side.LEFT:
            label_position, entry_position = {'row': 0, 'column': 0}, {'row': 0, 'column': 1}
            g = ['AB']
        elif label_side == Side.TOP:
            g = ['A', 'B']
            label_position, entry_position = {'row': 0, 'column': 0}, {'row': 1, 'column': 0}
        elif label_side == Side.RIGHT:
            g = ['BA']
            label_position, entry_position = {'row': 0, 'column': 1}, {'row': 0, 'column': 0}
        elif label_side == Side.BOTTOM:
            g = ['B', 'A']
            label_position, entry_position = {'row': 1, 'column': 0}, {'row': 0, 'column': 0}
        else:
            raise ValueError("Invalid value passed for label_side")
        
        if default_value is not None:
            self.default_value = str(default_value)
        
        # Issue: how to organize the initialization involving multi-widget classes
        # Use a separate subclass?
        self.init_widget_start(frame=self.frame, **kwargs)
        # sub_frame = GuiFrame(self.frame, grid_layout=g)
        sub_frame = ttk.Frame()
        self.label = tk.Label(sub_frame, text=label_text)
        self.label.grid(**label_position)
        
        self._entry_text = tk.StringVar() if entry_variable is None else entry_variable
        self._entry_text.set(self._default_value if value is None else value)
        
        super().__init__(sub_frame, textvariable=self._entry_text, **kwargs)
        self.init_widget_end(apply_grid=False, **kwargs)
        self.grid(**entry_position)
        sub_frame.grid(self.frame.get_grid_kwargs(self.tag, **kwargs))
    
    def set_label_text(self, text: str = None, text_variable: tk.StringVar = None):
        """ Update the text displayed by the label and/or the underlying variable """
        if text_variable is not None:
            self.label_text = text_variable
        if text is not None:
            self.label_text.set(text)
    
    def parse(self) -> str:
        """ Returns the value in the entry """
        return self._entry_text.get()
    
    def reset(self):
        """ Reset the textvariable underlying this entry to its default value """
        self._entry_text.set(self._default_value)
    
    @property
    def default_value(self) -> str:
        """ The default string that is set upon resetting the entry """
        return self._default_value
    
    @default_value.setter
    def default_value(self, value):
        self._default_value = str(value)
        