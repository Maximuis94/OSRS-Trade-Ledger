"""
Module with a ButtonPanel frame implementation.


"""
import tkinter as tk
from collections.abc import Iterable
from tkinter import ttk
from typing import Dict, Tuple

from gui.base.widget import GuiWidget
from gui.component.button import GuiButton


class ButtonPanel(ttk.Frame, GuiWidget):
    """
    Object that consists of a frame with 1-n buttons
    
    """
    __slots__ = ("buttons",)
    buttons: Tuple[GuiButton, ...]
    
    def __init__(self, kwargs_per_button: Iterable[Dict[str, any]], common_kwargs: Dict[str, any], **kwargs):
        self.init_widget_start(**kwargs)
        super().__init__(**kwargs)
        self.init_widget_end(**kwargs)
        
        _kwargs = []
        for kw in kwargs_per_button:
            kw.update(common_kwargs)
            _kwargs.append(kw)
        
        self.buttons = tuple((GuiButton(**kw) for kw in _kwargs))
        
    