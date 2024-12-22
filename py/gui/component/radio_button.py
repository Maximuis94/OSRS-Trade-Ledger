import tkinter as tk

from gui.base.widget import GuiWidget


class GuiRadiobutton(tk.Radiobutton, GuiWidget):
    """
    TODO: Rewrite as a set of radiobuttons OR add a buttonpanel class
    
    """
    __slots__ = ("_status",)
    _status: tk.Variable

    def __init__(self, variable: tk.Variable, value: any, **kwargs):
        self.init_widget_start(**kwargs)
        super().__init__(self.frame, textvariable=self._text, variable=variable, value=value)
        self.init_widget_end(**kwargs)
        