"""
Module with models for Gui Buttons (Button, Checkbutton, Radiobutton)

# TODO: Shift some stuff to controllers

"""
import tkinter as tk
import tkinter.ttk as ttk
from collections.abc import Callable
from typing import Dict

from gui.base.widget import GuiWidget
from gui.base.frame import TkGrid, GuiFrame


# Clickcommand is the method that is called when the button is pressed.
class GuiButton(ttk.Button, GuiWidget):
    command_kwargs: Dict[str, any]
    command: Callable = lambda: None
    
    def __init__(self, frame: GuiFrame, tag: str, command: Callable, command_kwargs: dict = None,
                 variable=None, event_bindings=(), width=None, button_text: str = None, **kwargs):
        """ Class for setting up tk Button widget.
        A tk.Button built on top of the given frame with frequently used attributes, used to define tk elements in a
        standardized fashion. Commonly tweaked parameters can be passed as well. All parameters with the exception of
        grid correspond to attributes used in original tk objects.

        Parameters
        ----------
        frame : ttk.Frame
            The ttk frame on which the Label will be placed.

        command : callable
            Callback for when the button is pressed.

        Attributes
        ----------
        frame : ttk.Frame
            Frame on which the object will be placed

        Methods
        -------
        set_text(string)
            Method for changing the text displayed by the label
        """

        self.init_widget_start(frame=frame, tag=tag, text=button_text, **{k:v for k,v in kwargs.items() if k != 'text'})
        self.set_command(command, command_kwargs)
        # self.command = lambda name: command(column_name=name)
        # self.command_kwargs = command_kwargs if isinstance(command_kwargs, dict) else {}

        super().__init__(self.frame, command=self.execute_command, textvariable=self._text, width=width)
        
        self.init_widget_end(event_bindings=event_bindings, **kwargs)
        
    def set_button_text(self, text: str):
        """ Sets the Button text to `text` """
        self.set_tk_var(name='button_text', value=text)
        
    def set_command(self, command: Callable = None, command_kwargs: dict = None):
        """ Sets the onclick command and/or kwargs passed to it `command` and `command_kwargs`, respectively """
        if command is not None:
            self.command = command
        if command_kwargs is not None:
            self.command_kwargs = command_kwargs
    
    def execute_command(self):
        """ Execute the configured command while passing the configured command_kwargs as kwargs """
        print(self.command_kwargs)
        self.command(**self.command_kwargs)


if __name__ == '__main__':
    def my_method(text: str = 'hoi'):
        print(text)
    f = GuiFrame(['aaabbb'])
    b = GuiButton(f, command=my_method, command_kwargs={'text': 'Ja'}, tag='a')
    b.execute_command()
    for k, v in b.__dict__.items():
        print(k, v)