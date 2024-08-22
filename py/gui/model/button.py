"""
Module with models for Gui Buttons (Button, Checkbutton, Radiobutton)

# TODO: Shift some stuff to controllers

"""
import tkinter as tk
import tkinter.ttk as ttk
from collections.abc import Callable
from typing import Dict

from gui.model.grid import TkGrid
from gui.model.gui_widget import GuiWidget


# Clickcommand is the method that is called when the button is pressed.
class GuiButton(ttk.Button, GuiWidget):
    command_kwargs: Dict[str, any]
    command: Callable = lambda: None
    
    
    def __init__(self, frame, grid: TkGrid, grid_tag: str, command: Callable, command_kwargs: dict = None,
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
        self.command = command
        self.command_kwargs = command_kwargs if isinstance(command_kwargs, dict) else {}

        self.button_text = kwargs.get('textvariable') if kwargs.get('textvariable') is not None else tk.StringVar()
        if button_text is not None:
            self.set_button_text(text=button_text)

        # super().__init__(command=self.execute_command, width=width, **kwargs)
        super().__init__(frame, command=self.execute_command, textvariable=self.tk_vars.get('button_text'), width=width)
        super(GuiWidget, self).__init__()
        # for key, value in GuiWidget(frame, grid_tag=grid_tag, grid=grid, event_bindings=event_bindings, **kwargs).__dict__.items():
        #     print(f'Set {key} to {value}')
        #     self.__setattr__(key, value)
        # super().__init__()
        self.add_tk_var(var=tk.BooleanVar() if variable is None else variable,
                        key='bool')
        
        
        
        if len(event_bindings) > 0:
            self.set_event_bindings(event_bindings=event_bindings)
        kwargs = self._set_padding(**kwargs)
        self.apply_grid(grid=grid, grid_tag=grid_tag, **kwargs)
        
    def set_button_text(self, text: str):
        """ Sets the Button text to `text` """
        self.set_value(var_key='button_text', value=text)
        
    def set_command(self, command: Callable = None, command_kwargs: dict = None):
        """ Sets the onclick command and/or kwargs passed to it `command` and `command_kwargs`, respectively """
        if command is not None:
            self.command = command
        if command_kwargs is not None:
            self.command_kwargs = command_kwargs
    
    def execute_command(self):
        """ Execute the configured command while passing the configured command_kwargs as kwargs """
        self.command(**self.command_kwargs)



if __name__ == '__main__':
    def my_method(text: str = 'hoi'):
        print(text)
    b = GuiButton(tk.Frame(), command=my_method, command_kwargs={'text': 'Ja'}, grid_tag='a', grid=TkGrid(['aaabbb']))
    b.execute_command()
    for k, v in b.__dict__.items():
        print(k, v)