"""
Module with the basic SatelliteFrame implementation.
It acts as a separate window, while also being a part of the original window, as it can directly interact with said
window

"""
import tkinter as tk
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from tkinter import ttk
from typing import Dict, final, Tuple, Optional

from gui.base.frame import GuiFrame
from gui.base.widget import GuiWidget

DEFAULT_WINDOW_NAME: str = "Satellite Frame"
"""Name assigned to a SatelliteFrame by default"""


class SatelliteFrame(tk.Toplevel, ABC):
    """
    A pop-up window ("satellite") that works in tandem with a main window.
    This Frame serves as a template class with some basic behaviour that may be overridden.
    By design, a SatelliteFrame is a disposable frame that serves some other window.
    
    Methods
    -------
    create_widgets()
        Method for setting up all widgets during initialization. Is to be customized by subclasses.
        
    
    """
    frame: GuiFrame
    """The GuiFrame all Widgets of this SatelliteFrame are placed onto"""
    
    widgets: Dict[str, GuiWidget]
    """A dict with all widgets placed on this frame as to provide a central point for accessing them"""
    
    data_callback: Callable[[Dict[str, any]], any]
    """Method used as a means to convey information from this window to the main window."""
    
    _data_collected: Dict[str, any]
    """The data that will be passed arg in `data_callback`"""
    
    callback_onclose: Optional[Callable] = None
    """Callback function to invoke when onclose is pressed. If on_close() is overridden, this is executed after that."""
    
    def __init__(self, grid_layout: Iterable[str] | str, data_callback: Callable[[Dict[str, any]], any] = None,
                 title: str = None, spawn_point: Optional[tk] = None, callback_onclose: Callable = None, **kwargs):
        """
        Launch the SatelliteFrame!
        
        Parameters
        ----------
        grid_layout : Optional[TkGrid | Iterable[str] | str]
            grid layout to apply for widgets on this Frame
        data_callback : Callable[[Dict[str, any]], any]
            Callback that is made to send data back
        title : Optional[str]
            Window title
        is_transient : Optional[bool], True by default
            If True, make this window appear before the main window
        callback_onclose : Callable
            function/method to invoke after the close button is pressed, but before this Frame is destroyed. This
            callable is different from the method of this frame in the sense that it is naive towards this frame, while
            the method of this frame is naive towards the receiving entity
        spawn_point : Optional[ttk.Frame]
            A spawn point for this window. It will appear in front of whatever is passed, if anything is passed. For
            instance, if you want to launch it from the widget that spawned it, pass that Frame.
        """
        super().__init__(master=ttk.Frame(), **kwargs)
        self.frame = GuiFrame(self, grid_layout)
        
        # Keep a reference to the callback function
        self._data_collected = {}
        self.data_callback: Callable[[Dict[str, any]], any] = data_callback
        
        # Give the pop-up a title (optional)
        self.title(DEFAULT_WINDOW_NAME if title is None else title)
        if spawn_point is not None:
            self.is_transient = kwargs.get('is_transient', True)
        
        # Optional: if you want to block interactions with the main window until
        # the pop-up is closed, uncomment the following line:
        # self.grab_set()
        
        # Example widgets inside the satellite frame
        self.widgets = {}
        
        # Optional: make the SatelliteFrame appear in front of the main window
        if spawn_point is not None:
            self.transient(spawn_point)
        
        if callback_onclose is not None:
            self.callback_onclose = callback_onclose
            
        self.create_widgets()
        self.protocol("WM_DELETE_WINDOW", self.on_close())
    
    @abstractmethod
    def create_widgets(self):
        """Create all Widgets contained by this SatelliteFrame. Should be implemented in subclasses."""
        raise NotImplementedError
    
    def collect_data(self, key: str, data: any):
        """
        Add data to the dict, which will be sent back to the main window whenever `send_data` is invoked.
        
        Parameters
        ----------
        key : str
            Key to use in the dict
            
        data : any
            The value to store under `key`
        """
        self._data_collected[key] = data
    
    def send_data(self):
        """
        Collect the data, organize it as a dict and send it to the main GUI via a callback method defined during
        initialization.
        
        This callback method is defined in this frame as `main_window_callback`, which is a function
        """
        self.data_callback(self._data_collected)
    
    def on_close(self):
        """Optional overridable method that is invoked right before the close button is pushed."""
        ...
    
    def generate_str(self, *args, **kwargs) -> Tuple[str, Dict[str, any]]:
        """String generation method. Can be overridden to customize printed strings. Input is same as print()"""
        return " ".join(args), kwargs
    
    # TODO move to Logger class
    @final
    def print(self, *msg, **kwargs):
        """Printer for thi SatelliteFrame. By default it prints, although it will also attempt to invoke log()"""
        msg, kwargs = self.generate_str(*msg, **kwargs)
        print(*msg, **kwargs)
        try:
            if hasattr(self, 'log'):
                self.log(msg, **kwargs)
        except AttributeError as e:
            msg = (f"Attribute 'log' should be a callable that accepts a positional string arg and a set of keyword args"
                   f"as input. E.g. print(*s, end: str = '\\n'")
            raise TypeError(msg)
    
    @final
    def _on_close(self):
        """Executed when clicking close button. It invokes `on_close()`, `callback_onclose()` as well as `destroy()`"""
        self.on_close()
        if self.callback_onclose is not None:
            self.callback_onclose()
        self.destroy()
    

if __name__ == "__main__":
    # Example usage of SatelliteFrame
    
    def main_callback(data):
        """
        A function defined in the main GUI that processes any data
        received from the SatelliteFrame.
        """
        print("Data from SatelliteFrame:", data)
        status_label.config(text=f"Last data received: {data}")
    
    
    root = tk.Tk()
    root.title("Main Window")
    
    # A button in the main window that opens the SatelliteFrame
    open_satellite_button = ttk.Button(
        root,
        text="Open Satellite",
        command=lambda: SatelliteFrame(master=root, data_callback=main_callback)
    )
    open_satellite_button.pack(pady=20)
    
    status_label = ttk.Label(root, text="No data received yet.")
    status_label.pack(pady=10)
    
    root.mainloop()
