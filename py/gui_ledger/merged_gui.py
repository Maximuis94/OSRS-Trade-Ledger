"""Module with a class that combines all individual elements into a single entity"""

from concurrent import futures

import tkinter as tk

from gui.base.frame import TkGrid, GuiFrame
from gui.component.button import GuiButton
from gui.util.constants import letters
from gui_ledger.navigation_frame import NavigationFrame

# from tab_inventory import InventoryFrame

thread_pool_executor = futures.ThreadPoolExecutor(max_workers=1)

# Implement the default Matplotlib key bindings.

simulating = False

"""
Parent GUI window code, in here the outer window + tabs are defined. The specific implementation of each tab can be
found in its respective class.
"""


class GraphicalUserInterface:
    """
    Main class for the Graphical User Interface (GUI).
    Depending on which mode is selected, the GUI will show different frames.
    On startup, the GUI consists of 3 frames;
    1. NavigationFrame - Primarily used to alter what the other frames display
    2. ListboxFrame - Pair of listboxes that can display various types of information
    3. GraphFrame - A Frame that displays some graph

    """
    application_name: str = ''
    initial_width: int = 800
    initial_height: int = 600
    
    tk_grid: TkGrid = TkGrid(['AABBBBBBBBBCCCCC',
                              'AABBBBBBBBBCCCCC',
                              'AABBBBBBBBBCCCCC',
                              'AABBBBBBBBBCCCCC',
                              'XXBBBBBBBBBCCCCC'])
    
    button_column: GuiFrame
    navigation_frame: NavigationFrame
    
    def __init__(self, window: tk.Tk):
        self.window = window
        self.window.geometry(f'{self.initial_width}x{self.initial_height}')
        self.window.protocol("WM_DELETE_WINDOW", self.close_button_callback)
        
        self.frame = GuiFrame(self.window, self.tk_grid)
        
        self.navigation_frame = NavigationFrame(self.frame, grid_kwargs=self.frame.get_grid_kwargs("A"), button_callback=self.button_column_callback, close_button_callback=self.close_button_callback, max_text_length=15)

        # self.grid(**self.tk_grid.get_dims(self.grid_tag, pady=self.pady, padx=self.padx, sticky=self.sticky))
        self.frame.grid(column=0, row=0, sticky="NW", padx=10, pady=10)
    
    def get_window(self):
        return self.window
    
    def button_column_callback(self, button_id: str, **kwargs):
        """Callback for the button column. Has a variety of responses, depending on which button is pushed."""
        self.navigation_frame.label_text = button_id
    
    def generate_button_column(self, n_buttons: int):
        """Generate a GuiFrame with a set of buttons for navigating through the application"""
        self.button_column = GuiFrame(self.frame, TkGrid([c for c in letters[:n_buttons]]), tag="A")
        common_kwargs = {"frame": self.button_column, "command": self.button_column_press, "width": 30}
        kws = [
            {"tag": tag, "button_text": text, "command_kwargs": {"method_id": text}} for tag, text in
            zip(letters, ["Inventory", "Results/day", "Item prices", "Overall results", "Import data"])
        ]
        self.buttons = [GuiButton(**{**kw, **common_kwargs}) for kw in kws]
        self.button_column.grid(**self.frame.get_grid_kwargs(self.button_column.tag))
    
    def button_column_press(self, **kwargs):
        """Callback for when a button from the column is pressed."""
        method_id = kwargs.pop('method_id').lower()
        
        if method_id.startswith("inventory"):
            ...
        elif method_id.startswith("results"):
            ...
        elif method_id.startswith("item"):
            ...
        elif method_id.startswith("overall"):
            ...
        elif method_id.startswith("import"):
            ...
    
    def close_button_callback(self):
        """Close button callback. It closes the application as soon as possible."""
        self.window.destroy()
