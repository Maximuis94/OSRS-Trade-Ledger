import time
import tkinter as tk
from collections.abc import Iterable
from concurrent import futures
from datetime import datetime
from tkinter import ttk

from gui.frames.navigation_frame import NavigationFrame
from gui.component.button import GuiButton
from gui.base.frame import TkGrid, GuiFrame
from gui.component.event_bindings import EventBinding, lmb
from gui.component.label import GuiLabel
from util.gui_formats import rgb_to_colorcode
# from tab_inventory import InventoryFrame

thread_pool_executor = futures.ThreadPoolExecutor(max_workers=1)

# Implement the default Matplotlib key bindings.

simulating = False

"""
Parent GUI window code, in here the outer window + tabs are defined. The specific implementation of each tab can be
found in its respective class.
"""


def do_this(var_a: int, var_b: int) -> bool:
    """_summary_

    Parameters
    ----------
    var_a : int
        _description_
    var_b
        _description_

    Returns
    -------
        _description_
    """       
    return


def say_hoi(text: str = 'hoi'):
    print(text)


def print_event(e: tk.Event, keys: Iterable[str] = None):
    """ Example of a callback that will print non-null values of event `e`. If keys is defined, follow that order. """
    if keys is None:
        print('Current time:', datetime.now())
        print(', '.join([f"{k}={v}" for k, v in e.__dict__.items() if v != '??']) + '\n')
    else:
        print(', '.join([f"{k}={e.__dict__[k]}" for k in keys if e.__dict__.get(k) != '??']))


class GraphicalUserInterface(tk.Frame):
    """
    Main class for the Graphical User Interface (GUI).
    Depending on which mode is selected, the GUI will show different frames.
    The GUI consists roughly of 4 parts; the NavigationFrame, the top ListboxFrame, the bottom ListboxFrame and the
    GraphFrame.
    More frames may be added later.
    
    """
    window_name: str = ''
    window_width: int = 800
    window_height: int = 600
    tk_grid: TkGrid = TkGrid(['AAAAAAAAAA', 'AABBBBBBBB', 'AACCCCCCCC', 'AADDDDDDDD', 'AAEEEEEEEE'])
    
    def __init__(self, window: tk.Tk, name: str, width: int = 800, height: int = 600, **kwargs):
        super().__init__(**kwargs)
        self.window_name = name
        self.window_width = width
        self.window_height = height
        self.window = window
        self.window.geometry(f'{self.window_width}x{self.window_height}')
        
        self.navigation_frame = NavigationFrame(window, grid_kwargs=self.tk_grid.get_dims('A', 10, 10, sticky='NW'))
        
        # self.grid(**self.tk_grid.get_dims(self.grid_tag, pady=self.pady, padx=self.padx, sticky=self.sticky))
        self.grid(column=0, row=0, sticky="NW", padx=10, pady=10)

    def get_window(self):
        return self.window
        

# GUI Contains the primary window. Each tab is defined as a separate class to keep stuff more organized
class _GUI(tk.Frame):
    def __init__(self, window, **kw):
        """
        Main GUI class that contains all the GUI tabs
        
        Parameters
        ----------
        window :
        kw :
        """
        self.start_time = time.time()
        super().__init__(window, **kw)
        self.window = window
        self.frame = GuiFrame(self.window, ['AAAAABBBBB', 'CCCCCCCCCC'])
        # self.inventory = Inventory(Ledger(), import_data=True)#ledger_ts=0)#self.ledger.last_updated)
        # self.inventory.create_timeline = True
        bgc = rgb_to_colorcode((125, 125, 125))
        self.configure()
        self.window_width, self.window_height = 1900, 580
        self.window.geometry(f'{self.window_width}x{self.window_height}')

        ttk.Style().configure("Main.Style", background='red')

        self.rootHeight = window.winfo_height()
        self.rootWidth = window.winfo_width()

        self.window.title("OSRS Trade DB")
        self.button = GuiButton(self.frame,
                                command=say_hoi,
                                command_kwargs={'text': f'BUTTON PRESSED'},
                                width=10,
                                tag='B',
                                # id='Button_',
                                button_text='hoi',
                                event_bindings=[lmb(print_event)])
        # self.button.bind("<Button-1>", lambda e: print_event(e))
        
        self.button = GuiButton(self.frame,
                                command=say_hoi,
                                command_kwargs={'text': 'HALLO DAAR'},
                                width=10,
                                tag='A',
                                # id='Button_',
                                button_text='hoi')
        
        self.label = GuiLabel(self.frame, text="Label test", tag='C', sticky='NWE')
        # self.frame.pack(fill='x')
        self.frame.grid(column=0, row=0)
        self.cursor_over_frame = False
        self.keyboard_over_frame = True
        # print_task_length('starting the application', start_time)

    def get_window(self):
        return self.window
