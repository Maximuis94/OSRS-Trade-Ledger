import time
import tkinter as tk
from concurrent import futures
from tkinter import ttk

from gui.model.button import GuiButton
from gui.model.grid import TkGrid
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

# GUI Contains the primary window. Each tab is defined as a separate class to keep stuff more organized
class GUI(tk.Frame):
    def __init__(self, window, **kw):
        """
        Main GUI class that contains all the GUI tabs
        
        Parameters
        ----------
        window :
        kw :
        """
        self.start_time = time.time()
        super().__init__(**kw)
        self.window = window
        self.frame = ttk.Frame(self.window)
        # self.inventory = Inventory(Ledger(), import_data=True)#ledger_ts=0)#self.ledger.last_updated)
        # self.inventory.create_timeline = True
        bgc = rgb_to_colorcode((125, 125, 125))
        self.configure()
        self.window_width, self.window_height = 1900, 580
        self.window.geometry(f'{self.window_width}x{self.window_height}')

        ttk.Style().configure("Main.Style", background='red')

        self.rootHeight = window.winfo_height()
        self.rootWidth = window.winfo_width()
        grid = TkGrid(['AAAAABBBBB', 'CCCCCCCCCC'])

        self.window.title("OSRS Trade DB")
        self.button = GuiButton(self.frame,
                                command=say_hoi,
                                command_kwargs={'text': 'HALLO DAAR'},
                                width=10,
                                grid=grid,
                                grid_tag='B',
                                # id='Button_',
                                button_text='hoi')
        # self.button.apply_grid()
        # Navigation tabs
        # self.tab_parent = ttk.Notebook(self.window)
        # self.tab_parent.pack(expand=1, fill='both')
        # self.inventory_tab = ttk.Frame(self.tab_parent, width=self.window_width, height=self.window_height)
        # self.ledger_tab = ttk.Frame(self.tab_parent, width=self.window_width * 0.95, height=self.window_height * 0.95)
        # self.planned_trades_tab = ttk.Frame(self.tab_parent, width=self.window_width, height=self.window_height)
        # self.wiki_tab = ttk.Frame(self.tab_parent, width=self.window_width, height=self.window_height)
        # self.settings_tab = ttk.Frame(self.tab_parent, width=self.window_width, height=self.window_height)
        # self.tab_parent.add(self.inventory_tab, text="Inventory")
        # self.tab_parent.add(self.ledger_tab, text="GE Ledger Entry")
        # self.tab_parent.add(self.planned_trades_tab, text="Planned trades")
        # self.tab_parent.add(self.wiki_tab, text="Item tracker")
        # self.tab_parent.add(self.settings_tab, text="Settings")

        # self.inventory_frame = ttk.Frame(self.inventory_tab, width=self.window_width * 0.95,
        #                                  height=self.window_height * 0.95)
        # self.inventory_frame.grid(row=0, column=0)
        # self.inventory_frame = InventoryFrame(self.inventory_frame)
        
        # self.ledger_frame = ttk.Frame(self.ledger_tab, width=self.window_width * 0.95, height=self.window_height * 0.95)
        # self.ledger_frame.grid(row=0, column=0)
        # self.ledger_frame = LedgerFrame(self.ledger_frame, None, res=self.res)
        
        # self.planned_trades_frame = ttk.Frame(self.planned_trades_tab, width=self.window_width * 0.95,
        #                                       height=self.window_height * 0.95, relief=tk.SUNKEN)
        # self.planned_trades_frame.grid(row=0, column=0)
        # self.planned_trades_frame = TradeAdvisorFrame(self.planned_trades_tab,  inventory=None)
        
        # self.wiki_frame = ttk.Frame(self.wiki_tab, width=self.window_width * 0.95,
        #                             height=self.window_height * 0.95)
        # self.wiki_frame.grid(row=0, column=0)
        # self.wiki_frame = ItemTracker(self.wiki_frame, res=self.res, val=self.val)
        
        # self.settings_frame = ttk.Frame(self.settings_tab, width=self.window_width * 0.95,
        #                                 height=self.window_height * 0.95)
        # self.settings_frame.grid(row=0, column=0)
        # self.settings_frame = SettingsFrame(self.settings_frame)

        # self.tab_parent.bind("<<NotebookTabChanged>>", self.changed_tab)
        # self.frame.pack(fill='x')
        self.frame.grid(column=0, row=0)
        self.cursor_over_frame = False
        self.keyboard_over_frame = True
        # print_task_length('starting the application', start_time)

    # This code will run when the user opens the tab
    # The cursor info is used to determine whether the user opened this frame from another tab or reactivated the window
    def changed_tab(self, event):
        # task = "Changed tabs"
        # start = time.time()
        # if self.inventory_frame.keyboard_over_frame.get():
        #     task = 'Switching to inventory tab'
        #     print(task)
        #     self.inventory_frame.entered_tab()
        ...
        # if self.ledger_frame.keyboard_over_frame.get():
        #     task = 'Switching to ledger tab'
        #     print(task)
        #     self.ledger_frame.entered_tab()
        # if self.wiki_frame.keyboard_over_frame.get():
        #     task = 'loading wiki tab'
        #     self.wiki_frame.entered_tab()
        # print_task_length(task, start)
        # if self.start_time is not None:
        #     print_task_length(task='started GUI', start_time=self.start_time)
        #     self.start_time = None
        # exit(0)

    def get_window(self):
        return self.window
