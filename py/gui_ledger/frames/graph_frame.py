"""
The graph frame is located on the far right and it displays a graph.
What to display is determined via the adjacent frames. E.g. selecting an item in a listbox.

"""
import tkinter as tk
from tkinter import ttk

from gui.base.frame import TkGrid


class GraphFrame(ttk.Frame):
    """
    Class representation of the GraphFrame.
    
    """
    tk_grid: TkGrid = TkGrid([''])
    graph: Graph = None
    
    def __init__(self, window, **kwargs):
        self.frame = window
        super().__init__(self.frame, **kwargs)
        
        
        
        