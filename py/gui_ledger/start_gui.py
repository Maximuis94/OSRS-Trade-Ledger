"""
Executable py script for launching the GUI

"""
import tkinter as tk

from merged_gui import GraphicalUserInterface


def run_gui():
    """Launch the Graphical User Interface"""
    root = tk.Tk()
    gui = GraphicalUserInterface(root)
    window = gui.get_window()
    window.mainloop()


if __name__ == '__main__':
    run_gui()
