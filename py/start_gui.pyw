"""
Executable py script for launching the GUI

"""
import tkinter as tk

from gui.gui import GUI


def start_db_gui():
    """ Launches the Graphical User Interface """
    print("Starting application...")
    root = tk.Tk()
    gui = GUI(root)
    window = gui.get_window()
    window.mainloop()


if __name__ == '__main__':
    start_db_gui()
    # rooot = tk.Tk()
    # popup = PopUpWindow('counter')
    # popup.mainloop()
    # popup.update_label()
    # frm = ttk.Frame(root, padding=10)
    # frm.grid()
    # root.mainloop()
