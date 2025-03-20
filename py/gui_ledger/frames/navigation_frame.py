"""
This module contains the implementation of the NavigationFrame, which is the frame on the far left with various buttons.
The buttons on this frame can be used to switch between various frames, as well as initiate certain actions.

The buttons are placed in a column on the far left. The NavigationFrame consists of a label and a series of buttons.
The label indicates the current 'mode' the GUI is in

"""
import os
from collections.abc import Callable
from tkinter.constants import RAISED

import pandas as pd

import global_variables.path as gp
import global_variables.osrs as go
from global_variables.variables import types

from gui.base.frame import GuiFrame
from gui.component.button import GuiButton
from gui.util.event_binding import lmb
from gui.component.listbox import GuiListbox
import gui.component._listbox.column as listbox_column
from gui.component._listbox.sort import Sort
from common import Database


class NavigationFrame(GuiFrame):
    """
    The navigation Frame is the frame on the left with the label and buttons
    TODO
        1. Implement GuiButton DONE
        2. Implement GuiLabel DONE
        3. Define label and buttons
        4. Add underlying behaviour
    """
    df = pd.DataFrame
    
    def __init__(self, window, button_width=15, grid_kwargs=None, button_callback: Callable = None, **kwargs):
        super().__init__(window, grid_layout=['A', '*', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'], **kwargs)
        self.configure()
        # kw = {'frame': self, 'tk_grid': self.tk_grid, 'width': button_width,
        #       'font': ('Helvetica', '12'), 'sticky': 'WE'}
        # self.label = GuiLabel(grid_tag='A', text="OSRS Trading GUI", relief=SUNKEN, padding=(5, 5, 3, 2), **kw)
        
        kw = {'frame': self, 'width': button_width, 'padxy': (5, 7), "top_label_text": "TOP LABEL TEXT", "bottom_label_text": "BOTTOM LABEL TEXT",
              'font': ('Helvetica', '12'), 'sticky': 'WE', 'relief': RAISED}
        
        e = lmb(self.button_b)
        print(type(window))
        df = pd.read_pickle(os.path.join(gp.dir_data, "test_df.dat"))
        df['item_name'] = df['item_id'].apply(lambda item_id: go.id_name[item_id])
        del df['transaction_id'], df['item_id']
        self.listbox = GuiListbox(self, tag='C', ipadxy=(20, 29), entry_width=80, entries=df.to_dict('records'),
                                  columns=self.listbox_columns, initial_sort=Sort('item_name', False),
                                  onclick_row=self.button_b)
        # print(self.listbox_columns)
        # self.listbox = GuiListboxFrame(tag='C', ipadxy=(20, 29), entry_width=80, entries=df.to_dict('records'),
        #                                columns=self.listbox_columns, initial_sort=Sort('item_name', False), **kw)
        
        self.btn_b = GuiButton(tag='B', command=self.button_b, button_text="Button B", text="Button B", **kw)
        #
        # self.btn_c = GuiButton(tag='C', command=self.button_c, button_text="Button C", event_bindings=[("<Button-1>", self.button_b)], **kw)
        #
        # self.btn_d = GuiButton(tag='D', command=self.button_d, button_text="", **kw)
        #
        # self.btn_e = GuiButton(tag='E', command=self.button_e, button_text="", **kw)
        #
        # self.btn_f = GuiButton(tag='F', command=self.button_f, button_text="", **kw)
        #
        # self.btn_g = GuiButton(tag='G', command=self.button_g, button_text="", **kw)
        #
        # self.btn_h = GuiButton(tag='H', command=self.button_h, button_text="", **kw)
        #
        # self.btn_i = GuiButton(tag='I', command=self.button_i, button_text="", **kw)
        #
        # self.btn_j = GuiButton(tag='J', command=self.button_j, button_text="", **kw)

        self.listbox.grid(row=0, column=0)
        if grid_kwargs is not None:
            self.grid(**grid_kwargs)
        # window.pack()
        
    def set_label_text(self, text: str):
        """ Alter the text in the top left label. Typically done to update the current mode that is displayed. """
        self.label.set_text(text)
        
    def button_b(self, e=None, **kwargs):
        for k, v in kwargs.items():
            print(k, v)
        
    def button_c(self, e=None):
        ...
        
    def button_d(self, e=None):
        ...
        
    def button_e(self, e=None):
        ...
        
    def button_f(self, e=None):
        ...
        
    def button_g(self, e=None):
        ...
        
    def button_h(self, e=None):
        ...
        
    def button_i(self, e=None):
        ...
        
    def button_j(self, e=None):
        ...
    
    def sort_listbox(self):
        self.df.sort_values()
        
    @property
    def listbox_columns(self):
        return [
            # ListboxColumn.make("item_id", 30, lambda s: shorten_string(s, 29), "item_name", self.sort_listbox),
            # ListboxColumn.make("item_id", 7, lambda s: str(s), "item_id", self.sort_listbox, True, True),
            listbox_column.ITEM_NAME,
            listbox_column.TIMESTAMP,
            listbox_column.PRICE,
            listbox_column.QUANTITY,
            listbox_column.VALUE
        ]
        

if __name__ == "__main__":
    db = Database(gp.f_db_local)
    data = db.execute("SELECT * FROM 'transaction' WHERE value > 0 ORDER BY timestamp ASC", factory=dict).fetchall()
    for el in data:
        if el['value'] == 0:
            continue
        print(el)
    exit(1)
    keys = tuple(data[0].keys())
    dtypes = {c: types.get(c).df.replace('U', '') for c in keys}
    pd.DataFrame(data=data, columns=keys).astype(dtypes).to_pickle(os.path.join(gp.dir_data, "test_df.dat"))
    for column, dtype in dtypes.items():
        print(column, dtype)
    