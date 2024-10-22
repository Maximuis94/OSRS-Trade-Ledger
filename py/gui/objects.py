"""
This module contains blueprint-like implementations for (t)tk widgets used throughout the project. All objects capture
behaviour and properties that were found to be overlapping for implementations of specific tk widgets. In almost every
case one GuiObject is actually composed of multiple widgets. The complexity of these widgets varies from placing a
simple label to deploying a listbox that can be filled and sorted in a plug-and-play like fashion.
Each object will be fully defined and ready by executing logic found in the constructor.


"""
import time
import tkinter as tk
from tkinter import ttk

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

import gui_graph
import path as p
import ts_util
from model_item import NpyArray
from filter import df_filter_num, df_filter_str, filter_df, get_operators
from ge_util import parse_integer, parse_dt_str, remap_item
from global_values import npyar_items, id_name, name_id, delta_t_utc
from graphs import PricesGraph, price_graph_by_dow
from graph_util import get_vplot_timespans, major_format_price_non_abbreviated
from gui_formats import format_listbox_row, value_formats, rgb_to_colorcode
from ledger import Ledger
from local_files import get_target_prices
from path import save_data
from str_formats import format_ts, format_n, shorten_string, format_is_buy, format_y_m, ts_to_dow
from ts_util import dt_to_ts, ts_to_dt

matplotlib.use('TkAgg')
# from matplotlib.backends._backend_tk import NavigationToolbar2
# from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg #, NavigationToolbar2Tk
try:
    # NavigationToolbar2TkAgg was deprecated in matplotlib 2.2 and is no
    # longer present in v3.0 - so a version test would also be possible.
    from matplotlib.backends._backend_tkagg import FigureCanvasTk, NavigationToolbar2Tk
except ImportError:
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
except:
    raise


"""
All classes defined in this file are used to greatly reduce the amount of code per GUI class. All classes have similar 
implement behaviour and are ready to use after creating an instance of the object.

Coordinates and sizes (x, y, w, h) are defined using the TkGrid class in this file
"""


# This method is designed to simplify defining widget grids in tkinter
# The input is a list of H strings (each string is a row, H being the height of the grid);
# each string in the list should be of equal length W (the width of the grid).
# Example:
# r0 = 'AAA***BBB'
# r1 = 'AAACCC***'
# r2 = 'DDDDDDD**'
# Each character represents a unique widget, * is an empty cell, which makes this is a grid with 4 widgets on it
# Feeding these rows to read_grid (read_grid([r1, r2, r3])) will produce the following output:
# {'A': {'xy': (0, 0), 'wh': (3, 2)},
# 'B': {'xy': (6, 0), 'wh': (3, 1)},
# 'C': {'xy': (3, 1), 'wh': (3, 1)},
# 'D': {'xy': (0, 2), 'wh': (7, 1)}}
# This method will only work properly if each object in it has a rectangular shape
# The output of this method can be used as xy and wh for each widget
class TkGrid:
    def __init__(self, grid: list):
        """
        An alternative approach for representing tk elements in a grid-like fashion
        Object for automatically creating a grid-like representation for tkinter elements. The input is a list of Y
        strings of length X, tkinter elements are tagged with single characters (e.g. ['AABB', 'CCCC']). A grid-like
        representation is derived from this list of strings, in which each string within the list represents a row and
        the amount of characters per row can be derived to the width of the smallest atom.
        There is also a wild-card character * that can be used for filling unassigned spaces.

        Parameters
        ----------
        grid : list
            List of Y strings of equal length X, where identical characters dictate how much space is assigned to this
            tag

        Attributes
        ----------
        grid : dict
            A dict with a key for each single character tag passed in the input list of strings. Each dict contains a
            (x, y) tuple and a (w, h) tuple.

        Methods
        -------
        xy(char)
            Short notation for fetching the x, y coordinate tuple of the element with tag char
            
        wh(char)
            Short notation for fetching the w, h dimensions tuple of the element with tag char
        
        xywh(char)
            Short notation for fetching the x, y, w, h tuple of the element with tag char
        
        
        Raises
        ------
        ValueError
            A ValueError will be raised if the strings within the list have varying lengths.
            
        Notes
        -----
        Given a row of AAABB, 3/5th of the space will be assigned to tk widget A on the left, and the remaining 2/5th
        to tk widget B on the right. This system was implemented to automatically assign proper X, Y, W and H
        coordinates, without having to tweak coordinates/dimensions for individual widgets. Note that each abstract
        object defined below can accept a TkGrid object used to derive its coordinates and size without having to
        explicitly define each one of them.
        """
        
        encountered, temp, self.grid = [], {}, {}
        for y in range(len(grid)):
            for x in range(len(grid[0])):
                char = grid[y][x]
                if char not in encountered and char != '*':
                    w = sum([1 if c == char else 0 for c in grid[y]])
                    temp[char] = {'pos': (x, y), 'w': w, 'h': 1}
                    encountered.append(char)
                elif char in encountered:
                    temp[char]['h'] = temp.get(char).get('h') + 1
        for char in encountered:
            self.grid[char] = {'xy': temp.get(char).get('pos'),
                               'wh': (temp.get(char).get('w'), temp.get(char).get('h') // temp.get(char).get('w'))}

    def xy(self, char):
        return self.grid.get(char).get('xy')

    def wh(self, char):
        return self.grid.get(char).get('wh')

    def xywh(self, char):
        return self.xy(char), self.wh(char)


# Tested; class to automatically fully setup some tk widgets, reduces the amount of code a LOT
# frame is the frame the label should be placed on
# default_str is the string that the label should display
# xy is a tuple of (x, y) coordinates that specify where the label should be placed on the grid of frame
# wh is a tuple of (width, height) that determines the width and the height of the label on thr grid of frame
# Sticky determines to which edge of the grid cells the label is placed in it will stick
# textvariable is can be passed if a specific textvariable should be bound to the label
# events_bindings is (a list of) tuple(s) of (event, method). If event is triggered, method will be called.
#   See https://effbot.org/tkinterbook/tkinter-events-and-bindings.htm for more information on this
class GuiLabel(tk.Label):
    def __init__(self, frame, text='', xy=(0, 0), wh=(1, 1), padxy=(0, 0), sticky='N', textvariable=None,
                 event_bindings=None, font=('Helvetica', 10), width=None, grid: list or tuple = None,
                 justify: str = 'left'):
        """ Class for setting up tk label widget.
        A tk.Label built on top of the given frame with frequently used attributes, used to define tk elements in a
        standardized fashion. Commonly tweaked parameters can be passed as well. All parameters with the exception of
        grid correspond to attributes used in original tk objects.

        Parameters
        ----------
        frame : ttk.Frame
            The ttk frame on which the Label will be placed.

        Attributes
        ----------
        frame : ttk.Frame
            Frame on which the object will be placed

        Methods
        -------
        set_text(string)
            Method for changing the text displayed by the label
        """
        if grid is not None:
            xy, wh = grid
            assert len(xy) == 2 and len(wh) == 2
        self.frame = frame
        if not isinstance(event_bindings, list) and event_bindings is not None:
            event_bindings = [event_bindings]
        elif event_bindings is None:
            event_bindings = []
        if not isinstance(textvariable, tk.StringVar):
            textvariable = tk.StringVar(self.frame)
        self.text = textvariable
        self.text.set(text)
        super().__init__(self.frame, textvariable=self.text, font=font, width=width, justify=justify)
        for binding in event_bindings:
            # print(binding)
            self.bind(binding[0], binding[1])
        self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky=sticky)

    def set_text(self, string):
        """ Display the given string in """
        self.text.set(string)


# Clickcommand is the method that is called when the button is pressed.
class GuiButton(tk.Button):
    def __init__(self, frame, command, text='', xy=(0, 0), wh=(1, 1), padxy=(0, 0), sticky='N', textvariable=None,
                 variable=None, event_bindings=None, width=None, grid: list or tuple = None):
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
        if grid is not None:
            xy, wh = grid
            assert len(xy) == 2 and len(wh) == 2
        self.frame = frame
        if not isinstance(event_bindings, list) and event_bindings is not None:
            event_bindings = [event_bindings]
        elif event_bindings is None:
            event_bindings = []
        if not isinstance(textvariable, tk.StringVar):
            textvariable = tk.StringVar(self.frame)
        if not isinstance(variable, tk.BooleanVar):
            variable = tk.BooleanVar(self.frame)
            variable.set(True)
        self.variable = variable
        self.text = textvariable
        self.text.set(text)
        super().__init__(self.frame, textvariable=self.text, command=command, width=width)
        for binding in event_bindings:
            # print(binding)
            self.bind(binding[0], binding[1])
        self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky=sticky)


# Checking the button will correspond with a boolean status; This means statusvar should be a tk.Booleanvar()
# Clickedcommand is the function to bind to the checkbutton when it's clicked. In fact this is already included in
# every class here in the form of event_bindings, but since it's more conventional to use this for a checkbutton, it was
# added as a separate argument as well.
class GuiCheckbutton(tk.Checkbutton):
    def __init__(self, frame, variable=None, command=None, text='', initial_state=None,
                 xy=(0, 0), wh=(1, 1), padxy=(0, 0), sticky='N', textvariable=None, event_bindings=None,
                 grid: list or tuple = None):
        if grid is not None:
            xy, wh = grid
            assert len(xy) == 2 and len(wh) == 2
        self.frame = frame
        if variable is None:
            self.status = tk.BooleanVar()
            self.status.set(True)
        else:
            self.status = variable
        if initial_state is not None:
            self.status.set(initial_state)
        if not isinstance(event_bindings, list) and event_bindings is not None:
            event_bindings = [event_bindings]
        elif event_bindings is None:
            event_bindings = []
        if not isinstance(textvariable, tk.StringVar):
            textvariable = tk.StringVar(self.frame)
        self.text = textvariable
        self.text.set(text)
        super().__init__(self.frame, textvariable=self.text, variable=self.status, onvalue=True, offvalue=False)
        if command is not None:
            self.bind('<Button-1>', command)
        for binding in event_bindings:
            # print(binding)
            self.bind(binding[0], binding[1])
        self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky=sticky)

    def get(self):
        return self.status.get()

    def set(self, new_status: bool):
        self.status.set(new_status)


# Statusvar is the tk variable that carries the current value (depending on which radiobutton is checked)
# Value is the value of statusvar that corresponds with this specific radiobutton; it it's 1, statusvar will be set to
# 1 if this radiobutton is checked
# As radiobuttons are designed to be used in sets carrying a common statusvariable, providing one is mandatory
class GuiRadiobutton(tk.Radiobutton):
    def __init__(self, frame, variable, value, command=None, text='', xy=(0, 0), wh=(1, 1), padxy=(0, 0), sticky='N',
                 textvariable=None, event_bindings=None, grid: list or tuple = None):
        if grid is not None:
            xy, wh = grid
            assert len(xy) == 2 and len(wh) == 2
        self.frame = frame
        if not isinstance(event_bindings, list) and event_bindings is not None:
            event_bindings = [event_bindings]
        elif event_bindings is None:
            event_bindings = []
        if not isinstance(textvariable, tk.StringVar):
            textvariable = tk.StringVar(self.frame)
        self.status = variable
        self.text = textvariable
        self.text.set(text)
        super().__init__(self.frame, textvariable=self.text, value=value, variable=self.status, command=command)
        for binding in event_bindings:
            self.bind(binding[0], binding[1])
        self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky=sticky)


# The text in the entry field can be modified or acquired with self.text.set(str) or self.text.get()
class GuiEntry(tk.Entry):
    def __init__(self, frame, text='', width=10, xy=(0, 0), wh=(1, 1), padxy=(0, 0), sticky='N', textvariable=None,
                 event_bindings=None, grid: list or tuple = None):
        if grid is not None:
            xy, wh = grid
            assert len(xy) == 2 and len(wh) == 2
        self.frame = frame
        if not isinstance(event_bindings, list) and event_bindings is not None:
            event_bindings = [event_bindings]
        elif event_bindings is None:
            event_bindings = []
        if not isinstance(textvariable, tk.StringVar):
            textvariable = tk.StringVar(self.frame)
        self.text = textvariable
        self.text.set(text)
        super().__init__(self.frame, textvariable=self.text, width=width)
        for binding in event_bindings:
            # print(binding)
            self.bind(binding[0], binding[1])
        self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky=sticky)


# The text in the combobox can be modified or acquired with self.text.set(str) or self.text.get()
class GuiCombobox(ttk.Combobox):
    def __init__(self, frame, values, text='', width=10, xy=(0, 0), wh=(1, 1), padxy=(0, 0), sticky='N',
                 textvariable=None, event_bindings=None, grid: list or tuple = None):
        if grid is not None:
            xy, wh = grid
            assert len(xy) == 2 and len(wh) == 2
        self.frame = frame
        if not isinstance(event_bindings, list) and event_bindings is not None:
            event_bindings = [event_bindings]
        elif event_bindings is None:
            event_bindings = []
        if not isinstance(textvariable, tk.StringVar):
            textvariable = tk.StringVar(self.frame)
        self.text = textvariable
        self.text.set(text)
        super().__init__(self.frame, textvariable=self.text, width=width)
        for binding in event_bindings:
            # print(binding)
            self.bind(binding[0], binding[1])
        self['values'] = list(values)
        self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky=sticky)

    def update_values(self, values):
        self['values'] = values


class ListboxColumn:
    def __init__(self, header: str, width: int, format: callable = None, df_column=None, button_click=None,
                 visible: bool = True, is_number: bool = True, push_left: bool = False):
        """ Class used for standardized formatting of listboxes
        
        A listbox column is an object that serves as an interface between the listbox dataframe and the listbox itself.
        It contains logic on how to present data, but also on how to interact with data. ListboxColumns used in
        ListboxDF objects are defined in static_values and grouped as a dict for each specific dataframe.
        
        :param header: String to use as column header in the listbox, e.g. 'Item name' instead of 'item_name'
        :param width: column width
        :param format: Method that dictates how values should be displayed in a listbox entry
        :param push_left: True if the formatted string should be pushed to the left, False for right (def=True)
        :param df_column: Identifier for the corresponding dataframe column. Should be identical to the corresponding
        df column name.
        :param button_click: Method to execute upon clicking the header button
        :param visible: True if the column should be displayed in the listbox (entries), False to hide it
        :param is_number: Type of the values this column can hold; num or str (refers to respectively numerical and
        textual values)
        """
        self.header = header
        self.df_column = header.replace(' ', '_').lower() if df_column is None else df_column
        self.width = width
        self.visible = visible
        self.value_format = format if format is not None else lambda x: value_formats.get(self.df_column)(x)
        # if self.value_format is None:
        #     self.value_format = value_formats.get(df_column)
        #     if self.value_format is None:
        #         self.value_format = shorten_string
        #         print(f"No value_format has been implemented for {self.df_column}...")
        self.button_click = button_click
        self.value_type = is_number
        self.push_left = push_left

    def get_value(self, x, print_warning: bool = True):
        """ Format value x according to the configurations of this ListboxColumn """
        try:
            f = f"{self.value_format(x): {'<' if self.push_left else '>'}{self.width-1}} " if self.visible else ""
            if len(f) > self.width and print_warning:
                print(f'Formatted value for column {self.header} exceeded configured width {self.width} (width={len(f)})')
            return f[:self.width]
        except TypeError:
            print(x)
            return x
    
    def apply_df_filter(self, filter: tuple, df: pd.DataFrame):
        """ Filter the given dataframe; apply the filter tuple to the `df_column` DataFrame column values. """
        column, operator, value = filter
        if self.value_type == 'num':
            return df_filter_num(df=df, c=column, o=operator, v=value)
        if self.value_type == 'str':
            return df_filter_str(df=df, c=column, o=operator, v=value)


# Class for automatically creating and placing a ttk listbox.
# The listbox also has a top label, a scrollbar and a bottom label (therefore, this class is a ttk Frame instead of a
# ttk Listbox).
# Entries is a list of entries the listbox should be filled with
# Entry width is the width of a listbox entry in characters
# listbox_height is the maximum amount of entries shown in the listbox at a time
# top_label_text is a string to be placed above the listbox
# bottom_label_text is a string to be placed below the listbox
# xy is the position (column, row) of this frame in the parent frame
# wh is the width and height of this frame in terms of grid cells of the parent frame
# Padxy are the paddings of this frame within the grid of the parent frame
# Sticky is the edge of the grid cells in the parent frame this frame will stick to
class GuiListboxFrame(ttk.Frame):
    def __init__(self, frame, df: pd.DataFrame = None, entry_width=20, listbox_height=10, top_label_text='',
                 xy=(0, 0), wh=(1, 1), padxy=(0, 0), sticky='N', header_button_callback=None, bottom_label_text='',
                 event_bindings=None, select_mode=tk.SINGLE, font=('Monaco', 10), columns: list or tuple = None,
                 grid: list or tuple = None, default_sort: tuple = (0, True), filter: callable = None):
    
        """ Class for setting up a listbox
        With the most minimal input, the listbox consists of a scrollbar and a listbox with a label above and below it.
        

        Parameters
        ----------
        frame : ttk.Frame
            The ttk frame on which the Label will be placed.

        df : pandas.DataFrame
            DataFrame that will be used to draw entries from
        
        header_button_callback : callable
            Callback method to use if one of the header buttons is pressed
            
        top_label_text : str
            String to print above the listbox
            
        bottom_label_text : str
            String to print below the listbox
            
        columns : list
            A list of ListboxColumns that dictates how the rows should be formatted. The used columns should have a
            reference (`df_column`) to an existing pandas DataFrame column used as df.
        
        default_sort : tuple
            A tuple indicating the sorting method to use by default. The first element refers to the index of the
            ListboxColumn to use as sorting value, the second one is a flag indicating an ascending sorting order or not
        
        

        Attributes
        ----------
        frame : ttk.Frame
            Frame on which the object will be placed

        Methods
        -------
        set_text(string)
            Method for changing the text displayed by the label
        """
        if grid is not None:
            xy, wh = grid
            assert len(xy) == 2 and len(wh) == 2
        self.n_entries = 0
        self.frame = frame
        if not isinstance(event_bindings, list) and event_bindings is not None:
            event_bindings = [event_bindings]
        elif event_bindings is None:
            event_bindings = []
        super().__init__(self.frame)
        if top_label_text is not None:
            self.top_label = GuiLabel(self, text=top_label_text, xy=(0, 0), wh=(2, 1), padxy=padxy, sticky=sticky, font=font)
        self.scrollbar = ttk.Scrollbar(self, orient="vertical")
        self.scrollbar.grid(row=1, column=entry_width, sticky="NS")
        self.listbox = tk.Listbox(self, width=entry_width, height=listbox_height, font=font, selectmode=select_mode,
                                  yscrollcommand=self.scrollbar.set)
        self.listbox.grid(row=1, column=0, columnspan=entry_width, sticky="NWE")
        self.scrollbar.config(command=self.listbox.yview)
        self.button_panel = ttk.Frame(self, width=entry_width)
        self.button_panel_buttons = None
        if bottom_label_text is not None:
            self.bottom_label = GuiLabel(self, text=bottom_label_text, xy=(0, 2), wh=(2, 1), padxy=padxy, sticky=sticky)
        self.header, self.columns, self.button_ids = "", [], {}
        self.header_button_callback = header_button_callback
        self.entry_chars, self.width_scaling, self.entry_width, self.columns = None, None, entry_width, None
        self.column_indices = {}
        self.default_sort = default_sort
        self.submitted_entries = []
        self.df = pd.DataFrame() if df is None else df
        if columns is not None:
            self.columns = columns
            self.make_button_header()
            self.sort_by = [self.columns[0].df_column]
            self.sort_reverse = [False]
        else:
            self.sort_by = None
            self.sort_reverse = None
        # self.bottom_frame = ttk.Frame(self, width=entry_width)
        # self.setup_bottom_frame()
        
        for binding in event_bindings:
            self.listbox.bind(binding[0], binding[1])
        self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky=sticky)
        
    def insert_entry(self, index, entry, bgc_rgb: tuple = None):
        """ Insert an (un)formatted entry into the listbox at the given index, coloring it with the given rgb tuple """
        try:
            if not isinstance(entry, str):
                entry_str = " "
                for formatted_value in [self.columns[i].get_value(entry.as_tuple()[i]) for i in range(len(self.columns))]:
                    entry_str += formatted_value
                entry = entry_str
            # if bgc_rgb is not None:
                # print(index, 'rgb:', bgc_rgb)
            self.listbox.insert(index, entry)
            if bgc_rgb is not None:
                self.set_entry_bgc(index, colorcode=rgb_to_colorcode(bgc_rgb))
        except AttributeError:
            print("Attribute error!")
            print('\t', index)
            print('\t', entry)

    # Clear the listbox and fill it with entries from entries_list
    def fill_listbox(self, entry_list: list, columns: list, color_format: callable = None):
        """
        Fill the listbox with the entries. Format them as dictated by the columns and the color format, if applicable.
        :param entry_list: List of rows (pd.Dataframe.to_dict('records')
        :param columns: List of columns the listbox consists of
        :param color_format: Method used to compute the background color of the row.
        :return:
        """
        self.clear_listbox()
        for idx, e in zip(range(len(entry_list)), entry_list):
            row = {c.df_column: e.get(c.df_column) for c in columns if e.get(c.df_column) is not None}
            rgb = color_format(row) if color_format is not None else None
            # rgb = color_format(entry_list.index(row)) if color_format is not None else None
            self.submitted_entries.append(row)
            self.insert_entry(idx, entry=format_listbox_row(row=e, listbox_columns=columns), bgc_rgb=rgb)
        return

    # Configure the background colour of entry with the specified index
    # A colorcode can be given or an RGB tuple (RRR, BBB, GGG) (R, B, G are 0 <= R/B/G <= 255)
    # If an RGB tuple is given, the color code is ignored
    def set_entry_bgc(self, index, colorcode='', rgb=None):
        if rgb is not None:
            colorcode = rgb_to_colorcode(rgb)
        # print(colorcode)
        self.listbox.itemconfig(index, bg=colorcode)

    # Delete an entry on a specific index OR delete a specific entry (and figure out the index
    # If an entry and an index are given, the entry will be deleted and the index ignored
    def delete_entry(self, index=-1, entry=''):
        if entry != '':
            current_index, index = 0, -1
            entries = self.listbox.get(0, tk.END)
            while current_index < len(entries) and index == -1:
                next_entry = entries[current_index]
                if next_entry == entry:
                    index = current_index
                else:
                    current_index += 1
        if index != -1:
            self.listbox.select_clear(index)
            self.n_entries -= 1
        else:
            print("Unable to find entry {e} in the listbox...".format(e=entry))

    # Delete all entries in the listbox
    def clear_listbox(self):
        self.n_entries = 0
        self.listbox.delete(0, tk.END)
        self.submitted_entries = []

    # Set the text of the label above the listbox
    def set_top_text(self, string, side: str = 'left', char_lim: int = -1):
        char_lim = len(string) if char_lim == -1 else str(char_lim)
        side = '<' if side == 'left' else '>' if side == 'right' else '^' if side == 'center' else ''
        self.top_label.set_text(string + (int(char_lim)-len(string))*' ')

    # Set the text of the label below the listbox
    def set_bottom_text(self, string):
        self.bottom_label.set_text(string)

    # Return a list with all indices of selected entries in the listbox
    def selected_indices(self):
        return self.listbox.curselection()

    # Return a list with all indices of selected entries in the listbox
    def get_selected_entries(self):
        return (self.listbox.get(i) for i in self.selected_indices())

    def make_button_header(self, entry_spacing: int = 1, default_sort: tuple = None):
        if isinstance(self.columns, dict):
            self.columns = [self.columns.get(c) for c in list(self.columns.keys()) if self.columns.get(c).visible]
        if False in [isinstance(self.columns, list)] + [isinstance(c, ListboxColumn) for c in self.columns]:
            raise TypeError("Error initializing button header; columns should be a list of properly initialized "
                            "ListboxColumns. ")
        self.entry_chars = sum([c.width + entry_spacing for c in self.columns])-entry_spacing
        self.width_scaling = 1 #self.entry_width / self.entry_chars
        self.entry_width = self.entry_chars
        if self.button_panel_buttons is not None:
            for b in self.button_panel_buttons:
                if isinstance(b, GuiButton):
                    b.destroy()
        x, w_sum, self.header, self.button_panel_buttons, self.button_ids = 0, 0, " ", [], {}
        buttongrid, button_grid_ids, column_grid_ids = '', "ABCDEFGHIJKLMNOPQRSTUVWXYZ", {}
        self.column_indices = {}
        for c in self.columns:
            # print(c.df_column)
            if c.visible:
                # print(c.__dict__)
                w = int(c.width / self.entry_chars * self.entry_width)
                w_sum += w
                self.header += f"{c.header: ^{c.width}} "
                b = GuiButton(self.button_panel, command=None, event_bindings=('<Button-1>', self.button_click),
                              text=c.header, width=w, xy=(x, 0), sticky='WE')
                self.button_panel_buttons.append(b)
                key = str(b.info).split('!')[-1].replace('>', '')
                self.button_ids[key] = c.df_column
                buttongrid += button_grid_ids[x]*w
                column_grid_ids[c.header] = button_grid_ids[x]
                x += 1
                self.column_indices[c.df_column] = self.columns.index(c)
                if default_sort is None:
                    self.default_sort = c.df_column, True
                    default_sort = self.default_sort
        grid, x = TkGrid([buttongrid]), 0
        # for c in columns:
        #     if c.visible:
        #         x += 1
        self.button_panel.grid(row=0, column=0, sticky='NW')
        # time.sleep(5)
        
        # Set default sort for this button panel. default_sort should be a tuple (column, bool(reverse))
        # The first value can be its index in the list of columns or the df_column value.
        
        # print(self.default_sort)
        
    def header_button_default(self, e=None):
        # self.header_button_callback(e)
        # return
        header_id = self.button_ids.get(f"{str(e.__dict__.get('widget')).split('!')[-1]}")
        sort_by = self.sort_by
        self.sort_by = header_id
        self.sort_reverse = False if self.sort_by != sort_by else not self.sort_reverse
        self.df = self.df.sort_values(by=[sort_by], ascending=[self.sort_reverse])
        self.fill_listbox(self.df, columns=self.columns)
        
        
    def button_click(self, e=None):
        # print(self.button_ids.get(f"{str(e.__dict__.get('widget')).split('!')[-1]}"))
        # self.header_button_default(e)
        self.header_button_callback(self.button_ids.get(f"{str(e.__dict__.get('widget')).split('!')[-1]}"))
        
    def sort_df(self, df=None, sort_by: list = None, reverse_sort: list = None):
        if df is not None:
            self.df = df
        
        try:
            if self.sort_reverse is None:
                self.sort_reverse = False
            self.df = self.df.sort_values(by=self.sort_by if sort_by is None else sort_by,
                                          ascending=self.sort_reverse if reverse_sort is None else reverse_sort,
                                          ignore_index=True)
            return self.df
        except KeyError:
            return self.df.sort_values(by=[self.df.column_list[0]], ascending=[True], ignore_index=True)
        
    def update_sort(self, var, reverse):
        if isinstance(var, int):
            self.sort_by, self.sort_reverse = [self.columns[var].df_column], [reverse]
        elif isinstance(var, str):
            self.sort_by, self.sort_reverse = [var], [reverse]
        elif isinstance(var, list):
            self.sort_by, self.sort_reverse = var, reverse
        if self.df is not None:
            self.df = self.df.sort_values(by=var, ascending=reverse)
            self.fill_listbox(self.df.to_dict('records'), columns=self.columns)
            return self.df.to_dict('records')

    # Code that will be executed when the user clicks on an entry in the listbox
    @staticmethod
    def clicked_entry(event):
        w = event.widget
        idx = int(w.curselection()[0])


class GuiListboxFilterPanel(ttk.Frame):
    def __init__(self, frame, update_callback, df: pd.DataFrame, columns: dict, default_sort: tuple,
                 grid: list or tuple, padxy=(0, 0), custom_apply: callable = None, hide_frame: bool = False):
        """
        A GUI Frame for extending a listbox with filter/sort capabilities. The filter panel is designed to use with
        pandas dataframes, and therefore assumes that the listbox entries are derived from a dataframe.
        :param frame: The (t)tk frame on which the filter/sort panel should be placed
        :param update_callback: Method that will be called after updating active sorts/filters. This arg was added to
        filter/sort the corresponding dataframe after modifying active filters/sorts.
        :param df: pandas dataframe from which the entries are drawn
        :param padxy:
        :param grid:
        :param custom_apply:
        :param hide_frame: True if the Frame should remain hidden. It still exists, however.
        """
        self.frame = frame
        self.df = df
        self.hide_frame = hide_frame
        self.columns = columns
        self.column_ids = list(self.columns.keys())
        self.default_sort, self.sort_1, self.sort_2 = default_sort, default_sort, None
        self.active_filters = {}
        self.callback = update_callback
        self.custom_filter_sort = custom_apply
        cbox_width = None
        cbtn_event = [('<Enter>', self.callback)]
        sort_cbox_event = ([('<<ComboboxSelected', self.callback)])
        
        super().__init__(self.frame, padding=padxy)
        g = TkGrid(grid=['AD*',  # Label top
                         'BC*',  # Label column, drop-down column, label negate
                         'EF*',  # Label operator, drop-down operator, checkbox negate
                         'HI*',  # Label filter value, entry filter value
                         '*G*',
                         'JK*',  # Button add_filter, button clear_filters
                         'LLL',
                         'MN*', # drop-down active filters, button remove_filter
                         'OP*',
                         'XQR',# label sorting, label reverse
                         '*S*',  # drop-down primary sort, checkbox reverse, button reset primary sort
                         'YTU',
                         '*V*'  # drop-down secondary sort, checkbox reverse, button remove secondary sort
                         ])
        pad = (3, 5)
        self.lb_header_top = GuiLabel(self, text='Filters',
                                      xy=g.xy('A'), wh=g.wh('A'), sticky='W', font=('Helvetica', 16), justify='left',
                                      padxy=pad)
        
        self.lb_column = GuiLabel(self, text='Filter column:', xy=g.xy('B'), wh=g.wh('B'), sticky='E')
        self.cbox_column = GuiCombobox(self, values=self.column_ids, xy=g.xy('C'), wh=g.wh('C'), sticky='WE',
                                      # width=cbox_width,
                                       event_bindings=[("<<ComboboxSelected>>", self.cbox_column_set)])
        
        # help_text =
        # self.btn_help = GuiButton(self, text='Filter/sort help', xy=g.xy('D'), wh=g.wh('D'), sticky='E', padxy=pad,
        #                           command=self.explanation_popup)
        self.btn_help = GuiButton(self, text='Filter/sort help', xy=g.xy('D'), wh=g.wh('D'), sticky='E', padxy=pad,
                                  command=self.parse_input)
        
        self.lb_operator = GuiLabel(self, text='Operator:', xy=g.xy('E'), wh=g.wh('E'), sticky='E')
        self.cbox_operator = GuiCombobox(self, values=[], xy=g.xy('F'), wh=g.wh('F'), sticky='WE')#, width=cbox_width)
        self.cbtn_negate = GuiCheckbutton(self, initial_state=False, xy=g.xy('G'), wh=g.wh('G'), sticky='W',
                                          text='Negate operator')
        
        self.lb_filter_value = GuiLabel(self, text='Filter value:', xy=g.xy('H'), wh=g.wh('H'), sticky='E')
        self.en_filter_value = GuiEntry(self, xy=g.xy('I'), wh=g.wh('I'), sticky='WE')
        
        pad = (6, 2)
        self.btn_add_filter = GuiButton(self, xy=g.xy('J'), wh=g.wh('J'), sticky='WE', #width=10,
                                        command=self.add_filter,
                                        padxy=pad, text='Add filter')
        self.btn_clear_filter = GuiButton(self, xy=g.xy('K'), wh=g.wh('K'), sticky='WE', #width=10,
                                          command=self.clear_filters, padxy=pad, text='Clear filters')
        
        pad = (3, 2)
        self.lb_active_filters = GuiLabel(self, text='Active filters (0):', xy=g.xy('L'), wh=g.wh('L'), sticky='W',
                                          padxy=pad, justify='left')
        self.cbox_active_filters = GuiCombobox(self, text='', values=[],
                                               xy=g.xy('M'), wh=g.wh('M'), sticky='WE', padxy=pad)#, width=cbox_width)
        self.btn_remove_filter = GuiButton(self, xy=g.xy('N'), wh=g.wh('N'), sticky='W', command=self.remove_filter,
                                           padxy=pad, text='Remove filter')
        
        self.lb_sorting = GuiLabel(self, text='Sorting', xy=g.xy('O'), wh=g.wh('O'), sticky='W', font=('Helvetica', 14))
        self.lb_reverse = GuiLabel(self, text='Column + reverse', xy=g.xy('P'), wh=g.wh('P'), sticky='E',
                                   justify='right')
        
        pad = (2, 4)
        self.lb_sort_primary = GuiLabel(self, text='Primary', xy=g.xy('X'), wh=g.wh('X'), sticky='E', justify='right',
                                        padxy=pad)
        self.cbox_sort_primary = GuiCombobox(self, values=list(self.columns.keys()), text=self.default_sort[0],
                                             xy=g.xy('Q'), wh=g.wh('Q'), sticky='WE')#, width=cbox_width)
        self.cbtn_neg_primary = GuiCheckbutton(self, initial_state=self.default_sort[1], xy=g.xy('R'), wh=g.wh('R'),
                                               sticky='W', padxy=pad,
                                               event_bindings=cbtn_event)
        self.btn_reset_primary = GuiButton(self, text='Reset primary', xy=g.xy('S'), wh=g.wh('S'), sticky='WE',
                                           command=self.reset_primary_sort)
        
        self.lb_sort_secondary = GuiLabel(self, text='Secondary', xy=g.xy('Y'), wh=g.wh('Y'), sticky='E',
                                          justify='right', padxy=pad)
        self.cbox_sort_secondary = GuiCombobox(self, text='', values=list(self.columns.keys()),
                                               xy=g.xy('T'), wh=g.wh('T'), sticky='WE', padxy=pad)#, width=cbox_width)
        self.cbtn_neg_secondary = GuiCheckbutton(self, initial_state=False, xy=g.xy('U'), wh=g.wh('U'), sticky='W',
                                                 event_bindings=cbtn_event, padxy=pad)
        self.btn_remove_secondary = GuiButton(self, text='Remove secondary', xy=g.xy('V'), wh=g.wh('V'), sticky='WE',
                                              command=self.remove_secondary_sort)#, width=15)
        if not self.hide_frame:
            xy, wh = grid
            self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky='NES')
    
    def explanation_popup(self):
        t = "FILTERS\n" \
            "The first set of input fields in this panel can be used to configure filters for the listbox.\n" \
            "- column_name refers to the value type that the filter should apply to, e.g. price/item_name\n" \
            "- operator refers to the logical operator that should be applied to the listbox entries\n" \
            "- filter_value refers to the value that the operator is applied on\n" \
            "- negate can be used to flip the resulting truth value\n" \
            "\te.g.: ('price', '>', 3000) will result in entries with a price higher than 3000 to be omitted.\n\n" \
            "SORTING\n" \
            "Sorting only requires a column_name and the reverse flag that will reverse the standard sort order\n" \
            "One or two columns can be assigned as sort column (primary/secondary sort).\n" \
            "The primary sort is also operated by the button header above the listbox\n" \
            "The secondary sort is optional and can be configured with this panel\n" \
            "\te.g. 'item_name', True will result in listbox entries sorted by item name in reversed alphabetical " \
            "ordern\n\n" \
            "USAGE\n" \
            "- Comboboxes: Restrict the given input to something from the list\n" \
            "- Filters: First select a column (this determines which operators to choose from), then an " \
            "operator/value\n" \
            "- Filter_value: make sure the filter value is valid for this column. E.g. no str filter value for a " \
            "column with numerical data.\n" \
            "- Primary sort: probably easier to configure using a button header if present"
        
        popup_msg(window_text=t, window_title='filter/sort panel help', window_size=(720, 250),
                  font=('Helvetica 11'))
    
    def cbox_column_set(self, e):
        """
        Update the operators combobox values with operators that correspond to the value type of the column that has
        been set, e.g. numerical operators are not applicable for string values.
        :return:
        """
        print("New column was set for filter, updating corresponding operators...")
        c = self.cbox_column.text.get()
        vt = self.columns.get(c).value_type
        self.lb_operator.text.set(f'Operator ({vt}):')
        self.cbox_operator.update_values(
            values=get_operators(value_type=vt))
    
    def add_filter(self):
        """
        Parse inputs, extract filter, add filter, reset inputs
        :return:
        """
        c, o, v = self.cbox_column.text.get(), self.cbox_operator.text.get(), self.en_filter_value.text.get()
        
        self.active_filters[f"Filter_{len(list(self.active_filters.keys())) + 1}: ('{c}', '{o}', '{v}')"] = c, o, v
        self.cbox_active_filters.update_values(list(self.active_filters.keys()))
        # Verify values before returning?
        self.cbox_column.text.set('')
        self.cbox_operator.text.set('')
        self.en_filter_value.text.set('')
        self.lb_active_filters.text.set(f"Active filters ({len(self.active_filters)}):")
        self.callback()
    
    def remove_filter(self):
        """
        Remove selected filter from active filters
        :return:
        """
        del self.active_filters[self.cbox_active_filters.text.get()]
        self.cbox_active_filters.update_values(list(self.active_filters.keys()))
        self.cbox_active_filters.text.set('')
        self.lb_active_filters.text.set(f"Active filters ({len(self.active_filters)}):")
        self.callback()
        return
    
    def clear_filters(self):
        """
        Remove all active filters
        :return:
        """
        self.active_filters = {}
        self.cbox_active_filters.update_values([])
        self.lb_active_filters.text.set(f"Active filters ({len(list(self.active_filters.keys()))}):")
        self.callback()
        return
    
    def reset_primary_sort(self):
        """
        Set primary sort to default value
        :return:
        """
        self.sort_1 = self.default_sort
        self.cbox_sort_primary.text.set(self.default_sort[0])
        self.cbtn_neg_primary.set(self.default_sort[1])
        self.callback()
    
    def remove_secondary_sort(self):
        """
        Remove secondary sort
        :return:
        """
        self.sort_2 = None
        self.cbox_sort_secondary.text.set('')
        self.callback()
    
    def parse_input(self):
        """
        Parse all submitted filter and sort inputs and return them as filter tuples and sort tuples;
        filter: (column, operator, value), sort: (column, reverse_sort)
        :return: list of parsed filter tuples, list of 1-2 sort tuples
        """
        filters = [self.active_filters.get(f) for f in list(self.active_filters.keys())]
        sorts = [self.sort_1]
        if self.sort_2 is not None:
            sorts += [self.sort_2]
        print(f'filters: {filters} | sorts: {sorts}')
        return filters, sorts
    
    def filter_and_sort(self, df: pd.DataFrame, primary_sort: str = None):
        """
        Return currently configured filters and sort orders. If a custom filter method is passed, it should accept
        :return:
        """
        # Header sort button was clicked...
        if primary_sort is not None:
            self.sort_1 = (primary_sort, not self.sort_1[1]) if self.sort_1[0] == primary_sort else (primary_sort, False)
            self.cbox_sort_primary.set(self.sort_1[0])
            self.cbtn_neg_primary.set(self.sort_1[1])
        else:
            if not self.cbox_sort_primary.text.get() in df.columns:
                raise ValueError(f"Primary sort is not a valid column...")
            self.sort_1 = (self.cbox_sort_primary.text.get(), self.cbtn_neg_primary.get())
            
        col_2 = self.cbox_sort_secondary.text.get()
        if col_2 in list(self.columns.keys()):
            self.sort_2 = (col_2, self.cbtn_neg_secondary.get())
        else:
            self.sort_2 = None
        sort_by, sort_ascending = [self.sort_1[0]], [not self.sort_1[1]]
        if self.sort_2 is not None:
            if self.sort_2[0] in df.columns:
                sort_by.append(not self.sort_2[0])
                sort_ascending.append(self.sort_2[1])
        
        for f in list(self.active_filters.keys()):
            df = filter_df(df=df, filter_tuple=self.active_filters.get(f))
        return df.sort_values(by=sort_by, ascending=sort_ascending)


class GuiListboxDF(ttk.Frame):
    def __init__(self, frame: ttk.Frame, df: pd.DataFrame, listbox_columns: dict, default_sort: tuple = None,
                 grid: list or tuple = ((0, 0), (1, 1)), remove_columns: list = None, padxy: tuple = (0, 0),
                 entry_bgc: callable = None, sticky: str = 'N', hide_filter_frame: bool = False,
                 listbox_entry_callback: callable = None, bottom_frame_setup: callable = None,
                 bottom_frame_args: dict = None, bottom_frame: ttk.Frame = None):
        """
        ttk Frame object designed to interact and represent information from its underlying dataframe.
        :param df: The full dataframe the listbox will draw its entries from
        :param listbox_columns: A dict with predefined ListboxColumn objects corresponding to the dataframe columns.
        Each column in the dataframe should have a ListboxColumn.
        :param default_sort: (Optional) A tuple with a listbox column name (as found in df.columns) and a boolean
        indicating whether to sort in reverse or not. (def=df.colums[0], False)
        :param remove_columns: A list of strings that indicate which columns should be removed from the dataframe before
        :param entry_bgc: Standard method for setting the background color of an entry while filling the listbox
        setting it as the listbox dataframe. Expected arg is a (r, g, b) tuple.
        :param hide_filter_frame: True to keep the filter frame hidden. The entries can still be sorted using the button
        :param listbox_entry_callback: Method to execute after clicking a listbox entry. Originally designed to fill the
        bottom frame of the listbox with data related to the entry. Listbox entry is passed as a dict in which the
        listbox entry index is included as well.
        header.
        """
        self.frame = frame
        super().__init__(self.frame, padding=padxy)
        if remove_columns is not None:
            if isinstance(remove_columns, str):
                remove_columns = [remove_columns]
            for c in remove_columns:
                del df[c]
        if default_sort is None:
            self.default_sort = (df.columns[0], False)
        else:
            if default_sort[0] not in df.columns or not isinstance(default_sort[1], bool):
                raise ValueError("Input error for default_sort while creating ListboxDF! default_sort should be a tuple"
                                 " with an existing dataframe column and a boolean to indicate reverse_sort."
                                 " E.g. (df.columns[0], True)")
            self.default_sort = default_sort
        self.df, self.df_filtered, self.entries = df, df, []
        self.columns = {col: listbox_columns.get(col) for col in df.columns}
        self.entry_string_generator = {c: self.columns.get(c).get_value for c in list(self.columns.keys())
                                       if self.columns.get(c).visible}
        
        # L: ListboxFrame, F: FilterFrame, E: EntryClickFrame
        self.tk_grid = TkGrid([
            'LLLLLLLFFF',
            'EEEEEEEFFF'
        ])
        
        self.selected_listbox_idx = -1
        self.selected_listbox_entry = {}
        self.frame_listbox = self.setup_listbox()
        
        self.hide_filter_frame = hide_filter_frame
        self.frame_filter = self.setup_filter()
        
        self.frame_button_header = None
        
        # self.frame_bottom = self.setup_bottom_frame(bottom_frame_setup)
        if bottom_frame is not None:
            self.frame_bottom = bottom_frame
            self.frame_bottom.frame = self
            xy, wh = self.tk_grid.xywh('E')
            self.frame_bottom.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=2, pady=2, sticky='S')
        self.listbox_entry_callback = listbox_entry_callback
        
        xy, wh = grid
        self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky=sticky)
    
    def add_column(self, column_name: str, values, listbox_column: ListboxColumn):
        self.df[column_name] = values
        self.columns[column_name] = listbox_column
        if listbox_column.visible:
            self.entry_string_generator[column_name] = listbox_column.get_value
            
    def setup_listbox(self) -> GuiListboxFrame:
        entry_width = sum([self.columns.get(c).width for c in list(self.columns.keys())])
        listbox = GuiListboxFrame(self, entry_width=entry_width, listbox_height=10, padxy=(2, 2), sticky='NW',
                                  grid=self.tk_grid.xywh('L'), font=("Monaco", 9),
                                  event_bindings=('<<ListboxSelect>>', self.listbox_entry_click),
                                  columns=self.columns,
                                  header_button_callback=lambda x:
                                  self.filter_sort_df(primary_sort=x))
        self.selected_listbox_idx = -1
        self.selected_listbox_entry = {}
        
        return listbox
    
    def setup_filter(self) -> GuiListboxFilterPanel:
        """
        Method for setting up the Filter frame on the right
        :return:
        """
        filter_frame = GuiListboxFilterPanel(frame=self, df=self.df, update_callback=self.filter_sort_df,
                                             default_sort=self.default_sort, padxy=(2, 2), columns=self.columns,
                                             grid=self.tk_grid.xywh('F'), hide_frame=self.hide_filter_frame)
        
        return filter_frame
        
    def setup_bottom_frame(self, bottom_frame: ttk.Frame, args: dict = None):
        """
        Setup method for the bottom frame. The bottom_frame that is passed will be placed below the primary listbox and
        can be configured after defining an instance of the super frame, allowing one to use for example the entry width
        of the primary listbox to define the width of the secondary listbox entries. Note that this method should be
        called separately after calling the constructor of this object in order to set up the bottom-left frame
        :param bottom_frame:
        :param args:
        :return:
        """
        if args is None:
            args = {}
        self.frame_bottom = bottom_frame
        xy, wh = self.tk_grid.xywh('E')
        self.frame_bottom.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=2, pady=2, sticky='WE')
    
    def get_entry_string(self, row):
        """
        Convert the given row from the dataframe to a formatted entry string for the listbox using the format methods
        defined in the ListboxColumn.
        :param row: Row from the dataframe as a dict
        :return: strings formatted according to the ListboxColumns
        """
        e = ''
        for c in list(self.entry_string_generator.keys()):
            e += self.entry_string_generator.get(c)(row.get(c))
        return e
    
    def listbox_entry_click(self, e=None):
        """
        Code that is executed upon clicking a listbox entry, provided a callback method has been defined. The defined
        callback method should accept the selected listbox entry as input. A selected listbox entry is the dataframe row
        converted to a dict (e.g. using df.to_dict('records'))
        :return:
        """
        self.selected_listbox_idx = self.frame_listbox.selected_indices()[0]
        self.selected_listbox_entry = self.entries[self.selected_listbox_idx]
        # print(f'{self.selected_listbox_idx} {self.selected_listbox_entry}')
        
        if self.listbox_entry_callback is not None:
            entry = self.selected_listbox_entry
            entry['listbox_entry_index'] = self.selected_listbox_idx
            self.listbox_entry_callback(self.selected_listbox_entry)
    
    def filter_sort_df(self, e=None, primary_sort: str = None):
        """
        Filter and sort the full dataframe according to configurations set in the filterpanel. Used as callback method
        in the filter panel and in sort buttons from the button header. If primary sort is passed, it will override
        existing primary sort config in the filter panel. If the primary sort already is the currently active primary
        sort, the corresponding reverse sort flag will be toggled.
        :param e: placeholder variable for tkinter events
        :param primary_sort: Set primary sort method to this column. Typically passed by header button sort callbacks.
        :return:
        """
        self.df_filtered = self.frame_filter.filter_and_sort(self.df, primary_sort=primary_sort)
        
        self.update_listbox()
        
    def update_listbox(self, filter_and_sort: bool = False):
        """
        Convert the filtered+sorted dataframe to a list of rows, format each row as defined by the visible listbox
        columns, then fill the listbox with the formatted entry strings. The list of entries is saved as a reference for
        listbox entry click callbacks.
        :param filter_and_sort: Apply active filters/sorting methods before generating and inserting the entries
        :return:
        """
        if filter_and_sort:
            self.filter_sort_df()
        idx = 0
        self.entries = self.df_filtered.to_dict('records')
        for e in [self.get_entry_string(row) for row in self.entries]:
            self.frame_listbox.listbox.insert(idx, e)
            idx += 1
            
    def get_listbox_entry(self, listbox_index: int) -> dict:
        """
        Get the listbox entry that corresponds to the given listbox index.
        :param listbox_index: Index of the entry in the listbox
        :return: The corresponding dataframe entry, returned as a dict with a key for each column
        """
        return self.entries[listbox_index]
    

class GuiGraph(ttk.Frame):
    def __init__(self, item_id: int = None, graphs: list = None, frame = None, title: str = '',
                 vertical_plots: bool = False, previous_fig=None, generate_graph: callable = None,
                 output_file: str = None, remap_items: bool = False):
        """ ttk Frame for displaying graphs on
        The GraphFrame serves as a display for graphs with additional information. The toolbar can be used to interact
        with the graph or export it as an image.

        Parameters
        ----------
        graphs : list
            A list of prepared matplotlib.pyplot.Axes objects, ready to be displayed in the frame

        Attributes
        ----------
        item_id : int
            Currently active item_id for which graphs are displayed

        Methods
        -------
        xy(char)
            Short notation for fetching the x, y coordinate tuple of the element with tag char

        wh(char)
            Short notation for fetching the w, h dimensions tuple of the element with tag char

        xywh(char)
            Short notation for fetching the x, y, w, h tuple of the element with tag char


        Raises
        ------
        ValueError
            A ValueError will be raised if the strings within the list have varying lengths.
            
        See Also
        --------
        graphs: implementations from the graphs module are used to display on the frame
        """
        window_size = (800, 600)
        if previous_fig is not None:
            plt.close(previous_fig)
        
        if output_file is None:
            if frame is None:
                top = tk.Toplevel(ttk.Frame())
                top.geometry(f"{window_size[0]}x{window_size[1]}")
                self.frame = top
            else:
                self.frame = frame
            super().__init__(self.frame)
        else:
            self.frame = ttk.Frame()
        # root = tkinter.Tk()
        # root.wm_title("Embedding in Tk")
        self.graph = None
        self.item_id = item_id
        self.title = ''
        self.add_vplots = vertical_plots
        self.remap_items = remap_items
        
        self.fig, self.axs, self.canvas, self.toolbar = None, None, None, None
        self.graph_interface_frame, self.toolbarFrame = ttk.Frame(self.frame), tk.Frame(self.frame)
        # self.plot_graph(None)

        # self.graph = graph
        # self.fig, self.axs = plt.subplots(len(graphs))
        # self.axs.plot(, y, 'r', linewidth=.3)
        # self.canvas.get_tk_widget().grid(row=1, column=0, columnspan=10, padx=5, pady=5, sticky='N')

        # navigation toolbar
        # self.toolbarFrame.grid(row=2, column=0, columnspan=10, sticky='N')
        # self.toolbar = NavigationToolbar2Tk(self.canvas, self.toolbarFrame)

        # self.canvas.draw()
        self.plot_graphs(graphs_list=graphs, gen_method=generate_graph)
    
    def plot_graphs(self, graphs_list: list, add_vplots: bool = None, gen_method: callable = None, output_file: str = None):
        """
        Plot the graphs from graphs. Additionally, vertical lines can also be plotted for indicating fixed spaces in
        time.
        
        Parameters
        ----------
        graphs_list : list
            One or more graphs, passed as a list.
        add_vplots : bool, optional, False by default
            Flag that indicates whether vertical plots that indicate fixed temporal distances should be indicated or not

        Returns
        -------

        """
        # self.graph = graph
        self.fig, self.axs = plt.subplots(len(graphs_list))
        # self.fig, self.axs = plt.figure(1), plt.plot()
        vplots, add_vplots = [], self.add_vplots if add_vplots is None else add_vplots
        
        for g in graphs_list:
            # todo: add abstract superior Graph class that can be used for identifying graphs
            # todo: Implement and pass specific graph generators as methods, like the dow price plot
            if not isinstance(g, PricesGraph):
                raise TypeError(f'Expected typing for passed graphs is a graphs.Graph class')
            item_id, t0, t1 = g.item.item_id, g._t0, g.t1
            remap = remap_item(item_id, 1000, 1000)
            
            if remap[0] != item_id and self.remap_items:
                remap_id = remap[0] if remap[1] != 1 else item_id
            else:
                remap_id = item_id
            
            p = self.axs[graphs_list.index(g)] if len(graphs_list) > 1 else self.axs
            # p = plot_prices(axs=p, np_ar=g.item, t0=t0, t1=t1)
            p = g.generate_graph(p=p, t0=t0, t1=t1, vplot_frequencies=get_vplot_timespans(delta_t=t1 - t0))
            if g.axs_gen is not None and not self.remap_items:
                continue
            
            def major_format_price_remapped(price: int, tick_id=None):
                """ For the y-axis on the right, display the remapped price instead, if applicable """
                return format_n(int(remap_item(g.item.item_id, price, 1)[1]-int(remap_item(g.item.item_id, price, 1)[1]*.01)), max_length=8, max_decimals=1)
            

            p.xaxis.set_minor_formatter(ts_to_dt)
            p.xaxis.set_major_formatter(ts_util.utc_ts_to_dt)
            p2 = p.twinx()
            p2.set_ylim(p.get_ylim())
            p2.set_ylabel("Price (gp)")
            # p2.set_yticks(p.get_yticks())
            p.set_ylabel(f'Price - tax {f"(decanted to 4 doses)" if remap_id!=item_id else ""}')
            p.set_xlabel(f'UTC | {-1*delta_t_utc} hour{"s" if abs(delta_t_utc) != 1 else ""} relative to local time')
            # print(p.get_yticks())
            p.yaxis.set_major_formatter(major_format_price_remapped)
            p2.yaxis.set_major_formatter(major_format_price_non_abbreviated)
            p2.yaxis.set_minor_formatter(major_format_price_non_abbreviated)
            # p = plot_prices_weekly(axs=p, np_ar=NpyArray(5952))
            # print(type(p))
            # p.set_title(g.title)
            # p.set_xticks([t0, (t0+t1)/2, t1])
            # p.set_xlim(t0, t1)
            # p.set_xticklabels([dow[(d + 2) % 7] for d in range(7)])
            # p.set_xlim(g.x_lim[0], g.x_lim[1]+300)
            # # p.set_xlim(ts_to_dt(g.x_lim[0]), ts_to_dt(g.x_lim[1]))
            # p = g.plot_me(p)
            
            
            # for y_v in g.y_values:
            #     x, y = g.plots.get(y_v)
            #     p.plot(x, y, 'r', linewidth=.3)
            if isinstance(output_file, str):
                plt.sca(self.axs)
                plt.show()
                time.sleep(123)
                plt.savefig(output_file)
                return
        
        # if isinstance(output_file, str):
        #     plt.imsave()
        
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.frame)
        self.canvas.get_tk_widget().grid(row=1, column=0, columnspan=10, padx=5, pady=5, sticky='N')
        
        # navigation toolbar
        self.toolbarFrame = tk.Frame(master=self.frame)
        self.toolbarFrame.grid(row=2, column=5, columnspan=5, sticky='N')
        self.toolbar = NavigationToolbar2Tk(self.canvas, self.toolbarFrame)
        
        self.canvas.draw()


f_int = lambda i: format_n(i)
listbox_columns = {
    'id': ListboxColumn(header='id', width=10, format=None, df_column='id', button_click=None, visible=True, is_number=True),
    'item_id': ListboxColumn(header='Item id', width=5, format=None, df_column='item_id', button_click=None, visible=True, is_number=True),
    'timestamp': ListboxColumn(header='Timestamp', width=10, format=format_ts, df_column='timestamp', button_click=None, visible=True, is_number=True),
    'price': ListboxColumn(header='Price', width=9, format=format_n, df_column='price', button_click=None, visible=True, is_number=True),
    'volume': ListboxColumn(header='Volume', width=10, format=None, df_column='volume', button_click=None, visible=True, is_number=True),
}

# Listbox columns of the show inventory listbox in the inventory tab
lbc_show_inventory = {
    'item_name': ListboxColumn(header="Item name", width=25, format=lambda x: shorten_string(x, 20),
                               df_column='item_name', is_number=False, push_left=False),
    'price': ListboxColumn(header='Price', width=9, format=f_int, df_column='price', visible=True, is_number=True),
    'quantity': ListboxColumn(header='Quantity', width=9, format=f_int, df_column='quantity', is_number=True),
    'value': ListboxColumn(header='Value', width=9, format=f_int, df_column='value', is_number=True),
    'profit': ListboxColumn(header='Profit', width=9, format=f_int, df_column='profit', is_number=True),
    'current_sell': ListboxColumn(header='Sell', width=9, format=f_int, df_column='current_sell', is_number=True),
    'current_buy': ListboxColumn(header='Buy', width=9, format=f_int, df_column='current_buy', is_number=True),
    'margin': ListboxColumn(header='Margin', width=9, format=f_int, df_column='margin', is_number=True),
    'tax': ListboxColumn(header='Tax', width=9, format=f_int, df_column='tax', is_number=True),
    'buy_limit': ListboxColumn(header='Limit', width=9, format=f_int, df_column='buy_limit', is_number=True)
}


# To-do: Implement a fixed set of listbox columns that can be used throughout the gui.
# Each column is unique, yet the same column can be used multiple times if it represents the same value..
def get_lbc():
    return {
        'id': shorten_string,
        # 'item_name': ListboxColumn("Item name", 25, lambda x: shorten_string(x, 25), None, lambda x: sort_lbox(x)),
        # 'price': ListboxColumn("Price", 7, lambda x: format_n(x), None, lambda x: sort_lbox(x)),
        # 'quantity': ListboxColumn("Price", 7, lambda x: format_n(x), None, lambda x: sort_lbox(x)),
        'amount': format_n,
        'profit': format_n,
        'current_sell': format_n,
        'current_buy': format_n,
        'margin': format_n,
        'tax': format_n,
        'item_id': shorten_string,
        'value': format_n,
        'buy_limit': format_n,
        'timestamp': format_ts,
        't0': format_ts,
        'day': ts_to_dow,
        'volume': format_n,
        'cur_sell': format_n,
        'cur_buy': format_n,
        'date': format_ts,
        'buys': format_n,
        'sales': format_n,
        'n_buy': format_n,
        'n_sell': format_n,
        'n_purchases': format_n,
        'n_sales': format_n,
        'invested': format_n,
        'returns': format_n,
        'month': format_y_m,
        'month_id': format_y_m,
        'bond_value': format_n,
        'buy': format_is_buy,
        'b/s': format_is_buy,
        'tag': shorten_string,
        'balance': format_n
        
    }

########################################################################################################################
# Tk pop-up windows
########################################################################################################################

def popup_msg(window_text: str, window_title: str = '', window_size: tuple = (100, 100), font: str = 'Helvetica 11'):
    """ Create a window displaying the given `window_text`
    Create a pop-up window with the sole intention of conveying `window_text` to the user. Can be used for more
    elaborate explanations/warnings that would not fit elsewhere. Optional parameters can be used to tweak specific
    GUI characteristics like size or font.
    
    Parameters
    ----------
    window_text : str
        Text to display in the window
    
    window_title : str, optional
        String to use as title for the window. By default, use ''
    
    window_size : tuple, optional
        Tuple with dimensions for window size. By default, use 100x100 pixels.
    
    font : str, optional
        A string specifying a font type and a font size, separated by a whitespace. By default, use Helvetica 11.
    
    
    """
    top = tk.Toplevel(ttk.Frame())
    top.geometry(f"{window_size[0]}x{window_size[1]}")
    top.title(window_title)
    
    GuiLabel(top, text=window_text, font=font, sticky='WE')


def popup_target_prices(item_name: str, window_size: tuple = (300, 150), font: str = 'Helvetica 10'):
    """ Create a pop-up window for configuring active target prices for the item specified
    
    Upon submitting target sell and/or buy prices, the window will be destroyed. Submitted data is saved locally and can
    also be accessed this way.
    
    Parameters
    ----------
    item_name : str
        Name of the item
    
    window_size : tuple, optional
        Dimensions of the pop-up window. Default is 300, 150
    
    Other Parameters
    ----------------
    font : str, optional
        Font name + font size, separated by a whitespace. Default is Helvetica 10.
        
    Raises
    ------
    A ValueError is raised if there is no item_id for the given name. This can be avoided by directly submitting a name
    from an item_id
    
    
    """
    item_id = name_id.get(item_name)
    if item_id is None:
        raise ValueError(f"An unknown item_name {item_name} was passed to popup_target_prices. Unable to determine "
                         f"the corresponding item_id...")
    
    top = tk.Toplevel(ttk.Frame())
    top.geometry(f"{window_size[0]}x{window_size[1]}")
    top.title(f'Setting target prices')
    
    cur = get_target_prices().get(item_id)
    if cur is None and item_id is not None:
        cur = {'buy': 0, 'sell': 0}
    
    lb_str = f'{item_name}\n' \
             f'Modify target prices here\n' \
             f'Entries will be colored if target is passed\n' \
             f'Save by pressing submit'

    def save_prices():
        """
        Parse the GUI entries, extract prices and save them if the prices are parseable and have legal values. After
        successfully logging the data, destroy the popup window.
        """
        b, s = parse_integer(en_buy.get()), parse_integer(en_sell.get())
    
        if (b > s != 0) or min(b, s) < 0 or max(b, s) > 2147000000:
            lb.set_text(item_name + '\nInvalid input! \n'
                                    'Price should be 2.147b > price >= 0...\n'
                                    'Sell price should be greater than buy price...')
        else:
            lb.set_text(f"Updating target prices for {item_name}...")
            tp = get_target_prices()
            tp[item_id] = {'buy': b, 'sell': s}
            save_data(tp, p.f_target_prices)
            time.sleep(2)
            top.destroy()
            
    grid = TkGrid(grid=['AAAA', 'BBCC', 'DDEE', 'FFFF'])
    str_b, str_s = tk.StringVar(), tk.StringVar()
    # lb = GuiLabel(top, text=id_name[item_id], font=font, sticky='WE', grid=grid.xywh('A'))
    lb = GuiLabel(top, text=lb_str, font=font, sticky='WE', grid=grid.xywh('A'))
    lb_buy = GuiLabel(top, text=f'Target buy price (cur={cur.get("buy")}):', font=font, sticky='WE',
                      grid=grid.xywh('B'))
    en_buy = GuiEntry(top, text=cur.get('buy'), textvariable=str_b, width=5, grid=grid.xywh('C'), padxy=(1, 1),
                      sticky="EW")
    lb_sell = GuiLabel(top, text=f'Target sell price (cur={cur.get("sell")}):', font=font, sticky='WE',
                       grid=grid.xywh('D'))
    en_sell = GuiEntry(top, text=cur.get('sell'), textvariable=str_s, width=5, grid=grid.xywh('E'), padxy=(1, 1),
                       sticky="EW")
    btn_submit = GuiButton(top, text=f"Submit target prices", command=save_prices, width=8, grid=grid.xywh('F'),
                           padxy=(1, 1), sticky="EW")
    

def popup_stock_correction(item_name: str, window_size: tuple = (350, 150), font: str = 'Helvetica 10',
                           ledger_db: str = p.f_db_local_new, en_width: int = 25):
    item_id = name_id.get(item_name)
    
    top = tk.Toplevel(ttk.Frame())
    top.geometry(f"{window_size[0]}x{window_size[1]}")
    top.title(f'{item_name} stock correction')
    
    def submit_price_correction():
        l = Ledger(db_path=ledger_db)
        applied_price, resulting_price = en_price.get(), en_resulting_price.get()
        l.submit_stock_correction(
            item_id=item_id,
            timestamp=dt_to_ts(parse_dt_str(en_ts.get())),
            resulting_quantity=parse_integer(en_quantity.get()),
            applied_price=0 if applied_price == '' else parse_integer(applied_price),
            resulting_price=1 if resulting_price == '' else parse_integer(resulting_price)
        )
        top.destroy()
    
    grid = TkGrid(grid=['AAAA', 'BBCC', 'DDEE', 'FFGG', 'HHII', 'JJ**'])
    
    lb_item = GuiLabel(top, text=item_name, grid=grid.xywh('A'), font=font, sticky='EW')
    
    lb_ts = GuiLabel(top, 'Transaction date (D-M-Y H:M:S)', grid=grid.xywh('B'), font=font, sticky='EW')
    en_ts = GuiEntry(top, text=format_ts(time.time()), width=en_width, grid=grid.xywh('C'), padxy=(3, 1),
                      sticky="EW")
    
    lb_quantity = GuiLabel(top, 'Counted quantity', grid=grid.xywh('D'), font=font, sticky='EW')
    en_quantity = GuiEntry(top, text='', width=8, grid=grid.xywh('E'), padxy=(3, 1),
                      sticky="EW")
    
    lb_price = GuiLabel(top, 'Apply price (optional)', grid=grid.xywh('F'), font=font, sticky='EW')
    en_price = GuiEntry(top, text='', width=en_width, grid=grid.xywh('G'), padxy=(3, 1),
                      sticky="EW")
    
    lb_resulting_price = GuiLabel(top, 'Resulting price (optional)', grid=grid.xywh('H'), font=font, sticky='EW')
    en_resulting_price = GuiEntry(top, text='', width=en_width, grid=grid.xywh('I'), padxy=(3, 1),
                      sticky="EW")
    
    btn_submit = GuiButton(top, text=f"Submit stock count", command=submit_price_correction, width=en_width, grid=grid.xywh('J'),
                           padxy=(3, 1), sticky="EW")
    

def popup_graphs(item_id: int = None, window_size: tuple = (300, 300)):
    """ Create pop-up GUI with a set of graphs showing data for the given item_id, graphs
    
    Parameters
    ----------
    item_id : int
        item_id for the item to show graphs for
    window_size : tuple
        Width and height of the GUI

    Returns
    -------

    """
    
    # To-do: Define graph creation in gui_graph and the gui here
    graph_root = gui_graph.GraphGui()
    window = graph_root.get_window()
    window.mainloop()
    grid = TkGrid(['AABBB'])
    top = tk.Toplevel(ttk.Frame())
    top.geometry(f"{window_size[0]}x{window_size[1]}")
    top.title(f'Price graph GUI')
    item_name = '' if item_id is None else str(id_name[item_id])
    label_cbox = GuiLabel(top, text='Selected item', sticky='E', grid=grid.xywh('A'))
    cbox_selected_item = GuiCombobox(top, values=[id_name[i] for i in npyar_items], text=item_name,
                                     grid=grid.xywh('B'), sticky='W')
    pass
    

if __name__ == '__main__':
    top = tk.Toplevel(ttk.Frame())
    top.geometry(f"{800}x{800}")
    ct = int(time.time())
    t0 = ct - ct%14400 - 86400*2
    t1 = ct - ct % 14400
    g = GuiGraph(graphs=[PricesGraph(item=NpyArray(2), t0=t0, t1=t1, y_values=['buy_price', 'sell_price'],
                             axs_gen=price_graph_by_dow)])
    popup_stock_correction(item_name='Adamantite ore')
    
    