"""
This module contains the implementation of the GuiListbox

"""
import tkinter as tk
import tkinter.ttk as ttk
from typing import List, Tuple

import pandas as pd

from global_variables.gui import value_formats
from gui.model.button import GuiButton
from gui.model.grid import TkGrid
from gui.model.gui_widget import GuiWidget
from gui.model.label import GuiLabel
from util.array import df_filter_num
from util.gui_formats import rgb_to_colorcode, format_listbox_row


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
            f = f"{self.value_format(x): {'<' if self.push_left else '>'}{self.width - 1}} " if self.visible else ""
            if len(f) > self.width and print_warning:
                print(
                    f'Formatted value for column {self.header} exceeded configured width {self.width} (width={len(f)})')
            return f[:self.width]
        except TypeError:
            print(x)
            return x
    
    def apply_df_filter(self, filter: tuple, df: pd.DataFrame):
        """ Filter the given dataframe; apply the filter tuple to the `df_column` DataFrame column values. """
        column, operator, value = filter
        if self.value_type == 'num':
            return df_filter_num(df, column, operator, value)
        # TODO
        # if self.value_type == 'str':
        #     return df_filter_str(df=df, c=column, o=operator, v=value)
        return df


class GuiListboxFrame(ttk.Frame, GuiWidget):
    def __init__(self, frame, grid: TkGrid, grid_tag: str, df: pd.DataFrame = None, entry_width=20, listbox_height=10, top_label_text='',
                 sticky='N', header_button_callback=None, bottom_label_text='', padxy: Tuple[int, int] = (0, 0),
                 event_bindings=None, select_mode=tk.SINGLE, font=('Monaco', 10), columns: List[ListboxColumn] = None,
                 default_sort: tuple = (0, True), filter: callable = None, **kwargs):
        
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
        grid = TkGrid(['A', '*', 'C'])
        self.n_entries = 0
        super().__init__(frame)
        if top_label_text is not None:
            self.top_label = GuiLabel(self, text=top_label_text, padxy=padxy, sticky=sticky,
                                      font=font, grid_tag='A', grid=grid)
        self.scrollbar = ttk.Scrollbar(self, orient="vertical")
        self.scrollbar.grid(row=1, column=entry_width, sticky="NS")
        self.listbox = tk.Listbox(self, width=entry_width, height=listbox_height, font=font, selectmode=select_mode,
                                  yscrollcommand=self.scrollbar.set)
        self.listbox.grid(row=1, column=0, columnspan=entry_width, sticky="NWE")
        self.scrollbar.config(command=self.listbox.yview)
        self.button_panel = ttk.Frame(self, width=entry_width)
        self.button_panel_buttons = None
        if bottom_label_text is not None:
            self.bottom_label = GuiLabel(self, text=bottom_label_text, grid=grid, grid_tag='C', padxy=(0, 0), sticky=sticky)
        self.header, self.button_ids = "", {}
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
        super(GuiWidget, self).__init__(frame, grid_tag, grid, event_bindings=event_bindings)
        # self.grid(**grid.get_dims(tag=grid_tag, sticky=sticky, padx=padxy[0], pady=padxy[1]))
    
    def insert_entry(self, index, entry, bgc_rgb: tuple = None):
        """ Insert an (un)formatted entry into the listbox at the given index, coloring it with the given rgb tuple """
        try:
            if not isinstance(entry, str):
                entry_str = " "
                for formatted_value in [self.columns[i].get_value(entry.as_tuple()[i]) for i in
                                        range(len(self.columns))]:
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
        for idx, e in enumerate(entry_list):
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
        self.top_label.set_text(string + (int(char_lim) - len(string)) * ' ')
    
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
        self.entry_chars = sum([c.width + entry_spacing for c in self.columns]) - entry_spacing
        self.width_scaling = 1  # self.entry_width / self.entry_chars
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
                buttongrid += button_grid_ids[x] * w
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
        self.fill_listbox(self.df.to_dict('records'), columns=self.columns)
    
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