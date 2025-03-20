"""
This module contains the implementation of the GuiListbox

"""
import tkinter as tk
import tkinter.ttk as ttk
from collections.abc import Callable, Iterable
from multipledispatch import dispatch
from typing import List, Tuple, Optional

from gui.base.frame import GuiFrame
from gui.base.frame import TkGrid
from gui.component._listbox.column import ListboxColumn
from gui.component._listbox.entry_manager import ListboxEntries
from gui.component._listbox.row import ListboxRow
from gui.component.button import GuiButton
from gui.component._listbox.filter import Filter, NUM_FILTERS
from gui.component.interface.row import IRow
from gui.component.label import GuiLabel
from gui.component._listbox.sort import Sort, Sorts
from gui.util.colors import Color, Rgba
from gui.util.constants import letters


class GuiListboxFrame(GuiFrame):
    """
    GuiFrame that holds a Listbox, as well as complementary components.
    
    The frame is composed of the following components;
    Primary Listbox
    - Is dedicated to a particular purpose
    - Onclick row -> Typically yields a response via the secondary listbox.
    
    Secondary Listbox
    - Is dedicated to complementing the primary listbox; it is filled via command sent by a responsive primary listbox
    -
    
    
    """
    
    entries: ListboxEntries
    columns: Tuple[ListboxColumn, ...]
    button_header_row: int = -1
    onclick_row: Optional[Callable[[IRow], any]] = None
    sort_sequence: Iterable[Sort]
    
    def __init__(self, frame: GuiFrame, tag: str, entries: List[dict or tuple], entry_width=20, listbox_height=10,
                 top_label_text='',
                 sticky='N', header_button_callback=None, bottom_label_text='', padxy: Tuple[int, int] = (0, 0),
                 event_bindings=None, select_mode=tk.SINGLE, font=('Monaco', 10), columns: List[ListboxColumn] = None,
                 filter: callable = None, onclick_row: Callable[[IRow], any] = None, **kwargs):
        
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
        
        onclick_row : Callable[[ListboxEntry], any], optional
            A command to execute whenever a row is clicked. The clicked row will be passed as arg.



        Attributes
        ----------
        frame : ttk.Frame
            Frame on which the object will be placed

        Methods
        -------
        set_text(string)
            Method for changing the text displayed by the label
        """
        self.button_panel_buttons = None
        self.button_panel = None
        
        self.init_widget_start(frame=frame, tag=tag, **{k: v for k, v in kwargs.items() if k != 'text'})
        self.n_entries = 0
        self.row = 0
        super().__init__(frame, ['A', "B", 'C'])
        
        self.columns = tuple(columns)
        if top_label_text is not None:
            self.top_label = GuiLabel(self, text=top_label_text, padxy=padxy, sticky=sticky,
                                      font=font, tag='A')
            self.top_label.grid(row=self.row, column=0, columnspan=entry_width + 1, sticky='WE')
            self.row += 1
        
        self.button_header_row = self.row
        self.row += 1
        self.scrollbar = ttk.Scrollbar(self, orient="vertical")
        self.scrollbar.grid(row=self.row, column=entry_width, sticky="NS")
        self.listbox = tk.Listbox(self, width=entry_width, height=listbox_height, selectmode=select_mode,
                                  yscrollcommand=self.scrollbar.set, font=ListboxEntries.font.tk)
        self.listbox.grid(row=self.row, column=0, columnspan=entry_width, sticky="NWE")
        self.scrollbar.config(command=self.listbox.yview)
        # self.button_panel = ttk.Frame(self, width=entry_width)
        if bottom_label_text is not None:
            self.bottom_label = GuiLabel(self, text=bottom_label_text, tag='C', padxy=(0, 0), sticky=sticky)
            self.row += 1
            self.bottom_label.grid(row=self.row, column=0, columnspan=entry_width + 1, sticky='WE')
        
        if isinstance(entries[0], dict):
            self.entries = ListboxEntries([ListboxRow({c.id: entry[c.column] for c in self.columns}) for entry in entries],
                                          listbox_columns=self.columns, insert_subset=self.fill_listbox, **kwargs)
        else:
            self.entries = ListboxEntries([ListboxRow(e) for e in entries], listbox_columns=columns,
                                          insert_subset=self.fill_listbox, **kwargs)
        self.entries.apply_configurations(filters=NUM_FILTERS.EQUAL.get("price", 0))
        self.make_button_header()
        
        self.header, self.button_ids = "", {}
        self.entry_chars, self.width_scaling, self.entry_width = None, None, entry_width
        self.column_indices = {}
        self.submitted_entries = []
        
        self.fill_listbox()
        # self.bottom_frame = ttk.Frame(self, width=entry_width)
        # self.setup_bottom_frame()
        
        if event_bindings is not None:
            for binding in event_bindings:
                self.listbox.bind(binding[0], binding[1])
        # super(GuiWidget, self).__init__(frame, tag, event_bindings=event_bindings)
        # self.grid(**grid.get_dims(tag=grid_tag, sticky=sticky, padx=padxy[0], pady=padxy[1]))
        self.last_sorted = "", True
        self.onclick_row = onclick_row
        self.listbox.bind("<<ListboxSelect>>", self.row_click)
    
    def insert_entry(self, index, entry, bgc_rgb: Rgba = None):
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
                self.set_entry_bgc(index, bgc_rgb)
        except AttributeError as e:
            print("Attribute error!")
            print('\t', index)
            print('\t', entry)
            raise e
    
    # Clear the listbox and fill it with entries from entries_list
    def fill_listbox(self, columns: list = None, sort_by=None,
                     color_format: Callable[[ListboxRow], Rgba or Tuple[int, int, int] or Color] = None):
        """
        Fill the listbox with the entries. Format them as dictated by the columns and the color format, if applicable.
        :param entry_list: List of rows (pd.Dataframe.to_dict('records')
        :param columns: List of columns the listbox consists of
        :param color_format: Method used to compute the background color of the row.
        :return:
        """
        # self.entries.apply_configurations(
        #     sort_sequence=self.sort_by if sort_by is None else sort_by,
        #     filters=None,
        #     column_order=self.columns if columns is None else columns
        # )
        self.clear_listbox()
        for idx, e in enumerate(self.entries.subset):
            row = e.strf(columns)
            # rgb = color_format(entry_list.index(row)) if color_format is not None else None
            self.submitted_entries.append(row)
            self.insert_entry(idx, entry=row, bgc_rgb=color_format(e) if color_format is not None else Color.WHITE)
        return
    
    @dispatch(int, Rgba)
    def set_entry_bgc(self, index: int, color: Rgba = None):
        """Set the background color for the given index to the color represented by `color`"""
        # print(colorcode)
        if color is not None:
            self.listbox.itemconfig(index, bg=color.hexadecimal)
    
    @dispatch(int, str)
    def set_entry_bgc(self, index: int, color: str = None):
        """Set the background color for the given index to the color represented by `color`"""
        if color is not None:
            self.listbox.itemconfig(index, bg=color)
    
    @dispatch(int, Color)
    def set_entry_bgc(self, index: int, color: Color = None):
        """Set the background color for the given index to the color represented by `color`"""
        if color is not None:
            self.listbox.itemconfig(index, bg=color.value.hexadecimal)
    
    @dispatch(int, tuple)
    def set_entry_bgc(self, index: int, rgb: Tuple[int, int, int]):
        """Set the background color for the given index to the color represented by `color`"""
        self.listbox.itemconfig(index, bg=Rgba(*rgb).hexadecimal)
    
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
    
    def make_button_header(self, entry_spacing: int = 1):
        """ 17-01: Tested and appears to be working
        Create the button header. Each visible column is assigned a button.
        
        Parameters
        ----------
        entry_spacing

        Returns
        -------

        """
        if isinstance(self.columns, dict):
            # self.columns = {c.column: listbox_columns.get(c) for c in list(self.columns.keys()) if
            #                 self.columns.get(c).visible}
            self.columns = tuple([c for _, c in self.columns.items() if c.visible])
        n = len(self.columns)
        if False in [isinstance(self.columns, tuple)] + [isinstance(c, ListboxColumn) for c in self.columns]:
            raise TypeError("Error initializing button header; columns should be a list of properly initialized "
                            "ListboxColumns. ")
        self.entry_chars = sum([c.width + entry_spacing for c in self.columns]) - entry_spacing
        self.width_scaling = 1.15  # self.entry_width / self.entry_chars
        self.entry_width = self.entry_chars
        
        # Reset button header
        if self.button_panel_buttons is not None:
            for b in self.button_panel_buttons:
                if isinstance(b, GuiButton):
                    b.destroy()
        
        x, w_sum, self.header, self.button_panel_buttons, self.button_ids = 0, 0, " ", [], {}
        buttongrid, button_grid_ids, column_grid_ids = "", letters, {}
        self.column_indices = {}
        buttons, columns = [], []
        for idx, c in enumerate(self.columns):
            if not c.visible:
                continue
            # print(c.__dict__)
            w = int(c.width / self.entry_chars * self.entry_width * self.width_scaling)
            w_sum += w
            self.header += f"{c.header: ^{c.width}} "
            buttons.append({
                "command": self.entries.header_button_sort,
                "command_kwargs": {"sort": Sort(c.column)},
                "button_text": c.header,
                "width": w,
                "sticky": "WE",
                "tag": letters[x]
            })
            columns.append(c)
            
            buttongrid += letters[x] * w
            column_grid_ids[c.header] = letters[x]
            x += 1
            self.column_indices[c.column] = self.columns.index(c)
        
        if self.button_panel is not None:
            self.button_panel.destroy()
        if self.button_header_row == -1:
            self.button_header_row = self.row
            self.row += 1
        self.button_panel = GuiFrame(self, TkGrid([buttongrid]))
        self.button_panel_buttons = []
        
        for column, button_kwargs in zip(columns, buttons):
            b = GuiButton(self.button_panel, **button_kwargs)
            key = str(b.info).split('!')[-1].replace('>', '')
            self.button_ids[key] = column.column
        self.button_panel.grid(row=self.button_header_row, column=0, sticky='NW')
    
    # def header_button_default(self, e=None):
    #     # self.header_button_callback(e)
    #     # return
    #     header_id = self.button_ids.get(f"{str(e.__dict__.get('widget')).split('!')[-1]}")
    #     sort_by = self.sort_by
    #     self.sort_by = header_id
    #     self.sort_reverse = False if self.sort_by != sort_by else not self.sort_reverse
    #     self.df = self.df.sort_values(by=[sort_by], ascending=[self.sort_reverse])
    #     self.fill_listbox(self.df.to_dict('records'), columns=self.columns)
    
    # def button_click(self, e=None, column_name: str = None):
    #     # print(self.button_ids.get(f"{str(e.__dict__.get('widget')).split('!')[-1]}"))
    #     # self.header_button_default(e)
    #     if column_name is not None:
    #         if self.last_sorted[0] == column_name:
    #             self.last_sorted = column_name, not self.last_sorted[1]
    #         else:
    #             self.last_sorted = column_name, True
    #     self.entries.apply_configurations()
    #     # self.header_button_callback(self.button_ids.get(f"{str(e.__dict__.get('widget')).split('!')[-1]}"))
    
    # def update_sort(self, var, reverse):
    #     if isinstance(var, int):
    #         self.sort_by, self.sort_reverse = [self.columns[var].df_column], [reverse]
    #     elif isinstance(var, str):
    #         self.sort_by, self.sort_reverse = [var], [reverse]
    #     elif isinstance(var, list):
    #         self.sort_by, self.sort_reverse = var, reverse
    #     if self.df is not None:
    #         self.df = self.df.sort_values(by=var, ascending=reverse)
    #         self.fill_listbox(self.df.to_dict('records'), columns=self.columns)
    #         return self.df.to_dict('records')
    
    def filter_entries(self, filters: Optional[Filter]):
        """Filter the listbox entries by applying the filters that are given"""
        raise NotImplementedError   # TODO
    
    def sort_entries(self, sort_sequence: Sorts):
        """Sort the listbox entries by sequentially applying the given sequence"""
        self.entries.apply_configurations(sort_sequence)
    
    # Code that will be executed when the user clicks on an entry in the listbox
    @staticmethod
    def clicked_entry(event):
        w = event.widget
        idx = int(w.curselection()[0])
    
    @property
    def active_filters(self) -> None or Filter or List[Filter]:
        """Return the list of Filters that are currently active"""
        ...
    
    def row_click(self, e):
        """Executes whenever a row is clicked. """
        if self.onclick_row is not None:
            idx = self.listbox.curselection()[0]
            entry = self.entries.subset[idx]
            self.onclick_row(entry)
    
    @property
    def default_sort(self) -> Sorts:
        """The sorting sequence that is applied by default"""
        return self.entries.default_sort_sequence
    
    @default_sort.setter
    def default_sort(self, sort_by: str | Tuple[str, bool] | List[Tuple[str, bool]]):
        self.entries.initial_sort(sort_by)
#
# if __name__ == '__main__':
#     import global_variables.path as gp
#     db = Database(gp.f_db_local)
#     rows = db.execute("""SELECT * FROM "transaction" WHERE item_id < 100""", factory=dict).fetchall()
#     df = pd.DataFrame(rows)
#     df.to_pickle(os.path.join(gp.dir_data, "test_df.dat"))
    