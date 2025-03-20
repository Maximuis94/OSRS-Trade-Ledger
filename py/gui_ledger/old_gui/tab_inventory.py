""" This module implements the inventory tab of the graphical user interface

The inventory tab consists of two listboxes that display information about the Inventory. Using a button panel up top,
the current display mode can be altered. The display mode dictates with what kind of information the listboxes are
filled, e.g., showing results per day or a summary per item.
The top listbox is filled entirely according to the current display mode.
The bottom listbox is filled when clicking on a row on the top listbox, it will be filled with rows related to the row
clicked on in the top listbox.

Listbox modes:
1. Current inventory
- Top listbox: Show Inventory statistics per item
- Bottom listbox: Show all transactions for the item that was clicked on
2. Results per day
- Top listbox: Show results summarized per day
- Bottom listbox: Show individual transactions for the day that was clicked on

Code rework
Implementations in this module should be GUI-related. Most of the code is defined elsewhere;
gui_objects: general/abstract representation of gui components in the context of this project
gui_backend: getting specific data
gui_formats: formatting listbox rows, background colors, etc.
"""
import os.path

from filter import InventoryFilter
from ge_util import *
from global_values import months_tuple, db_itemdb, t_unit_sec
from graphs import price_graph_by_hod, price_graph
from gui_backend import get_entries_listbox_daily_top, get_entries_listbox_inventory_top, \
    get_entries_listbox_prices_24h_top, get_entries_listbox_prices_24h_bot
from gui_formats import listbox_inventory_bgc, listbox_daily_bgc, listbox_annually_bgc, listbox_monthly_bgc
from gui_objects import *
from ledger import Inventory, InventoryEntry, inventory_up_to_date
from str_formats import shorten_string, format_ts, format_n, format_int
from threaded_tasks import AsyncDataTransfer, AsyncJSONParse, AsyncTask


class InventoryFrame(ttk.Frame):
    def __init__(self, inventory_frame, **kw):
    
        """
        The InventoryFrame is the implementation of all logic involving the Inventory tab in the GUI. It is composed of
        a button panel, two listboxes and an additional frame. Clicking an entry of the primary listbox will result in
        the second listbox showing additional information on this entry.
        The button panel can be used to switch modes, which dictates what kind of information the listboxes are
        displaying.

        Parameters
        ----------
        inventory_frame: ttk.Frame
            This is the frame the InventoryFrame will be placed on.

        Attributes
        ----------
        self.listbox_mode : int
            An integer indicating the currently active display mode
        h_frame : ttk.Frame
            Header Frame, a frame displaying info on currently active configs
        b_frame : ttk.Frame
            Button frame, a frame with various buttons to change the currently active display mode
        lb_frame : ttk.Frame
            Listbox frame, the frame with the primary listbox
        slb_frame : ttk.Frame
            Secondary listbox frame, frame with a listbox that displays info on selected entries from the other listbox
        c_frame_0 : ttk.Frame
            Control frame, frame with various utilities that is active if listbox mode = 0
        c_frame_2 : ttk.Frame
            Control frame, frame with various utilities that is active if listbox mode = 2

        Methods
        -------
        update_listbox()
            Updates the primary listbox according to current configurations and clears the secondary listbox
        lbox_onclick_item()
            Executes upon clicking a primary listbox entry and ensures the proper methods are called, according to
            current configurations
        
        
        """

        super().__init__(**kw)
        self.selected_id = 0
        self.last_update_time = time.time()
        self.setup = False
        print("Initializing inventory GUI...", end='\r')
        
        self.inventory = None
        self.frame = inventory_frame
        self.selected_ids = []
        self.selected_entry_string = ""
        self.cbox_sort_value_list = ['item_name', 'stock_value', 'profit', 'margin', 'tax']
        self.cbox_filter_value_list = ['current_buy', 'current_sell', 'stock_value', 'profit', 'margin', 'tax']
        self.active_filters = [InventoryFilter('Quantity > 1'), InventoryFilter('Value > 1m')]
        self.listbox_sorting_value = tk.StringVar(value="profit")
        self.listbox2_sorting_value = tk.StringVar(value="profit")
        self.listbox_filter_value = tk.StringVar(value="")
        try:
            self.buy_list = {el.get('item_name'): el.get('price') for el in
                             pd.read_csv(p.f_buy_list).to_dict('records')}
            # print(f'Loaded buy list \n{self.buy_list}')
        except FileNotFoundError:
            self.buy_list = {}
        self.prices_listbox_update_time = None
        self.reverse_sort_order = True
        self.reverse_sort_secondary = True
        self.pg_days, self.pg_add_vplots = 3, True
        self.lbox_top_group_by = 'd'

        self.listbox_mode = -1
        self.lb_header = ["Current stock", "Results/", "Prices/4h"]
        # Header_frame, button_frame, listbox_fram
        self.h_frame, self.b_frame, self.lb_frame = ttk.Frame(self.frame), ttk.Frame(self.frame), ttk.Frame(self.frame)
        
        # Secondary_listbox_frame
        self.slb_frame = ttk.Frame(self.frame)
        
        # Control_frame_0, control_frame_2
        self.c_frame_0, self.c_frame_2 = ttk.Frame(self.frame), ttk.Frame(self.frame)
        
        # Graph frame
        self.g_frame = ttk.Frame(self.frame)
        
        # self.se_frame = ttk.Frame(self.frame)
        fwidth, fheight = self.frame.winfo_width(), self.frame.winfo_height()
        grid = TkGrid(grid=["A"])
        padxy, font = (3, 3), ('Monaco', 9)
        self.label_listbox_header = GuiLabel(self.h_frame, text="Current stock", font=('Helvetica', 16), padxy=(5, 5),
                                             grid=grid.xywh('A'), sticky='W', width=12)

        self.transferring_data, self.parsing_exports, self.computing_inventory = False, False, False
        self.str_transfer_a, self.str_transfer_b = 'Update Data', 'Updating...'
        self.str_parse_a, self.str_parse_b = 'Parse GE Exports', 'Parsing Exports...'
        self.str_inv_a, self.str_inv_b = 'Recompute inventory', 'Computing...'
        grid = TkGrid(grid=["B", "C", "D", "E", "A", "F", "G"])
        self.btn_show_inventory = GuiButton(self.b_frame, command=self.show_inventory, text='Show inventory',
                                            grid=grid.xywh('B'), padxy=(2, 2), sticky='NWE')
        self.btn_results_daily = GuiButton(self.b_frame, command=self.show_results_daily, text='Results per day',
                                           grid=grid.xywh('C'), padxy=(2, 2), sticky='NWE')
        self.btn_results_monthly = GuiButton(self.b_frame, command=self.show_price_analysis, text='Item prices',
                                             grid=grid.xywh('D'), padxy=(2, 2), sticky='NWE')
        self.btn_show_lifetime = GuiButton(self.b_frame, command=self.show_lifetime, text='Lifetime results',
                                            grid=grid.xywh('E'), padxy=(2, 2), sticky='NWE')
        self.btn_compute_inventory = GuiButton(self.b_frame, command=self.compute_inventory_threaded, padxy=(2, 2),
                                               text=self.str_inv_a, grid=grid.xywh('A'), sticky='NWE')
        self.btn_update_data = GuiButton(self.b_frame, command=self.transfer_data, text=self.str_transfer_a,
                                         grid=grid.xywh('F'), padxy=(2, 2), sticky='NWE')
        self.btn_parse_ge_exports = GuiButton(self.b_frame, command=self.parse_ge_exports, text=self.str_parse_a,
                                              grid=grid.xywh('G'), padxy=(2, 2), sticky='NWE')
        grid = TkGrid(grid=['A'])
        s_lb = self.sort_primary_listbox
        
        # Convert item prices interval start to readable dates
        e = get_entries_listbox_prices_24h_top()[0]
        del e['ts']
        dt_0 = dt_to_ts(parse_dt_str(e.get('t0')))
        # dt_0 = e.get('ts')
        self.dt_cols = [k for k in list(e.keys()) if k[0] == 'b']
        order = reversed(list(e.keys())[1:7])
        self.dt_cols = {self.dt_cols[idx]: format_ts(dt_0+idx*14400, str_format='%d-%m %Hh') for idx in range(6)}
        self.dt_cols = {k: self.dt_cols.get(k) for k in order}

        self.default_sort_top = [
            ('item_name', True),
            ('t0', False),
            ('item_name', True)
        ]
        
        # this is a list of lists; the outer list refers to the listbox mode, while the nested list contains a full
        # listbox button header
        self.top_listbox_columns = [
            [ListboxColumn("Item name", 20, lambda x: shorten_string(x, max_length=19), 'item_name', lambda x: s_lb(x),
                           push_left=True),
                ListboxColumn("Price", 9, lambda x: format_n(x, 1), 'price', lambda x: s_lb(x)),
                ListboxColumn("Quantity", 9, lambda x: format_n(x, 1), 'quantity', lambda x: s_lb(x)),
                ListboxColumn("Value", 9, lambda x: format_n(x, 1), 'value', lambda x: s_lb(x)),
                ListboxColumn("Profit", 9, lambda x: format_n(x, 1), 'profit', lambda x: s_lb(x)),
                ListboxColumn("Cur sell", 8, lambda x: format_n(x, 1), 'current_sell', lambda x: s_lb(x)),
                ListboxColumn("Cur buy", 8, lambda x: format_n(x, 1), 'current_buy', lambda x: s_lb(x)),
                ListboxColumn("Margin", 9, lambda x: format_n(x, 1), 'margin', lambda x: s_lb(x)),
                ListboxColumn("Tax", 9, lambda x: format_n(x, 1), 'tax', lambda x: s_lb(x)),
                ListboxColumn("Buy limit", 9, None, 'buy_limit', lambda x: s_lb(x))],
            
            [ListboxColumn("Date", 9, lambda x: format_ts(x, '%d-%m-%y'), 't0', lambda x: s_lb(x)),
             ListboxColumn("Day", 4, lambda x: ts_to_dow(x)[:3], 't0', lambda x: s_lb(x)),
             ListboxColumn("Buys", 5, lambda x: format_n(x, 1), 'n_buy', lambda x: s_lb(x)),
             ListboxColumn("Invested", 9, lambda x: format_n(x, 1), 'invested', lambda x: s_lb(x)),
             ListboxColumn("Sales", 5, lambda x: format_n(x, 1), 'n_sell', lambda x: s_lb(x)),
             ListboxColumn("Returns", 9, lambda x: format_n(x, 1), 'returns', lambda x: s_lb(x)),
             ListboxColumn("Profit", 9, lambda x: format_n(x, 1), 'profit', lambda x: s_lb(x)),
             ListboxColumn("Tax", 9, lambda x: format_n(x, 1), 'tax', lambda x: s_lb(x))],
            
            [ListboxColumn("Item name", 20, lambda x: shorten_string(x, max_length=19), 'item_name', lambda x: s_lb(x),
                           push_left=True),
                # ListboxColumn("Day", 3, None, 11, lambda x: s_lb(x)),
                ListboxColumn("Cur sell", 9, lambda x: format_n(x, 1), 'current_sell', lambda x: s_lb(x)),
                ListboxColumn("Volume", 9, lambda x: format_n(x, 1), 'volume', lambda x: s_lb(x)),
                ListboxColumn("s_24h_high", 9, lambda x: format_n(x, 1), 's_24h_high', lambda x: s_lb(x)),
                ListboxColumn("Buy low", 9, lambda x: format_n(x, 1), 'buy_low', lambda x: s_lb(x)),
                ListboxColumn("Buy high", 9, lambda x: format_n(x, 1), 'buy_high', lambda x: s_lb(x)),
                ListboxColumn("Last buy", 8, lambda x: format_n(x, 1), 'last_buy', lambda x: s_lb(x)),
                ListboxColumn("Last sell", 8, lambda x: format_n(x, 1), 'last_sell', lambda x: s_lb(x)),
                ListboxColumn("Last trade", 10, lambda x: format_ts(x, '%d-%m %Hh'), 'last_traded', lambda x: s_lb(x)),
                ListboxColumn("Buy_list", 8, lambda x: format_n(x, 1), 'buy_list', lambda x: s_lb(x))
                
             ]
            
            # [ListboxColumn("Item name", 20, lambda x: shorten_string(x, max_length=19), 'item_name', lambda x: s_lb(x), push_left=True),
            #     # ListboxColumn("Day", 3, None, 11, lambda x: s_lb(x)),
            #     ListboxColumn("Cur sell", 9, lambda x: format_n(x, 1), 'current_sell', lambda x: s_lb(x)),
            #     ListboxColumn("Volume", 9, lambda x: format_n(x, 1), 'volume', lambda x: s_lb(x)),
            #     ListboxColumn("s_24h", 9, lambda x: format_n(x, 1), 's_24h', lambda x: s_lb(x))
            #  ] + [ListboxColumn(self.dt_cols.get(dt), 9, lambda x: format_n(x, 1), dt, lambda x: s_lb(x))
            #       for dt in list(self.dt_cols.keys())]
            
             #    ListboxColumn(self.dt_cols.get("b_0_4"), 9, lambda x: format_n(x, 1), 'b_0_4', lambda x: s_lb(x)),
             #    ListboxColumn(self.dt_cols.get("b_4_8"), 9, lambda x: format_n(x, 1), 'b_4_8', lambda x: s_lb(x)),
             #    ListboxColumn(self.dt_cols.get("b_8_12"), 9, lambda x: format_n(x, 1), 'b_8_12', lambda x: s_lb(x)),
             #    ListboxColumn(self.dt_cols.get("b_12_16"), 9, lambda x: format_n(x, 1), 'b_12_16', lambda x: s_lb(x)),
             #    ListboxColumn(self.dt_cols.get("b_16_20"), 9, lambda x: format_n(x, 1), 'b_16_20', lambda x: s_lb(x)),
             #    ListboxColumn(self.dt_cols.get("b_20_24"), 9, lambda x: format_n(x, 1), 'b_20_24', lambda x: s_lb(x))
             # ]
            
        ]
        self.default_sort_bot = [
            ('t0', False),
            ('t0', False),
            ('t0', False)
        ]
        s_lb = self.sort_secondary_listbox
        self.bot_listbox_columns = [
    
            [ListboxColumn("Date", 16, lambda x: format_ts(x, '%d-%m-%y'), 'timestamp', lambda x: s_lb(x)),
             ListboxColumn("Buy", 6, lambda x: 'Buy' if x == 1 else 'Sell', 'is_buy', lambda x: s_lb(x)),
             ListboxColumn("Price", 8, lambda x: format_n(x, 1), 'price', lambda x: s_lb(x)),
             ListboxColumn("Quantity", 8, lambda x: format_n(x, 1), 'quantity', lambda x: s_lb(x)),
             ListboxColumn("Value", 9, lambda x: format_n(x, 1), 'value', lambda x: s_lb(x)),
             ListboxColumn("Profit", 9, lambda x: format_n(x, 1), 'profit', lambda x: s_lb(x)),
             ListboxColumn("Tax", 9, lambda x: format_n(x, 1), 'tax', lambda x: s_lb(x)),
             # ListboxColumn("Tag", 3, None, 'tag', lambda x: s_lb(x)),
             ListboxColumn("Balance", 9, lambda x: format_n(x, 1), 'balance', lambda x: s_lb(x))],
    
            [ListboxColumn("Item", 16, lambda x: shorten_string(x, max_length=15), 'item_name', lambda x: s_lb(x)),
             ListboxColumn("Buy", 6, lambda x: 'Buy' if x == 1 else 'Sell', 'is_buy', lambda x: s_lb(x)),
             ListboxColumn("Price", 8, lambda x: format_n(x, 1), 'price', lambda x: s_lb(x)),
             ListboxColumn("Quantity", 8, lambda x: format_n(x, 1), 'quantity', lambda x: s_lb(x)),
             ListboxColumn("Value", 9, lambda x: format_n(x, 1), 'value', lambda x: s_lb(x)),
             ListboxColumn("Profit", 9, lambda x: format_n(x, 1), 'profit', lambda x: s_lb(x)),
             ListboxColumn("Tax", 9, lambda x: format_n(x, 1), 'tax', lambda x: s_lb(x)),
             # ListboxColumn("Tag", 3, None, 'tag', lambda x: s_lb(x)),
             ListboxColumn("Balance", 9, lambda x: format_n(x, 1), 'balance', lambda x: s_lb(x))],
            
            [ListboxColumn("Date", 6, lambda x: x[:5], 't0', lambda x: s_lb(x), push_left=True),
             # ListboxColumn("Timestamp", 6, lambda x: x, 'ts', lambda x: s_lb(x), visible=True),
        # ListboxColumn("Date", 6, lambda x: x[:5]; format_ts(x, str_format='%d-%m'), 't0', lambda x: s_lb(x), push_left = True),
             # ListboxColumn("Day", 3, None, 11, lambda x: sort_lbox(x)),
             # ListboxColumn("Cur sell", 9, lambda x: format_n(x, 1), 'current_sell', lambda x: sort_lbox(x)),
             # ListboxColumn("Volume", 9, lambda x: format_n(x, 1), 'volume', lambda x: sort_lbox(x)),
             ListboxColumn("s_24h_high", 9, lambda x: format_n(x, 1), 's_24h_high', lambda x: s_lb(x)),
             ListboxColumn("s_24h_last", 9, lambda x: format_n(x, 1), 's_24h_last', lambda x: s_lb(x)),
             ListboxColumn("delta_s_b", 9, lambda x: format_n(x, 1), 'delta_s_b', lambda x: s_lb(x))
             ] + [ListboxColumn(self.dt_cols.get(dt), 9, lambda x: format_n(x, 1), dt, lambda x: s_lb(x))
                  for dt in list(self.dt_cols.keys())]
             # ListboxColumn(self.dt_cols.get("b_0_4"), 9, lambda x: format_n(x, 1), 'b_0_4', lambda x: s_lb(x)),
             # ListboxColumn(self.dt_cols.get("b_4_8"), 9, lambda x: format_n(x, 1), 'b_4_8', lambda x: s_lb(x)),
             # ListboxColumn(self.dt_cols.get("b_8_12"), 9, lambda x: format_n(x, 1), 'b_8_12', lambda x: s_lb(x)),
             # ListboxColumn(self.dt_cols.get("b_12_16"), 9, lambda x: format_n(x, 1), 'b_12_16', lambda x: s_lb(x)),
             # ListboxColumn(self.dt_cols.get("b_16_20"), 9, lambda x: format_n(x, 1), 'b_16_20', lambda x: s_lb(x)),
             # ListboxColumn(self.dt_cols.get("b_20_24"), 9, lambda x: format_n(x, 1), 'b_20_24', lambda x: s_lb(x))
             # ]
        ]
        # self.secondary_listbox_sort = ([])
        self.change_graph_btn_text = ['Prices plot', 'Price/day-of-week', 'Price/hour-of-day']
        
        self.listbox_top = GuiListboxFrame(self.lb_frame, event_bindings=('<<ListboxSelect>>', self.lbox_top_onclick),
                                           df=None, top_label_text='',
                                           bottom_label_text='', grid=grid.xywh('A'),
                                           padxy=padxy, sticky='E', listbox_height=8, entry_width=140, font=font,
                                           columns=self.top_listbox_columns[0],
                                           header_button_callback=self.sort_primary_listbox,
                                           default_sort=('item_name', True))

        ## SW Frame
        grid = TkGrid(grid=['AAA', 'BCD', 'EEE'])
        self.listbox_secondary = GuiListboxFrame(self.slb_frame, grid=grid.xywh('A'), padxy=padxy,
                                                 sticky='W', listbox_height=8, entry_width=100, font=font,
                                                 header_button_callback=self.sort_secondary_listbox,
                                                 df=None)

        self.current_buy_list = GuiLabel(self.slb_frame, text=f"", width=30, grid=grid.xywh('E'),
                                         padxy=padxy, sticky="NW")

        ## SE Frame 0
        grid = TkGrid(grid=['AAAAA', 'BB*GG', 'EEEEE', 'CC*DD', 'KK*FF'])
        # grid = TkGrid(grid=['AAAA', 'BBGG', 'EEEE', 'CCDD', 'FFFF', 'KKKK'])
        padxy = (3, 6)
        self.label_selected_entry = GuiLabel(self.c_frame_0, text=f"No entry selected",
                                             width=25, grid=grid.xywh('A'),
                                             padxy=padxy, sticky="NW")
        self.btn_target_prices = GuiButton(self.c_frame_0, text="Set target prices", width=12,
                                           command=self.alter_target_prices, grid=grid.xywh('B'), padxy=padxy, sticky="NW")
        self.btn_target_prices = GuiButton(self.c_frame_0, text="Count stock", width=12,
                                           command=self.count_stock, grid=grid.xywh('G'), padxy=padxy, sticky="NE")
        self.new_filter = tk.StringVar()
        self.entry_filter_item = GuiEntry(self.c_frame_0, text=f"Type a filter, e.g. price > 100k", textvariable=self.new_filter,
                                          width=12, grid=grid.xywh('E'), padxy=(12, 3), sticky="SEW")
        self.btn_update_filter = GuiButton(self.c_frame_0, text=f"Add filter",
                                           width=12, grid=grid.xywh('C'), command=self.set_filter,
                                           padxy=(12, 3), sticky="W")
        self.btn_clear_filter = GuiButton(self.c_frame_0, text=f"Clear filters",
                                          width=15, grid=grid.xywh('D'), command=self.btn_filter_clear,
                                          padxy=(12, 3), sticky="W")
        self.label_active_filters = GuiLabel(self.c_frame_0, grid=grid.xywh('F'))
        self.set_filter(active_filters=self.active_filters)
        self.btn_refresh_inventory = GuiButton(self.c_frame_0, text='Refresh inventory', command=self.update_listbox,
                                               sticky='WE', grid=grid.xywh('K'))
        
        
        ## SE frame mode 2
        # grid = TkGrid(grid=['AAAAGEEEEE', 'BBCCCFFFFF', 'EEEEE', 'DDDDD', 'FFFFF'])#, '****E'])#, 'EE', 'FF', 'KK'])
        grid = TkGrid(grid=['AAAA', 'DDDD', 'BBCC', 'EEEE', 'FFFF', 'GGGG', 'HHII', 'JJKK', 'LMNN'])#, '****E'])#, 'EE', 'FF', 'KK'])
        self.header_label_se2 = GuiLabel(self.c_frame_2, text=f"No top listbox entry selected",
                                         width=25, grid=grid.xywh('A'),
                                         padxy=padxy, sticky="NW")
        self.buy_list_price_var = tk.StringVar()
        self.label_buy_list = GuiLabel(self.c_frame_2, text=f"Price", grid=grid.xywh('B'),
                                       padxy=padxy, sticky="E", width=6)
        self.entry_buy_list = GuiEntry(self.c_frame_2, text=f"", textvariable=self.buy_list_price_var,
                                       width=18, grid=grid.xywh('C'), padxy=padxy, sticky="W")
        self.btn_submit_to_buy_list = GuiButton(self.c_frame_2, text=f"Update buy list",
                                                width=24, grid=grid.xywh('E'), command=self.submit_to_buy_list,
                                                padxy=padxy, sticky="WE")
        
        self.clicked_clear = False
        self.btn_clear_buy_list = GuiButton(self.c_frame_2, text=f"Clear buy list",
                                            width=12, grid=grid.xywh('D'), command=self.clear_buy_list,
                                            padxy=padxy, sticky="E")
        
        self.prioritize_recent = tk.BooleanVar(value=False)
        self.cbtn_traded_at_top = GuiCheckbutton(self.c_frame_2, text='Prioritize recent trades', initial_state=True,
                                                 grid=grid.xywh('F'),  #command=self.cbtn_prioritize_recent_clicked,
                                                 padxy=padxy, sticky="E",
                                                 event_bindings=('<Button-1>', self.cbtn_prioritize_recent_clicked))
        self.lb_graph_config = GuiLabel(self.c_frame_2, text='Graph configurations', grid=grid.xywh('G'),
                                       padxy=padxy, sticky="WE")

        self.graph_span_unit, self.graph_span_size, self.str_graph_t1 = tk.StringVar(), tk.StringVar(), tk.StringVar()
        self.graph_t0, self.graph_t1, self.graph_id, self.max_graph_id = 0, 0, 0, 2
        self.en_graph_t0 = GuiEntry(self.c_frame_2, text=f"2", textvariable=self.graph_span_size,
                                       width=18, grid=grid.xywh('J'), padxy=padxy, sticky="WE")
        self.cbox_graph_t0 = GuiCombobox(self.c_frame_2, text='days', grid=grid.xywh('K'),
                                      padxy=padxy, sticky="W", values=['hours', 'days', 'weeks'],
                                         textvariable=self.graph_span_unit)
        self.lb_graph_t1 = GuiLabel(self.c_frame_2, text='Graph end (local)', grid=grid.xywh('H'),
                                       padxy=padxy, sticky="W")
        self.en_graph_t1 = GuiEntry(self.c_frame_2, text=f"", textvariable=self.str_graph_t1,
                                       width=18, grid=grid.xywh('I'), padxy=padxy, sticky="WE")
        self.btn_reset_t0_t1 = GuiButton(self.c_frame_2, text=f"Reset timespan",
                                            width=12, grid=grid.xywh('L'), command=self.reset_timespan,
                                            padxy=padxy, sticky="E")
        self.btn_refresh_graph = GuiButton(self.c_frame_2, text=f"Refresh graph",
                                            width=12, grid=grid.xywh('M'), command=self.refresh_graph,
                                            padxy=padxy, sticky="E")
        self.btn_change_graph = GuiButton(self.c_frame_2, text=self.change_graph_btn_text[self.graph_id],
                                            width=12, grid=grid.xywh('N'), command=self.change_graph,
                                            padxy=padxy, sticky="WE")
        self.graph = None
        
        # rows = get_entries_listbox_prices_24h_top(item_ids=[13190], inv=self.inventory, buy_list=self.buy_list)[0]
        # self.initial_row = [self.listbox_top.submitted_entries[idx] for idx in ][0]
        # self.initial_row = [r for r in get_entries_listbox_prices_24h_top(inv=self.inventory, buy_list=self.buy_list) if r.get('item_name') == id_name[13190)][0]
        # self.refresh_graph(row=self.initial_row)

        # self.label_listbox_bottom = tk.Label(self.n_frame, textvariable=self.str_listbox_bottom)
        self.h_frame.grid(row=0, column=0, rowspan=1, columnspan=3, sticky='NW', padx=3, pady=3)
        self.b_frame.grid(row=1, column=0, rowspan=7, columnspan=3, sticky='NW', padx=3, pady=3)
        self.lb_frame.grid(row=0, column=4, rowspan=4, columnspan=8, sticky='NWE', padx=3, pady=3)
        self.slb_frame.grid(row=4, column=4, rowspan=4, columnspan=5, sticky='NWE', padx=3, pady=3)
        self.c_frame_0.grid(row=4, column=9, rowspan=4, columnspan=3, sticky='NE', padx=3, pady=3)
        self.g_frame.grid(row=0, column=12, rowspan=10, columnspan=3, sticky='NWS', padx=5, pady=3)
        

        ####### INVENTORY TAB FRAMES ###########################################################
        # Add entries entry/modify section
        # Add scrollbox for entries entries
        # Add view section
        # self.keyboard_in will trigger when the keyboard focus shifts to this frame (i.e. frame is opened)
        self.frame.bind("<FocusIn>", self.keyboard_in)
        self.frame.bind("<Enter>", self.cursor_in)
        self.frame.bind("<Leave>", self.cursor_out)
        self.frame.bind("<FocusOut>", self.keyboard_out)
        self.cursor_over_frame = False
        self.keyboard_over_frame = tk.BooleanVar()
        self.keyboard_over_frame.set(True)
        self.compute_inventory()
        self.show_price_analysis()
        self.show_inventory()
        print_task_length('inventory GUI setup', self.last_update_time)

    def entered_tab(self):
        """ Executes if the tab is entered """
        if self.keyboard_over_frame.get():
            print("Entered inventory tab")
            self.compute_inventory()
            # self.ledger = Ledger()
            self.update_listbox()

    # This code will run when the user opens the tab
    def keyboard_in(self, event):
        """ Executes if the keyboard is active in the frame """
        self.keyboard_over_frame.set(True)

    def keyboard_out(self, event):
        """ Executes if the keyboard is no longer active in the frame """
        self.keyboard_over_frame.set(False)

    def cursor_in(self, event):
        """ Executes if the cursor enters the frame"""
        self.cursor_over_frame = True

    def cursor_out(self, event):
        """ Executes if the cursor is no longer over the frame """
        self.cursor_over_frame = False

    def compute_inventory(self, e=None, refresh_listbox: bool = False):
        """ Process all transactions within the inventory, if there are new transactions """
        t1 = time.time()
        # self.label_listbox_header.set_text(f'Computing Inventory and updating entries...')
        # time.sleep(10)
        self.inventory = Inventory(Ledger(), import_data=False)
        # self.inventory.ledger.sync_transactions()
        # self.inventory = Inventory()
        # self.inventory.execute_all()
        if refresh_listbox:
            self.update_listbox()
        # self.label_listbox_header.set_text(temp)
        # self.ledger.recompute = False
        print(f'Forced recomputing ledger (time taken: {int(1000*(time.time()-t1))}ms)')

    def show_inventory(self, e=None):
        """ Button callback for changing display mode to Inventory """
        self.change_listbox_mode(m=0)
        if self.inventory is None or not inventory_up_to_date():
            self.compute_inventory()
        self.update_listbox()

    def show_results_daily(self, e=None):
        """ Button callback for changing display mode to Results per day """
        # Group by a different time unit if the button is pressed again
        if self.listbox_mode == 1:
            ltgb = self.lbox_top_group_by
            self.lbox_top_group_by = 'm' if ltgb == 'd' else 'y' if ltgb == 'm' else 'd'
        self.change_listbox_mode(m=1)
        self.update_listbox()

    def show_price_analysis(self, e=None):
        """ Button callback for 4h prices analysis. Alter various objects across the tab """
        self.header_label_se2.set_text("Currently no item selected")
        self.change_listbox_mode(m=2)
        self.btn_clear_buy_list._text.set('Clear buy list')
        self.clicked_clear = False
        self.cbtn_traded_at_top.set(True)
        self.prioritize_recent.set(False)
        
        self.prices_listbox_update_time = format_ts(timestamp=os.path.getmtime(p.f_tracked_items_listbox))
        self.reset_timespan()
        self.update_listbox()
        self.refresh_graph(row=[r for r in self.listbox_top.submitted_entries if r.get('item_name') == id_name[13190]][0])

    def show_lifetime(self, e=None):
        self.listbox_mode = 3
        self.update_listbox()
    
    def change_listbox_mode(self, m: int):
        """ Change what kind of information the top listbox currently displays """
        header_text = self.lb_header[m]
        if m == 1:
            header_text = header_text + {'d': 'day', 'm': 'month', 'y': 'year'}.get(self.lbox_top_group_by)
        self.label_listbox_header.set_text(header_text)
        if m != self.listbox_mode:
            self.listbox_mode = m
            self.listbox_secondary.clear_listbox()
            
            self.listbox_secondary.set_bottom_text('Select an entry above to display more detailed information here')
            self.listbox_top.column_list = self.top_listbox_columns[m]
            self.listbox_top.make_button_header()
            self.listbox_secondary.column_list = self.bot_listbox_columns[m]
            self.listbox_secondary.make_button_header()
            frame_prefix = 'c_frame_'
            for se in [k for k in list(self.__dict__.keys()) if k[:len(frame_prefix)] == frame_prefix and int(k[-1]) != m]:
                self.__dict__.get(se).grid_forget()
            c_frame = f'{frame_prefix}{m}'
            if isinstance(self.__dict__.get(c_frame), ttk.Frame):
                # self.__dict__.get(c_frame).grid(row=5, column=13, rowspan=5, columnspan=5, sticky='NE', padx=3, pady=3)
                self.__dict__.get(c_frame).grid(row=4, column=9, rowspan=4, columnspan=3, sticky='NE', padx=3, pady=3)
        self.selected_id = -1
        self.listbox_sorting_value.set(self.default_sort_top[m][0])
        self.reverse_sort_order = self.default_sort_top[m][1]
        self.listbox_secondary.sort_by, self.listbox_secondary.sort_reverse = self.default_sort_bot[m]

    class Filter:
        def __init__(self, filter):
            self.filter = filter
            self.evaluations = []
            self.active = False

        def evaluate(self):
            return True
    
    def cbtn_prioritize_recent_clicked(self, e=None):
        """ Checkbutton to indicate whether to sort item price entries also on last traded timestamp """
        self.prioritize_recent.set(not self.prioritize_recent.get())
        self.update_listbox()

    def update_listbox(self):
        """ Fetch entries, format them and fill the listbox with them, according to the currently active mode """
        columns = self.top_listbox_columns[self.listbox_mode]
        color_format, header_text = None, self.lb_header[self.listbox_mode]
        
        sort_by, sort_asc = self.listbox_sorting_value.get(), self.reverse_sort_order
        if self.listbox_mode == 0:
            entries = get_entries_listbox_inventory_top(inventory=self.inventory, columns=columns)
            entries = [e for e in entries if False not in [f.evaluate(e) for f in self.active_filters]]
            color_format = listbox_inventory_bgc
            lb_bot = f"Totals ["
            for k in ['value', 'n_purchases', 'invested', 'n_sales', 'returns', 'profit', 'tax']:
                lb_bot += f'{k}: {format_n(self.inventory.total.get(k), max_decimals=1)} '
            self.listbox_top.set_bottom_text(lb_bot + 'Bonds: ' + format_int(self.inventory.bond_stats.get('value')) + ']')
        elif self.listbox_mode == 1:
            self.inventory.create_timeline(timeline=self.inventory.timeline, results=self.inventory.results)
            entries = get_entries_listbox_daily_top(timeline=self.inventory.timeline, group_by=self.lbox_top_group_by) #, columns=columns)
            color_format = listbox_daily_bgc if self.lbox_top_group_by == 'd' else listbox_monthly_bgc if self.lbox_top_group_by == 'm' else listbox_annually_bgc
            # entries = get_entries_listbox_monthly_top(timeline=self.inventory.timeline)
            # color_format = None
            
            dtn = datetime.datetime.now()
            min_ts = dt_to_ts(datetime.datetime(dtn.year, dtn.month, 1, 0, 0, 0))
            cur_mon = [self.inventory.timeline.get(ts) for ts in list(self.inventory.timeline.keys()) if ts >= min_ts]
            profit, tax, invested, returns = 0, 0, 0, 0
            
            bond = self.inventory.results.get(13190).transactions
            bond = sum([bond.get(k).get('price') for k in list(bond.keys()) if bond.get(k).get('timestamp') > min_ts])
            
            for e in cur_mon:
                profit += e.get('stats').get('profit')
                tax += e.get('stats').get('tax')
                invested += e.get('stats').get('invested')
                returns += e.get('stats').get('returns')
                df = e.get('subset')
                df = df.loc[df['item_id'] == 13190]['value'].to_list()
                if len(df) > 0:
                    bond += sum(df)
                
            self.listbox_top.set_bottom_text(f'{months_tuple[dtn.month]} profit: {format_int(profit)} '
                                             f'tax: {format_int(tax)} invested: {format_int(invested)} '
                                             f'returns: {format_int(returns)} bonds: {format_int(bond)}')
            
        elif self.listbox_mode == 2:
            # To-do: Implement bottom listbox filler, set labels, more frequent npy updates?
            
            id_list = [name_id.get(i) for i in pd.read_csv(p.f_tracked_items_csv)['item_name'].to_list()]
            entries = get_entries_listbox_prices_24h_top(inv=self.inventory, buy_list=self.buy_list)
            color_format = None
            s = f'Last updated: {self.prices_listbox_update_time}'
            self.listbox_top.set_bottom_text(string=f'Last updated: {self.prices_listbox_update_time}')
            
            # At this particular listbox, always show last traded items on top
            if not self.prioritize_recent.get():
                sort_by, sort_asc = ['last_traded', sort_by], [False, sort_asc]
        else:
            entries = []
        # print(entries)
        print(self.listbox_mode)
        entries = pd.DataFrame(entries).sort_values(by=sort_by, ascending=sort_asc)
        self.listbox_top.fill_listbox(entry_list=entries.to_dict('records'), columns=columns, color_format=color_format)
            
    def set_filter(self, active_filters=None):
        if active_filters is None:
            self.active_filters.append(InventoryFilter(self.entry_filter_item.get()))
        else:
            self.active_filters = active_filters
        string = ""
        for f in self.active_filters:
            string += f"{f.as_str()}\n"
        self.label_active_filters.set_text(string)

    def se_frame_load_item(self, e=None):
        return
            
    def sort_primary_listbox(self, sorting_criterion: int):
        """ Header button callback for the top listbox - Sort rows based on whatever button was clicked on """
        if self.listbox_sorting_value.get() == sorting_criterion:
            self.reverse_sort_order = not self.reverse_sort_order
        else:
            sorting_criterion = sorting_criterion.lower().replace(' ', '_')
            self.listbox_sorting_value.set(sorting_criterion)
            self.reverse_sort_order = True
        self.update_listbox()
        
    def sort_secondary_listbox(self, sort_id: int):
        """ Header button callback for the bottom listbox - Sort rows based on whatever button was clicked on """
        c = self.bot_listbox_columns[sort_id] if isinstance(sort_id, int) else sort_id
        
        if isinstance(c, ListboxColumn) or isinstance(c, str):
            c = c.df_column if isinstance(c, ListboxColumn) else c
            if self.listbox2_sorting_value.get() == c:
                self.reverse_sort_secondary = not self.reverse_sort_secondary
            else:
                self.reverse_sort_secondary = True
            self.listbox2_sorting_value.set(c)
            self.listbox_secondary.update_sort(var=c, reverse=self.reverse_sort_secondary)
            
    def alter_target_prices(self, e=None):
        """ Create a pop-up window to modify target prices of the item loaded in the bottom listbox """
        item = NpyArray(2, full_days=False)
        # print(x)
        # print(y1)
        # window_size = (600, 600)
        # top = tk.Toplevel(ttk.Frame())
        # top.geometry(f"{window_size[0]}x{window_size[1]}")
        # top.title(f'Setting target prices')
        if self.listbox_mode == 0:
            selected_item = self.listbox_top.submitted_entries[self.selected_id].get('item_name')
            if selected_item is not None and not self.label_selected_entry._text.get() == 'No entry selected':
                popup_target_prices(item_name=selected_item)
            
    def count_stock(self, e=None):
        """ Create a pop-up window to submit a stock count """
        if self.listbox_mode == 0:
            selected_item = self.listbox_top.submitted_entries[self.selected_id].get('item_name')
            if selected_item is not None:
                popup_stock_correction(item_name=selected_item)
            # popup_target_prices(item_name=self.listbox_top.submitted_entries[self.selected_id].get('item_name'))
            
    def btn_filter_add(self, e=None):
        # print(self.listbox_inventory.selected_indices())
        self.selected_id = self.cbox_filter_value_list.index(self.listbox_sorting_value.get())
        as_dict = {}
        if self.listbox_mode == 0:
            self.lbox_top_onclick_item()
            # self.listbox_secondary.set_top_text(f"{'Date': ^10} {'B/S': ^3} {'Amount': ^6} "
            #                                     f"{'Price': ^7} {'Value': ^7} {'profit': ^7} {'tax': ^7}")
        if self.listbox_mode == 1:
            self.lbox_top_onclick_daily()
            
    def btn_filter_clear(self, e=None):
        # print(self.listbox_inventory.selected_indices())
        self.active_filters = []
        self.update_listbox()
        self.label_active_filters.set_text("No active filters")
        as_dict = {}
        # if self.listbox_mode == 0:
        #     self.lbox_top_onclick_item()
            # self.listbox_secondary.set_top_text(f"{'Date': ^10} {'B/S': ^3} {'Amount': ^6} "
            #                                     f"{'Price': ^7} {'Value': ^7} {'profit': ^7} {'tax': ^7}")
        # if self.listbox_mode == 1:
        #     self.lbox_top_onclick_daily()
            
    def submit_to_buy_list(self, e=None):
        """ Button callback for submitting to buy list. If GuiEntry is empty, delete selected entry instead. """
        # print(self.listbox_inventory.selected_indices())
        if name_id.get(self.selected_entry_string) is None:
            raise ValueError(f"Attempting to submit a non-existent item {self.selected_entry_string} to the buy_list")
        n = self.buy_list_price_var.get()
        
        # Entry is empty -- delete the selected item instead
        if len(n) == 0:
            del self.buy_list[self.selected_entry_string]
        else:
            n = parse_integer(n)
            if not isinstance(n, int):
                raise ValueError(f"Input {self.buy_list_price_var.get()} cannot be converted to an integer...")
            self.buy_list[self.selected_entry_string] = n
            self.buy_list_price_var.set('')
        df = pd.DataFrame( [{'item_name': k, 'price': self.buy_list.get(k)} for k in list(self.buy_list.keys())])
        try:
            df.to_csv(p.f_buy_list, index=False)
        except PermissionError:
            popup_msg(f'Unable to save buy_list data! Make sure to close the csv file at {p.f_buy_list} to prevent '
                      f'accidental loss of data...')
            self.header_label_se2.set_text("Did not update ./resources/buy_list.csv, make sure the file is closed...")
        
        s = ''
        for k in list(self.buy_list.keys()):
            s += f'{k} for {self.buy_list.get(k)} ea\n'
        self.current_buy_list.set_text(s)
        # self.selected_id += 1
        self.update_listbox()
        self.lbox_top_onclick(force_idx=self.selected_id+1)
    
    def clear_buy_list(self, e=None):
        if not self.clicked_clear:
            self.clicked_clear = True
            self.btn_clear_buy_list._text.set("Click to confirm")
        else:
            self.buy_list = {}
            pd.DataFrame(columns=['item_name', 'price']).to_csv(p.f_buy_list, index=False)
            self.btn_clear_buy_list._text.set("Clear buy list")
            self.update_listbox()
    
    def reset_timespan(self, e=None):
        """ Set the configured graph timespan to its default values """
        self.graph_t1 = os.path.getmtime(p.f_tracked_items_listbox)
        self.graph_t1 = int(self.graph_t1 - self.graph_t1%3600)
        self.graph_t0 = self.graph_t1 - 2 * 86400
        self.str_graph_t1.set(format_ts(self.graph_t1, '%d-%m-%Y %H:00'))
        self.graph_span_size.set('2')
        self.graph_span_unit.set('days')
    
    def refresh_graph(self, e=None, row=None):
        if row is None:
            row = self.listbox_top.submitted_entries[self.selected_id]
        self.graph_t1 = dt_to_ts(parse_dt_str(self.str_graph_t1.get()))
        span_size, span_unit = self.graph_span_size.get(), self.graph_span_unit.get()
        self.graph_t0 = self.graph_t1 - int(span_size) * t_unit_sec.get(span_unit[:-1])
        # print(f'{ts_to_dt(self.graph_t0)} - {ts_to_dt(self.graph_t1)}')
        self.lbox_top_onclick_prices(selected_row=row)
    
    def change_graph(self, e=None, row=None):
        """ Change the graph type that is currently displayed and plot it with data from the last selected row """
        self.graph_id += 1
        if self.graph_id > self.max_graph_id:
            self.graph_id = 0
        self.btn_change_graph._text.set(self.change_graph_btn_text[self.graph_id])
        self.refresh_graph(row=row)

    def lbox_top_onclick(self, e=None, force_idx: int = None):
        """ User clicked on a top listbox row; fetch values of this row and execute onclick related to listbox mode """
        if force_idx is None:
            try:
                self.selected_id = self.listbox_top.selected_indices()[0]
                selected_row = self.listbox_top.submitted_entries[self.selected_id]
            except IndexError:
                return
        else:
            self.selected_id = force_idx
            selected_row = self.listbox_top.submitted_entries[self.selected_id]
            self.listbox_top.listbox.select_set(self.selected_id)
        
        # Proceed depending on which listbox is activated
        if self.listbox_mode == 0:
            self.lbox_top_onclick_item(selected_row=selected_row)
        if self.listbox_mode == 1:
            self.lbox_top_onclick_daily(selected_row=selected_row)
        if self.listbox_mode == 2:
            self.refresh_graph(row=selected_row)
            # self.lbox_top_onclick_prices(selected_row=selected_row)
            # self.se_frame_2.focus_force()
            
    def transfer_data(self):
        """ Update data button callback; Create a thread to import RBPi data and update npy arrays"""
        if not self.transferring_data:
            self.transferring_data = True
            try:
                self.btn_update_data._text.set(self.str_transfer_b)
                data_transfer = AsyncDataTransfer(full_transfer=True, callback_oncomplete=self.transfer_completed)
                data_transfer.start()
            finally:
                # self.transferring_data = False
                pass
    
    def transfer_completed(self):
        self.btn_update_data._text.set(self.str_transfer_a)
        self.transferring_data = False
    
    def parse_ge_exports(self):
        """ Parse GE exports button callback; Create a thread to parse exported GE json files from Runelite.net """
        if not self.parsing_exports:
            self.parsing_exports = True
            try:
                self.btn_parse_ge_exports._text.set(self.str_parse_b)
                json_parsing = AsyncJSONParse(callback_oncomplete=self.parse_completed)
                json_parsing.start()
            finally:
                # self.parsing_exports = False
                pass
    
    def parse_completed(self):
        """ Callback to execute upon completing the parse """
        self.compute_inventory(refresh_listbox=False)
        self.btn_parse_ge_exports._text.set(self.str_parse_a)
        self.parsing_exports = False
        
    def compute_inventory_threaded(self):
        """ Asynchronously compute the current item balances by executing all submitted transactions """
        if not self.computing_inventory:
            self.computing_inventory = True
            self.btn_compute_inventory._text.set(self.str_inv_b)
            try:
                task = AsyncTask(task=self.compute_inventory)
                task.start()
            finally:
                self.btn_compute_inventory._text.set(self.str_inv_a)
                self.computing_inventory = False
        
    def lbox_top_onclick_item(self, selected_row: dict):
        """ Callback for clicking a top listbox row in inventory mode """
        item_name = selected_row.get('item_name')
        item_id = name_id.get(item_name)
        current_item = self.inventory.results.get(item_id)
        if current_item is None:
            self.listbox_secondary.set_bottom_text(f"No item selected")
            return
        self.label_selected_entry.set_text(item_name)
        self.selected_entry_string = item_name
        try:
            item_transactions = current_item.transactions
            df = []
            for k in list(item_transactions.keys()):
                # print(k, item_transactions.get(k))
                transaction = current_item.augment_transaction(item_transactions.get(k))
                transaction['value'] = transaction.get('quantity') * transaction.get('price')
                df.append(transaction)
            self.listbox_secondary.df = pd.DataFrame(df)
            tp = get_target_prices().get(item_id)
            
            txt = f" {item_name} ({len(df)} transactions) | Current stock: {current_item.quantity} (value: " \
                  f"{int(current_item.quantity*current_item.price)}) | Profit: {int(current_item.profit)}\n"
            if tp is not None:
                  txt += f"Target prices [buy: {tp.get('buy')} / sell: {tp.get('sell')}]"
            self.listbox_secondary.set_bottom_text(txt)
        except AttributeError:
            self.selected_id = -1
            self.listbox_secondary.set_bottom_text(f"No item selected")
            return
        self.listbox_secondary.sort_by, self.listbox_secondary.sort_reverse = 'timestamp', False
        self.listbox_secondary.fill_listbox(entry_list=self.listbox_secondary.sort_df().to_dict('records'),
                                            columns=self.bot_listbox_columns[self.listbox_mode])

    def lbox_top_onclick_daily(self, selected_row: dict):
        """ Callback for clicking a top listbox row in results per day mode """
        timestamp = selected_row.get('t0')
        e = self.inventory.timeline.get(timestamp)
        s = e.get('stats')
        self.label_selected_entry.set_text(f'{s.get("date")}')
        self.listbox_secondary.sort_df(df=self.inventory.timeline.get(timestamp).get('subset'))
        txt = f'{s.get("date")} | Profit: {s.get("profit")} Invested: {s.get("invested")} ' \
              f'Returns: {s.get("returns")} N bought/sold: {s.get("n_buy")}/{s.get("n_sell")} Tax: {s.get("tax")}'
        self.listbox_secondary.set_bottom_text(txt)
        self.listbox_secondary.fill_listbox(
            entry_list=self.listbox_secondary.sort_df(df=e.get('subset')).to_dict('records'),
            columns=self.bot_listbox_columns[self.listbox_mode]
        )

    def lbox_top_onclick_prices(self, selected_row: dict):
        """ Callback for clicking a top listbox row in results per day mode """
        item_name = selected_row.get('item_name')
        item_id = name_id.get(item_name)
        entry_list = get_entries_listbox_prices_24h_bot(item_id=item_id)
        item = self.inventory.results.get(item_id)
        self.selected_entry_string = item_name
        self.header_label_se2.set_text(f"{item_name}")
        self.label_buy_list.set_text("Price:")
        df = pd.DataFrame(entry_list).sort_values(by=['ts'], ascending=[False])
        del df['ts']
        # def to_ts(dt: str):
        #     d, t = dt.split(' ')
        #     d, m = d.split('-')
        #     return f"{m:0>2}{d:0>2}{t}"
        #
        # df['ts'] = df['t0'].apply(lambda x: to_ts(x))
        # df = df.sort_values(by=['ts'], ascending=[False])
        # del df['ts']
        self.listbox_secondary.fill_listbox(
            entry_list=df.to_dict('records'), # self.listbox_secondary.sort_df(df=df).to_dict('records'),
            columns=self.bot_listbox_columns[self.listbox_mode]
        )
        lb_bot = f'{selected_row.get("item_name")} | Daily volume: {selected_row.get("volume")} | '
        if isinstance(item, InventoryEntry):
                 lb_bot += f'Balance: {item.quantity} | Avg buy: {int(item.price)} | Buy limit: {item.buy_limit}'
        else:
            idb_entry = db_itemdb.read_db(where_clause='WHERE item_id = :item_id', values_dict={'item_id': item_id})
            lb_bot += f'Balance: -  | Avg buy: NA | Buy limit: {idb_entry.get("buy_limit").values[0]}'
        self.listbox_secondary.set_bottom_text(lb_bot)
        buy_list_price = self.buy_list.get(item_name)
        self.buy_list_price_var.set('' if buy_list_price is None else buy_list_price)
        ar = NpyArray(item_id, full_days=False)
        t_max = max(ar.timestamp)
        # x = item.timestamp[np.nonzero(item.timestamp >= t_max - 86400 - t_max % 86400)]
        # y1 = item.buy_price[np.nonzero(item.timestamp >= t_max - 86400 - t_max % 86400)]
        # y2 = item.sell_price[np.nonzero(item.timestamp >= t_max - 86400 - t_max % 86400)]
        # h = PricesGraph(item=ar, y_values='sell_price', t0=t_max - 2 * 86400, t1=t_max)
        # Prices graph
        remap_prices = self.graph_id == 0
        if self.graph_id == 0:
            # def major_format_price_remapped(price: int, tick_id=None):
            #
            #     """ For the y-axis on the right, display the remapped price instead, if applicable """
            #     return format_n(int(remap_item(ar.item_id, price, 1)[1] - int(
            #         remap_item(ar.item_id, price, 1)[1] * .01)), max_length=8, max_decimals=1)
            # def pg_gui(axs, item, t0, t1, vplot_frequencies):
            #     return price_graph(axs, item, t0, t1, vplot_frequencies, None, y_format=major_format_price_remapped)
            pg = PricesGraph(item=ar, t0=self.graph_t0, t1=self.graph_t1, y_values=['buy_price', 'sell_price'],
                             axs_gen=price_graph)
            
        # Price per day of week, relative to weekly average
        elif self.graph_id == 1:
            pg = PricesGraph(item=ar, t0=self.graph_t0, t1=self.graph_t1, y_values=['buy_price', 'sell_price'],
                             axs_gen=price_graph_by_dow)
        elif self.graph_id == 2:
            pg = PricesGraph(item=ar, t0=self.graph_t0, t1=self.graph_t1, y_values=['buy_price', 'sell_price'],
                             axs_gen=price_graph_by_hod)
        else:
            pg = None
        
        try:
            fig_old = self.graph.fig
        except AttributeError:
            fig_old = None
        
        if pg is not None:
            self.graph = GuiGraph(graphs=[pg], frame=self.g_frame, vertical_plots=self.pg_add_vplots, previous_fig=fig_old, remap_items=remap_prices)
        # g.plot_graph(None, x, y1)
        return
        


