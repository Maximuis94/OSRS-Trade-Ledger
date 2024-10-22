import tkinter as tk
from tkinter import ttk

from global_values import item_names
from path import save_data, load_data


class SettingsFrame(ttk.Frame):
    def __init__(self, settings_frame, **kw):
        super().__init__(**kw)
        print("Initializing settings tab")
        self.filters = ["Entry ID", "Item name", "Account", "Status", "Quantity",
                        "GE Price", "Total Value", "Date logged", "Date complete"]
        self.sorts = ["Entry ID", "Item name", "Account", "Status", "Quantity",
                      "GE Price", "Total Value", "Date logged", "Date complete"]
        self.frame = settings_frame
        self.res_path = 'resources/values.dat'
        self.resources = load_data(path=self.res_path)
        self.resources['all_items'] = item_names
        self.accs = self.resources.get('account_list') if self.resources.get('account_list') is not None else []

        self.items = self.resources.get('item_ids') if self.resources.get('item_ids') is not None else []

        self.left_panel = ttk.Frame(self.frame)
        # self.topright_panel = ttk.Frame(self.frame)
        # self.bottomright_panel = ttk.Frame(self.frame)

        # Left Panel
        self.itemname = tk.StringVar()
        self.accountname = tk.StringVar()
        self.lb_itemname = tk.Label(self.left_panel, text="Enter item to selection: ")
        self.combo_itemname = ttk.Combobox(self.left_panel, width=25, textvariable=self.itemname)
        self.btn_add_itemname = tk.Button(self.left_panel, text="Add item", width=12)
        self.btn_del_itemname = tk.Button(self.left_panel, text="Del item", width=12)

        self.lb_accountname = tk.Label(self.left_panel, text="Enter account to selection: ")
        self.combo_accountname = ttk.Combobox(self.left_panel, width=25, textvariable=self.accountname)
        self.btn_add_accountname = tk.Button(self.left_panel, text="Add account", width=12)
        self.btn_del_accountname = tk.Button(self.left_panel, text="Del account", width=12)

        self.left_panel_widgets()
        self.left_panel.grid(row=0, column=0, rowspan=2, sticky='N', padx=10, pady=10)

    def load_data(self):
        msg = "*** LOADING DATA ***"
        print('{a} en {b}'.format(a=self.frame))
        print(msg)

    def save_resources(self):
        self.resources['quantities'] = self.items
        self.resources['accs'] = self.accs
        save_data(self.resources, path=self.res_path)

    ####### LEDGER FRAMES ###########################################################

    # Add entries entry/modify section
    # Add scrollbox for entries entries
    # Add view section
    def left_panel_widgets(self):
        # ROW 0,1: Add item to itemlist
        self.lb_itemname.grid(row=0, column=0, columnspan=2, padx=10, pady=12, sticky='W')
        self.combo_itemname['values'] = self.items
        self.combo_itemname.grid(row=1, column=0, columnspan=1, padx=10, pady=12, sticky='W')
        self.btn_add_itemname.bind('<Button-1>', self.submit_item)
        self.btn_add_itemname.grid(row=1, column=1, padx=10, pady=5, sticky='NW')
        self.btn_del_itemname.bind('<Button-1>', self.delete_item)
        self.btn_del_itemname.grid(row=1, column=2, padx=10, pady=5, sticky='NW')

        # ROW 2,3: Add account to itemlist
        self.lb_accountname.grid(row=2, column=0, columnspan=1, padx=10, pady=12, sticky='W')
        self.combo_accountname.grid(row=3, column=0, columnspan=1, padx=10, pady=12, sticky='W')
        self.combo_accountname['values'] = self.accs
        self.btn_add_accountname.bind('<Button-1>', self.submit_account)
        self.btn_add_accountname.grid(row=3, column=1, padx=10, pady=5, sticky='NW')
        self.btn_del_accountname.bind('<Button-1>', self.delete_account)
        self.btn_del_accountname.grid(row=3, column=2, padx=10, pady=5, sticky='NW')

        # TO-DO: Add combobox for adding/removing preset intervals for graph GUI

    def selected_ledger_ids(self):
        # print(self.lbox_ledger.curselection())
        self.selected_ids = [idx for idx in self.ledger_list.curselection()]
        return self.selected_ids

        ############# BUTTON ACTIONS #################################################################################

    def submit_item(self, event):
        print(self.items)
        item = self.combo_itemname.get()
        if item in self.resources['all_items'] and item not in self.items:
            print("Valid submission!")
            print('Submitting {name}'.format(name=item))
            self.items.append(item)
            print(self.items)
            self.itemname.set("")
            self.combo_itemname['values'] = self.items
            self.save_resources()
        else:
            print("Did not submit item {i}; duplicate or invalid".format(i=item))

    def submit_account(self, event):
        account = self.accountname.get()
        print(self.accs)
        if account not in self.accs:
            print('Submitting {name}'.format(name=account))
            self.accs.append(account)
            self.accountname.set("")
            self.combo_accountname['values'] = self.accs
            self.save_resources()
        else:
            print("Account {a} not added, duplicate!".format(a=account))

    def delete_item(self, event):
        item = self.itemname.get()
        print("Valid removal!")
        print('Removing {name}'.format(name=item))
        print(self.frame.winfo_screenwidth())
        self.items.remove(item)
        self.itemname.set("")
        self.combo_itemname['values'] = self.items
        self.save_resources()

    def delete_account(self, event):
        account = self.accountname.get()
        print('Deleting {name}'.format(name=account))
        self.accs.remove(account)
        self.accountname.set("")
        self.combo_accountname['values'] = self.accs
        self.save_resources()

    def undo(self, event):
        print("Cancel button clicked")  # + self.url_input.get())

    def remove_filter(self, event):
        print("Removing ")  # + self.url_input.get())

    def apply_filter(self, event):
        print("Cancel button clicked")  # + self.url_input.get())

    def add_filter(self, event):
        print("Cancel button clicked")  # + self.url_input.get())

    def buy_checked(self):
        self.buy_entry = not self.buy_entry
        if self.buy_entry:
            print("Buy entry")

            return
