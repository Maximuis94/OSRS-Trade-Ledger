"""
Old inventory code

# TODO: Rework inventory management

"""
import pandas as pd

from model.item import Item

from global_variables.importer import *



# Placeholder entry
def create_inv_item_entry():
    return {
        "buy_price": 0,  # Average buy price
        "sell_price": 0,  # Target price
        "cur_profit": 0,  # Price from OSB
        "quantity": 0,  # Current amount of quantities owned
        "buy_value": 0,  # Total value (bought)
        "sell_value": 0,  # Total value (target)
        "total_profit": 0,  # Absolute profit in GP
        "profit": 0,  # Profit in % increase
        "buy_date": date.datetime.today(),
        "active": False,
        "share": 0,
        "lifetime_profit": 0  # Total profit made trading this item
    }

class InventoryEntry(Item):
    def __init__(self, item_id: int, transactions: pd.DataFrame = None, default_tag: str = 'p'):
        """
        An entry used in the Inventory for each unique item_id. All information regarding the corresponding item_id is
        stored within the InventoryEntry. This information ranges from basic item data to statistics related to
        executing transactions. By design, the InventoryEntry is regulated by the Inventory it is a part of.

        Parameters
        ----------
        item_id : int
            The item_id of the item this object will represent
        transactions: pd.DataFrame, optional
            If passed, it is a dataframe containing transactions that should be logged and executed.

        Attributes
        ----------
        item : Item
            The Item this InventoryEntry

        Methods
        -------
        get_transactions_df(transactions: list = None, status_filter: bool = True): Fetch the transactions logged in
        this InventoryEntry as a chronologically sorted pandas dataframe, where each row represents one Transaction

        Other Parameters
        ----------
        default_tag : str, optional
            Single char to use by default as transaction tag. Note that setting undocumented default tags gets confusing

        Notes
        -----
        By design, the InventoryEntry is an internal element of the Inventory. It is created automatically by the
        Inventory when it encounters a new item and its values are regulated by the Inventory. Each item has its own
        InventoryEntry. Since a Transaction affects up to one item, the set of InventoryEntry objects is a set of
        independent data structures.
        """
        super().__init__(item_id=item_id, from_dict=go.itemdb.get(item_id))
        
        # Values derived from executing transactions
        self.price = 0
        self.quantity = 0
        
        # This dict is log that describes the values after executing the transaction with the corresponding id
        self.execution_log = {}
        self.value = 0
        self.profit = 0
        self.n_purchases = 0
        self.n_sales = 0
        self.newest_ts = 0
        self.ts_sort, self.df_dup = (['timestamp', 'is_buy', 'transaction_id'], [True, False, True]), ['transaction_id']
        self.df, self.transactions = pd.DataFrame, {}
        self.snapshots = {}
        self.executed = []
        # self.transactions_df = pd.DataFrame(transactions, columns=list(Transaction.dtypes().keys())).astype(Transaction.dtypes())
        
        
        # print(self.current_buy, self.current_sell, current_tax)
        # self.current_margin = self.buy_limit
        # if self.margin > 10000000 and self.current_buy > self.current_sell+current_tax:
        #	 print(self.current_buy, self.current_sell, current_tax, self.margin, self.buy_limit)
        # print(f'tax {self.item_name} {max(self.current_buy, self.current_sell)}', min(5000000, int(0.01*max(self.current_buy, self.current_sell))))
        self.current_value = self.quantity * self.current_sell
        self.tax = 0
        self.update_values()
        self.transaction_tag = default_tag
        self.update_ts = int(time.time())
        self.min_queue_ts = int(time.time())
        self.default_values = self.get_values_snapshot()
        if transactions is not None and len(transactions) > 0:
            self.add_transactions(t_new=transactions, execute=False)
            self.execute_transactions(execute_all=True)
        # self.execute_transactions()
        # if transactions is not None:
        # 	if isinstance(transactions, pd.DataFrame):
        # 		transactions = transactions.to_dict('records')
        # 	# self.queue_transaction(t=transactions, execute=True)
        self.last_exe_ts = 0
    
    def add_transactions(self, t_new, execute: bool = True, check_item_ids: bool = False):
        """
        Add transactions to this Entry / overwrite info on existing transactions. Update relevant data structures
        accordingly. Result is an updated self.queue that is ready to be executed with data from self.transactions
        :param t_new: A list/dataframe with new transactions or a single Transaction/dict/df
        :param execute: True if the resulting queue should be executed immediately
        :param check_item_ids: True to check if new transactions actually involve the relevant item_id
        :return:
        """
        add_start = time.time()
        # print('   Adding transactions...')
        try:
            t_new = Transaction.as_df(transactions=t_new)
        except TypeError:
            if t_new.get('transaction_id') == -1:
                self.to_sql()
            
            exit(-1)
        
        # print(f'   Checking ids... (t={time.time()-add_start:.1f})')
        if check_item_ids:
            t_new = t_new.loc[t_new['item_id'] == self.item_id]
        
        new_ids = t_new['transaction_id'].to_list()
        try:
            t_all = Transaction.as_df(pd.concat([t_new, self.df]), drop_dup=True, cast_df=True, sort_df=True)
        except TypeError:
            t_all = t_new
        # self.transactions.update({t.get('transaction_id'): t for t in t_new.to_dict('records')})
        self.queue, self.min_queue_ts = self.queue + new_ids, min(self.min_queue_ts, int(t_new['timestamp'].min()))
        self.df = t_all.copy(deep=True)
        self.transactions = {el.get('transaction_id'): el for el in self.df.to_dict('records')}
        
        # At this point, the queue is filled with to-be-updated ids and ready for processing.
        # min_queue_ts is equal to the lowest timestamp among queued transactions.
        # df is an up-to-date, sorted dataframe
        # transactions is a dict derived from this dataframe, using transaction_id as keys.
        
        # print(f'Self.transactions:')
        # print(self.transactions)
        # print(self.min_queue_ts, len(self.queue))
        # print(self.executed)
        # print(frozenset(new_ids).intersection(self.executed))
        
        # for t_id in frozenset(new_ids).intersection(self.executed):
        for t_id in frozenset(new_ids).intersection(list(self.execution_log.keys())):
            # print(f'Moving {t_id} from self.executed to queue...')
            # self.executed.remove(t_id)
            del self.execution_log[t_id]
        self.executed = list(self.execution_log)
        
        # At this point, self.queue and self.executed no longer contain overlapping values.
        if execute:
            # print(f'   Executing... (t={time.time()-add_start:.1f})')
            self.execute_transactions()
        # print(f'   Executed! (t={time.time()-add_start:.1f})')
    
    def get_transactions_df(self, transactions: list = None, status_filter: bool = True):
        """
        Convert transactions into a chronologically sorted dataframe. Drop duplicate transaction_ids and keep the newest
        transaction. By default, exclude transactions with status=0, unless specified otherwise.
        Resulting Dataframe is a sequence of transactions that can be executed sequentially.
        :param status_filter: True if entries with status=0/False should be omitted
        :return: Chronologically sorted dataframe containing all transactions for this item.
        """
        if transactions is None:
            transactions = self.transactions + self.queue
        df = pd.DataFrame(transactions, columns=list(Transaction.dtypes().keys())).astype(
            dtype=Transaction.dtypes()).sort_values(
            by=['timestamp', 'is_buy', 'update_ts'], ascending=[True, False, True]).drop_duplicates(
            keep='last', subset=['transaction_id'], ignore_index=True)
        # print(df)
        if status_filter:
            df = df.loc[df['status']]
        return df
    
    def update_transactions(self, transactions=None, df: pd.DataFrame = None, merge_transactions: bool = True):
        b, a = self.ts_sort
        if isinstance(transactions, dict):
            transactions = [transactions.get(item) for item in list(transactions.keys())]
        if isinstance(transactions, list):
            temp = pd.DataFrame([(t.__dict__ if isinstance(t, Transaction) else t) for t in transactions])
            df = pd.concat([temp, df]).drop_duplicates(subset=self.df_dup) if isinstance(df, pd.DataFrame) else temp
        df = df.astype(dtype=Transaction.dtypes()).sort_values(by=b, ascending=a)
        
        # Merge transaction data; overwrite old transactions
        if merge_transactions:
            cur = pd.DataFrame([self.transactions.get(t.get('transaction_id')) for t in list(self.transactions.keys())])
            df = df.loc[df['item_id'] == self.item_id]
            df = pd.concat([df, cur, self.df]).drop_duplicates(ignore_index=True, subset=self.df_dup)
        
        self.df = df.sort_values(by=b, ascending=a).astype(dtype=Transaction.dtypes())
        self.transactions = {t.get('transaction_id'): t for t in list(df.to_dict('records'))}
    
    def execute_transactions(self, execute_all: bool = False):
        """
        Sequentially execute all (queued) transactions in chronological order.

        Parameters
        ----------
        execute_all : bool, optional by default False
            If True, execute all transactions rather than the unlogged subset
        """
        self.rollback(0 if execute_all else self.min_queue_ts)
        # self.executed = []
        
        # with open('output/receipt.txt', 'w') as f:
        self.update_ts = int(time.time())
        keys = ['quantity', 'profit', 'price']
        printing = False
        if True:
            for el in Transaction.as_df([self.transactions.get(t_id) for t_id in self.queue]).to_dict('records'):
                # if el.get('item_id') != 567:
                # 	continue
                # Stock correction transaction; process it accordingly
                if el.get('tag') == 'X':
                    el = Transaction.create_from_dict(el)
                    # if el.item_id == 567:
                    # 	printing = True
                    # print(df)
                    # for t in Transaction.as_df([self.transactions.get(t_id) for t_id in self.queue]):
                    key = self.stock_correction(el)
                else:
                    if self.transaction_tag is not None:
                        el['tag'] = self.transaction_tag
                    el = Transaction.create_from_dict(el)
                    key = self.buy(el) if el.is_buy else self.sell(el)
                # if printing:
                # 	print(self.augment_transaction(el.__dict__))
                self.newest_ts = el.timestamp
                self.snapshots[key] = self.get_values_snapshot()
            # self.executed = list(self.execution_log.keys())
            
            # print('\t', len(self.snapshots), len(self.execution_log), len(self.executed))
            # if len(self.executed) > 100000:
            # 	print(self.executed)
            # 	print(np.unique(self.executed))
            # 	print(list(self.execution_log.keys()))
            # 	print(len(self.executed), len(np.unique(self.executed)), len(list(self.execution_log.keys())))
            # 	exit(-1)
            
            # e = self.execution_log.get(el.transaction_id)
            # del e['execution_ts'], e['totals']
            # print(f'[{len(self.executed)}] Executed {el.__dict__}')
            # # del e['']
            # print(f'	{e}')
            # v = self.get_values_snapshot()
            # for k in keys:
            # 	print(f'\t{k} {format_int(v.get(k))}')
            self.executed = list(self.execution_log.keys())
    
    def buy(self, t: Transaction, s: str = None, exe_ts: int = int(time.time())):
        """ Execute a purchase; log new quantity and compute the new weighted average buy price. """
        old_quantity = 0 if self.quantity < 0 else self.quantity
        self.price = (self.price * old_quantity + t.price * t.quantity) / (old_quantity + t.quantity)
        self.quantity += t.quantity
        self.value = self.price * self.quantity if self.quantity > 0 else 0
        self.n_purchases += 1
        self.execution_log[t.transaction_id] = {'balance': self.quantity, 'buy_price': self.price, 'profit': 0,
                                                'tax': 0, 'execution_ts': self.update_ts,
                                                'totals': self.get_values_snapshot()}
        # if s is not None:
        # 	s += f'\tResulting balance: {format_int(self.quantity)} Average price: {format_int(self.price)}\n'
        return t.timestamp
    
    def sell(self, t: Transaction, s: str = None, exe_ts: int = int(time.time())):
        """ Execute a sale; compute and log new quantity, profits and tax """
        tax = 0 if t.timestamp < ge_tax_min_ts else int(min(5000000.0, t.price * 0.01))
        profit = (t.price - self.price - tax) * t.quantity
        self.tax += tax * t.quantity
        self.profit += profit
        self.quantity -= t.quantity
        self.value = self.price * self.quantity if self.quantity > 0 else 0
        self.n_sales += 1
        self.execution_log[t.transaction_id] = {'balance': self.quantity, 'buy_price': self.price, 'profit': profit,
                                                'tax': tax * t.quantity, 'execution_ts': self.update_ts,
                                                'totals': self.get_values_snapshot()}
        return t.timestamp
    
    # Used only in delete_transaction
    def determine_most_recent_transaction(self):
        pass
    
    def csv_row(self):
        return {key: self.__dict__.get(key) for key in self.csv_columns}
    
    def stock_correction(self, t: Transaction):
        """
        Special transaction used to set the stock to the values specified to sync the Inventory with the actual
        inventory. As such, the trade is neither a buy nor a sell (what it is depends on the inventory before
        executing it). Stock corrections are logged as a regular transaction, but are distinguished through their tag
        'X', resulting in a different processing. The submission_status is also used as a post-execution buy price.

        Parameters
        ----------
        t : Transaction
            Stock correction transaction

        Returns
        -------
            Whatever the InventoryEntry method for stock_correction would return with the given args

        Raises
        ------
        ValueError
            If the given item_id is not found in the results dict, a ValueError is raised. This implies you are
            attempting to correct the stock of an item that has not been traded before, which makes no sense.

        See Also
        --------
        Inventory.stock_correction()
            By design, this method is called from the overarching Inventory object.


        """
        if t.tag != 'X':
            raise ValueError(f"A transaction with tag {t.tag} was submitted as a stock correction...")
        # print('stock correction')
        # for k in list(self.__dict__.keys()):
        # 	print('	', k, self.__dict__.get(k))
        # for k in list(t.__dict__.keys()):
        # 	print('	', k, t.__dict__.get(k))
        deficit = self.quantity - t.quantity
        self.quantity = t.quantity
        
        if t.price > 0 and deficit > 0:
            tax = min(5000000, int(0.01 * t.price))
            profit = deficit * (t.price - self.price - tax)
            self.profit += profit
            self.tax += tax
        else:
            tax, profit = 0, 0
        if t.status > 1:
            self.price = t.status
        # for k in list(self.__dict__.keys()):
        # 	print('	', k, self.__dict__.get(k))
        self.execution_log[t.transaction_id] = {'balance': self.quantity, 'buy_price': self.price, 'profit': profit,
                                                'tax': tax * deficit, 'execution_ts': self.update_ts,
                                                'totals': self.get_values_snapshot()}
    
    def augment_transaction(self, transaction: dict):
        """ Augment the given transaction with post-execution data """
        e = self.execution_log.get(transaction.get('transaction_id'))
        if e is not None:
            transaction.update(e)
        return transaction
    
    def get_values_snapshot(self):
        """ Export the subset of values that change after a transaction is executed """
        return {var: self.__dict__.get(var) for var in ('price', 'quantity', 'value',
                                                        'profit', 'n_purchases', 'n_sales', 'newest_ts')}
    
    def rollback(self, timestamp: int = 0, transaction_id: int = None):
        """ Revert the InventoryEntry to the specified timestamp by undoing all transactions newer than `timestamp` """
        if timestamp == 0:
            # Determine timestamp -> recursive call using said timestamp
            if transaction_id is not None:
                return self.rollback(timestamp=self.transactions.get(transaction_id).get('timestamp'))
            
            # Timestamp = 0 and no transaction_id -> Execute all
            self.executed, self.queue, self.execution_log = [], list(self.transactions.keys()), {}
            self.snapshots = {0: self.default_values}
        
        # Transfer transactions from executed to queue, if needed
        if len(self.executed) > 0:
            df = pd.DataFrame([self.transactions.get(t_id) for t_id in self.executed])
            # Timestamp is known -> Add newer transactions to queue + reset values
            for t_id in df.loc[df['timestamp'] > timestamp]['transaction_id'].to_list():
                self.queue.append(t_id)
                self.executed.remove(t_id)
                try:
                    del self.execution_log[t_id]
                    del self.snapshots[self.transactions.get(t_id).get('timestamp')]
                except KeyError:
                    pass
        
        try:
            # Set current values to previously logged snapshot
            self.__dict__.update(self.snapshots.get(timestamp))
        except TypeError:
            print(f'Unable to rollback to snapshot for timestamp {ts_to_dt(timestamp)}')
            self.rollback(0, None)
        
        # Sort the full queue by timestamp and transaction_id
        df = pd.DataFrame([self.transactions.get(t_id) for t_id in self.queue])
        self.queue = df.sort_values(by=self.ts_sort[0], ascending=self.ts_sort[1])['transaction_id'].to_list()
    
    def print(self):
        n_b = len(self.df.loc[self.df['is_buy'] == True])
        n_s = len(self.df) - n_b
        string = f" * Results for item {self.item_name} ({self.item_id})\n" \
                 f" * Current stock: {self.quantity}  Price: {self.price}  Profit: {self.profit}\n" \
                 f" * {len(self.transactions)} have been submitted so far ({n_b}/{n_s} buy/sales)\n"
    
    def log_transaction(self, t, tax, profit):
        pass


class Inventory:
    def __init__(self, ledger: Ledger, import_data: bool = True, export_data: bool = True):
        """
        Centralized data structure for processing transactions. The Inventory has a separate InventoryEntry for each
        item for which a transaction has been logged, the Inventory itself ties all entries together and serves as an
        interface to interact with specific entries. Furthermore, it computes overall statistics for all
        InventoryEntries.

        Parameters
        ----------
        ledger : Ledger
            Transaction ledger this inventory will be built on

        Attributes
        ----------
        ledger : Ledger
            Data structure in which transactions logged into this inventory are stored
        results : dict
            A dict with entries for each item for which at least one Transaction was submitted, stored as InventoryEntry
            objects using the corresponding item_id as key.
        timeline : dict
            Chronologically organized representation of transactions processed. Each entry covers a full day, starting
            at 00:00-23:59 UTC time. It uses the very first unix timestamp of the corresponding day as key.
        total : dict
            A dict with data resulting from all executed transactions

        Methods
        -------
        execute_all(force_execution: bool = False)
            Execute all transactions in this ledger that are logged but not executed, or enforce a full execution
        submit_to_sql(submissions: list = None)
            Submit all transactions logged across the entries to the sqlite database + execute queued transactions
        augment_transactions(df: pd.DataFrame, columns: list = None)
            Augment transactions stored in the dataframe with values resulting from inventory executions

        Other Parameters
        ----------
        import_data : bool, optional
            Flag to allow the inventory to import a previously computed setup, rather than to submit all transactions
            again (True by default)
        export_data : bool, optional
            Flag to allow the inventory to export its data after submitting transactions, as to prevent executing the
            same transactions multiple times
       """
        t_start = time.time()
        self.export_data, self.export_path, self.synced = export_data, p.f_inventory_export, False
        
        # Ledger object: Get data / submit data
        self.ledger = ledger
        self.ledger.sync_transactions(prioritize_sql=True)
        self.dup_columns = ['transaction_id', 'update_ts']
        
        try:
            self.stock_corrections = load_data(p.f_stock_corrections)
        except FileNotFoundError:
            self.stock_corrections = []
        
        # Results: One entry for each item; an entry describes the results for said item
        if os.db_file.exists(self.export_path) and import_data:
            self.results, timeline = self.import_entries()
        else:
            self.results = {item_id: InventoryEntry(item_id=item_id,
                                                    transactions=self.ledger.transactions.loc[
                                                        self.ledger.transactions['item_id'] == item_id
                                                        ].to_dict('records')) for item_id in
                            np.unique(self.ledger.transactions['item_id'].to_numpy())}
            timeline = None
        
        # print(self.bond_stats)
        
        # Exclude bonds from inventory, but keep track of spendings
        try:
            self.bonds = self.results.get(13190)
            self.bond_stats = {'n_bought': self.bonds.quantity, 'price': self.bonds.price, 'value': self.bonds.value}
        except KeyError:
            pass
        except AttributeError:
            self.bonds = InventoryEntry(item_id=13190, transactions=None)
            self.bond_stats = {
                'n_bought': 0, 'price': 0, 'value': 0
            }
        
        # Determine values for entire inventory
        self.df = pd.DataFrame(
            [self.results.get(item_id).csv_row() for item_id in list(self.results.keys()) if item_id != 13190])
        self.df['value'] = self.df['price'] * self.df['quantity']
        # df = self.df.loc[self.df['item_id'] != 13190]
        self.total = {col: int(np.sum(self.df[col].to_numpy())) for col in self.df.columns}
        self.timeline = self.create_timeline(timeline=timeline, results=self.results)
        invested, returns = 0, 0
        for ts in list(self.timeline.keys()):
            invested += int(self.timeline.get(ts).get('stats').get('invested'))
            returns += int(self.timeline.get(ts).get('stats').get('returns'))
        self.total['invested'] = invested
        self.total['returns'] = returns
        # print(self.timeline.keys())
        self.t0, self.t1 = min(list(self.timeline.keys())), int(time.time() // 86400 * 86400 + 86399)
        self.max_sql_id = self.ledger.get_max_id()
        self.n_days = (self.t1 - self.t0) // 86400
        
        # for kt in list(self.total.keys()):
        # 	print(kt, self.total.get(kt))
        print(f'n_days:', self.n_days, ts_to_dt(self.t0), ts_to_dt(self.t1))
        print(f'Time taken: {int(1000 * (time.time() - t_start))}ms')
        
        self.exe_ts = int(time.time())
        self.execute_all(force_execution=True)
        self.export_entries()
    
    def execute_transaction(self, transaction):
        """ Execute a transaction for this inventory """
        # print(f'   Executing transaction...')
        ie = self.results.get(transaction.item_id)
        if ie is None:
            ie = InventoryEntry(item_id=transaction.item_id)
            print(f"Created entry for item", id_name[transaction.item_id])
        self.max_sql_id += 1
        transaction.transaction_id = self.max_sql_id
        # print(transaction.__dict__)
        ie.add_transactions(t_new=transaction, execute=True)
        self.results[transaction.item_id] = ie
        print(f'   Transaction was executed and logged!\n')
        return transaction
    
    def execute_all(self, force_execution: bool = False):
        """ Chronologically execute all transactions, but only if there are new transactions (unless it is forced) """
        if self.exe_ts > os.db_file.getmtime(self.ledger.db_file) or force_execution:
            return
        self.ledger.sync_transactions(prioritize_sql=True)
        if not self.ledger.synced_inv or force_execution:
            self.__init__(ledger=self.ledger, import_data=False, export_data=self.export_data)
            self.ledger.synced_inv = True
    
    def submit_to_sql(self, submissions: list = None):
        """ Fetch all queued transactions and submit them to the sql database.	"""
        if submissions is None:
            submissions = []
        for item_id, ie in self.results.items():
            if not isinstance(ie, InventoryEntry):
                continue
            # Add submissions from queue to submissions list
            submissions += ie.queue
        # Submit all submissions to sqlite database
        self.ledger.submit_transactions_list(submissions)
        
        # Execute all transactions again
        self.execute_all()
    
    def augment_transactions(self, df: pd.DataFrame, columns: list = ('balance', 'buy_price', 'profit', 'tax')):
        """
        Augment a dataframe with transactions with execution results balance, buy_price, profit, tax.

        Parameters
        ----------
        df : pandas.DataFrame
            The to-be augmented DataFrame
        columns :  list, optional by default ('balance', 'buy_price', 'profit', 'tax')
            A list of values that should be added to each transaction.

        Returns
        -------
        The augmented transaction object as a pandas.DataFrame

        """
        df['value'] = df['price'] * df['quantity']
        balances = []
        # Augment non-bond transactions with post-transaction data (e.g. resulting buy price, profit generated)
        for t in df.loc[df['item_id'] != 13190].sort_values(by=['item_id', 'timestamp'],
                                                            ascending=[True, False]).to_dict('records'):
            e = self.results.get(t.get('item_id')).execution_log.get(t.get('transaction_id'))
            t.update({k: int(e.get(k)) if isinstance(e.get(k), float) else e.get(k) for k in columns if
                      e.get(k) is not None})
            balances.append(t)
        return Transaction.as_df(pd.DataFrame(balances).fillna(0))
    
    def create_timeline(self, timeline: dict = None, results: dict = None):
        """ Group transactions per 24 hours. Use UTC time for the definition of a full day """
        
        today = int(time.time())
        today = today - today % 86400 + 86400
        if isinstance(timeline, dict):
            cur_day = max(list(timeline.keys()))
            # print(f"Resuming timeline from ts {ts_to_dt(cur_day)}")
            # print(timeline.keys())
            if results is not None:
                self.results = results
        else:
            print('Creating new timeline')
            cur_day = dt_to_ts(datetime.datetime.now() - datetime.timedelta(seconds=int(time.time() - 1580515200)))
            timeline = {}
        df = self.augment_transactions(df=self.ledger.transactions, columns=['balance', 'buy_price', 'profit', 'tax'])
        df['value'] = df['price'] * df['quantity']
        while cur_day <= today:
            t0 = cur_day if isinstance(cur_day, int) else int(cur_day.timestamp())
            t1 = t0 + 86399
            # print(cur_day, t0, t1)#datetime.timedelta(seconds=86399)))
            subset = df.loc[(df['timestamp'] >= t0) & (df['timestamp'] < t1)]
            if len(subset) == 0:
                cur_day += datetime.timedelta(seconds=86400) if isinstance(cur_day, datetime.datetime) else 86400
                continue
            b, s = subset.loc[subset['is_buy'] == True], subset.loc[subset['is_buy'] == False]
            timeline[t0] = {
                'subset': subset,
                'stats': {
                    'date': format_ts(t0, str_format='%d-%m-%y'), 't0': t0, 't1': t1,
                    'n_buy': len(b), 'invested': sum(b['value'].to_list()),
                    'n_sell': len(s), 'returns': sum(s['value'].to_list()),
                    'profit': sum(subset['profit'].to_list()), 'tax': sum(subset['tax'].to_list())
                }
            }
            cur_day += 86400 if isinstance(cur_day, int) else datetime.timedelta(seconds=86400)
        return timeline
    
    def import_entries(self):
        """ Import previously exported Inventory attributes and submit new transactions, if applicable """
        t = time.time()
        try:
            data = load_data(self.export_path)
            timeline = data.get('timeline')
            imported_submits = Transaction.as_df(data.get('transactions'))
            entries = data.get('entries')
            results = {}
        except FileNotFoundError:
            return {}, {}
        
        for item in list(entries.keys()):
            e = InventoryEntry(item_id=item, transactions=None)
            e.__dict__.update(entries.get(item))
            results[item] = e
        
        # Remove identical rows, overwrite the resulting subset with up-to-date entries.
        to_submit = pd.concat([imported_submits, self.ledger.transactions]).drop_duplicates(keep=False,
                                                                                            subset=['transaction_id',
                                                                                                    'update_ts'])
        to_submit = to_submit.drop_duplicates(subset=['transaction_id'], keep='last', ignore_index=True)
        
        new_transactions = 0
        # print(min_submit_ts)
        # Submit new or modified transactions
        for item in np.unique(to_submit['item_id'].to_numpy()):
            subset = to_submit.loc[to_submit['item_id'] == int(item)]
            # self.execute_transaction(transaction=subset)
            ie = results.get(item)
            if not isinstance(ie, InventoryEntry):
                ie = InventoryEntry(item_id=item, transactions=None)
            ie.add_transactions(t_new=subset, execute=True)
            results[item] = ie
            new_transactions += len(subset)
            print(f'\tSubmitted {len(subset)} new/modified transactions for item {id_name[item]}')
        
        if new_transactions > 0:
            min_submit_ts = int(to_submit['timestamp'].min())
            timeline = {ts: timeline.get(ts) for ts in list(timeline.keys()) if ts < min_submit_ts}
            timeline = self.create_timeline(timeline=timeline, results=results)
        
        print(f'Imported inventory entries from {self.export_path} in {1000 * (time.time() - t):.0f}ms')
        print(f" * A total of {new_transactions} new/modified transactions were submitted")
        if new_transactions > 0:
            self.export_entries(results=results, timeline=timeline)
            self.synced = True
        return results, timeline
    
    def export_entries(self, results=None, timeline=None):
        """ Export the inventory entries as builtin data types. """
        if self.export_data:
            if results is None:
                results = self.results
            if timeline is None:
                timeline = self.timeline
            exported = {
                'entries': {item: results.get(item).__dict__ for item in list(results.keys())},
                'transactions': Transaction.as_df(self.ledger.transactions).to_dict('records'),
                'timeline': timeline
            }
            save_data(exported, path=p.f_inventory_export)
            print(f'Exported inventory data to file ./{self.export_path[-25:]} '
                  f'(size={format_n(os.db_file.getsize(self.export_path))}b)', os.db_file.getsize(self.export_path))
        
    