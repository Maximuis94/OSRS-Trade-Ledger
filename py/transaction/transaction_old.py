"""
Controller class for transaction db

querying transaction: use cases
1. Search for transactions with specific IDs
2. Search for transactions of a specific item
3. Search for transactions within a designated period of time
4. Search for transactions with a specific tag
5. ...

"""

# class TransactionController(Database, metaclass=SingletonMeta):
#     """
#     Controller for Transactions.
#
#     Fetch methods have been specified for specific purposes, although they can return more or less the same, depending
#     on the keyword args specified.
#
#     All fetch methods accept the following keyword args;
#     t0: int = lower bound timestamp
#     t1: int = upper bound timestamp
#     item_ids: int or Iterable = One or more item_ids
#     transaction_ids: int or Iterable = One or more transaction_ids
#     tags: str or Iterable = One or more tags
#     chrono_sort_asc: bool = True -> ORDER BY timestamp ASCENDING; False -> ORDER BY timestamp DESCENDING
#     For the exact implementation, see the generate_sql() method
#
#
#     Outside of direct database interactions, specific rows are defined as Transaction objects.
#     """
#     db_path = gp.f_db_transaction
#     table_name = 'transaction'
    # row = TransactionRow()
    # select_by_id = """SELECT * FROM "transaction" WHERE transaction_id=?"""
    # select_by_ids = """SELECT * FROM "transaction" WHERE transaction_id IN """
    # select_by_item = """SELECT * FROM "transaction" WHERE item_id=?"""
    #
    # def __init__(self, **kwargs):
    #     super().__init__(path=self.db_path, read_only=True)
    #     self.table = self.tables.get(self.table_name)
    #     if len(self.tables) > 1:
    #         self.tables = {self.table_name: self.table}
    #     self.types = {c: gd.types.get(c) for c in list(self.table.column_list.keys())}
    #     self.sql_insert = sqlite.sql_exe.sql_insert(row=self.types, table=self.table_name, replace=False)
    #     self.sql_insert_replace = self.sql_insert.replace('INSERT INTO', 'INSERT OR REPLACE INTO')
    #
    #     self.submission_queue = {}
    #     self.row_factory = factory_transaction_subclass
    #
    #     # Connection used for fetching transaction_id
    #     self.con_t_id = None
    #
    # def parse_exchange_logger(self):
    #     """  """
    #     ...
    #
    # def fetch_by_id(self, transaction_id: int or Iterable) -> Transaction or List[Transaction]:
    #     """ Get a single Transaction from the database with id=`transaction_id` """
    #     if isinstance(transaction_id, int):
    #         return self.execute(self.select_by_id, (transaction_id,)).fetchone()
    #     else:
    #         return self.execute(self.select_by_ids+str(tuple(transaction_id)), ()).fetchall()
    #
    # def fetch_by_item(self, item_id: int) -> List[Transaction]:
    #     return self.execute(self.select_by_item, (item_id,)).fetchall()
    #
    # def fetch_transactions(self, **kwargs):
    #     """ Fetch transactions from the database. See generate_sql() for parameter specifications. """
    #     sql, parameters = self.generate_sql(**kwargs)
    #     try:
    #         return self.execute(sql, parameters).fetchall()
    #     except sqlite3.Error as e:
    #         print('sqlite3 Error occurred in TransactionController.fetch_transactions()\n'
    #               f'\tExecuted sql: {sql}\n'
    #               f'\tError: {e}')
    #         raise e
    #
    # def verify_transaction(self, t: Transaction):
    #     """ Verify value types of all attributes of transaction `t` """
    #     for k, v in t.__dict__.items():
    #         if k == 'item':
    #             v = create_item(t.item_id)
    #             t.item_id = v.item_id
    #         elif not isinstance(v, self.types.get(k)):
    #             raise TypeError(f'Transaction attribute {k}={v} does not have expected type {self.types.get(k)}')
    #     return True
    #
    # def submit(self, t: Transaction or dict):
    #     """ Submit+commit all transactions from the submission queue to the sqlite database """
    #     if isinstance(t, Transaction):
    #         t = t.__dict__
    #     self.execute(self.sql_insert_replace, t)
    #
    # def verify_and_submit(self, t: Transaction):
    #     """ Submit Transaction `t`, but only if its attribute types have been successfully verified """
    #     try:
    #         if self.verify_transaction(t):
    #             self.submit(t)
    #             return True
    #     except TypeError:
    #         print(f'Failed to submit transaction {t.__dict__} due to an attribute type mismatch')
    #     return False
    #
    # def get_next_transaction_id(self) -> int:
    #     """ Return the next transaction_id to use when creating a new transaction """
    #     if self.con_t_id is None:
    #         self.con_t_id = sqlite3.connect(self.database_arg, uri=True)
    #         self.con_t_id.row_factory = sqlite.row_factories.factory_single_value
    #     return self.con_t_id.execute(f"SELECT MAX(transaction_id) FROM {self.table_name}").fetchone() + 1
    #
    # def insert_transaction(self, transaction: Transaction, allow_update: bool = True):
    #     """ Insert `transaction` into the database. Only overwrite if `allow_update`=True. """
    #     con = sqlite3.connect(self.db_path)
    #     try:
    #         con.execute(self.sql_insert, transaction.sql_row())
    #     # Transaction with this id already exists
    #     except sqlite3.OperationalError as sqlite_error:
    #         if allow_update:
    #             self.update_transaction(transaction.transaction_id, transaction.sql_row())
    #         else:
    #             raise sqlite_error
    #
    # def create_transaction(self, item_id: int, timestamp: int, is_buy: bool, quantity: int,
    #                        price: int, status: int, tag: str):
    #     transaction_id = self.get_next_transaction_id()
    #     t_dict = dict(locals())
    #     t_dict['update_ts'] = int(time.time())
    #
    #     transaction = transaction_from_dict({var: t_dict.get(var) for var in Transaction.columns})
    #     # self.insert_transaction()
    #
    # def update_transaction(self, transaction_id: int, updated_values: dict):
    #     """ Fetch existing data from database for the transaction, update with `updated_values`  and submit it. """
    #     transaction = self.fetch_by_id(transaction_id=transaction_id)
    #     transaction.update_transaction(values=updated_values)
    #     con = sqlite3.connect(self.db_path)
    #     self.execute(self.sql_insert_replace, transaction.sql_row())
    #     con.commit()
    #     con.close()
    #
    # def generate_sql(self, transaction_ids=None, item_ids=None, t0: int = None, t1: int = None, tags=None,
    #                  chrono_sort_asc: bool = None, **kwargs) -> (str, dict):
    #     """
    #     Construct a sqlite select query, based on the args passed. If an arg is set to None (i.e. not specified), it
    #     will not be added to the resulting sqlite statement. If nothing is specified, all transactions are returned.
    #
    #     Parameters
    #     ----------
    #     transaction_ids : int or Iterable, optional, None by default
    #         One or more transaction ids to include in the select query
    #     item_ids :  int or Iterable, optional, None by default
    #         One or more item ids to include in the select query
    #     t0 : int, optional, None by default
    #         Lower bound timestamp to include in the select query (inclusive)
    #     t1 : int, optional, None by default
    #         Upper bound timestamp to include in the select query (inclusive)
    #     tags : str or Iterable, optional, None by default
    #         One or more transaction tags to include in the select query
    #     chrono_sort_asc : bool, optional, None by default
    #         If passed, order the fetched transactions by timestamp in ascending (True) or descending (False) order
    #
    #     Returns
    #     -------
    #     str, dict
    #         An executable sql statement and the parameter dict required to execute it
    #
    #     """
    #     sql, parameters = f"SELECT * FROM {self.table_name} WHERE ", {}
    #
    #     for column, values in zip(['transaction_id', 'item_id', 'tag'], [transaction_ids, item_ids, tags]):
    #         # Allows for passing 'item_id' instead of 'item_ids'
    #         if values is None and kwargs.get(column) is not None:
    #             values = kwargs.get(column)
    #
    #         if values is not None:
    #             sql += f"{sqlite.sql_exe.where_clause_by_column(column_name=column, values=values)} AND "
    #             if f':{column}' in sql:
    #                 parameters[column] = values
    #
    #     if t0 is not None or t1 is not None:
    #         sql += f"{sqlite.sql_exe.where_clause_between(lower_bound=t0, upper_bound=t1)} AND "
    #         if t0 is not None:
    #             parameters['timestamp_0'] = t0
    #         if t1 is not None:
    #             parameters['timestamp_1'] = t1
    #
    #     sql = sql[:-5] if sql[-4:] == 'AND ' else sql[:-7]
    #
    #     if chrono_sort_asc is not None:
    #         sql += f" ORDER BY timestamp {'A' if chrono_sort_asc else 'DE'}SC"
    #
    #     return sql, parameters
        

if __name__ == '__main__':
    ...
    