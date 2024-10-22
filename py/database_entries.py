import sqlite3
import pandas as pd

'''
This file contains Entry objects for each database table, these entries are used to
modify (insert, update, delete) entries in the database.
3 additional function calls are used to insert/update/delete an array of database entries at once, opening,
saving and closing the database as well.
'''
def create_entries(entries, db_name="Test.db"):
    con = sqlite3.connect(db_name)
    cursor = con.cursor()
    for e in entries:
        e.insert(cursor)
    con.commit()
    con.close()


def update_entries(entries, db_name="Test.db"):
    con = sqlite3.connect(db_name)
    cursor = con.cursor()
    for e in entries:
        e.update(cursor)
    con.commit()
    con.close()


def insert_or_update_entries(entries, db_name):
    con = sqlite3.connect(db_name)
    cursor = con.cursor()
    for e in entries:
        try:
            e.insert(cursor)
        except sqlite3.IntegrityError:
            e.update(cursor)
    con.commit()
    con.close()


def delete_entries(entries, db_name="Test.db"):
    con = sqlite3.connect(db_name)
    cursor = con.cursor()
    for e in entries:
        e.delete(cursor)
    con.commit()
    con.close()


class EntryItemDB:
    def __init__(self, item_id=None, name=None, members=None, alch_value=None, buy_limit=None, release_date=None,
                 stackable=None, equipable=None, weight=None):
        """
        Create an entry object for itemDB table
        Eventueel ipv argumenten item dict meegeven (TO-DO)
        :param item_id: OSRS item ID
        :param name: Item name that corresponds to the Item ID
        :param members: 1 if it's a members item, 0 if not
        :param alch_value: High-alchemy value of this item
        :param buy_limit: The GE buy-limit for this item
        :param release_date: The release date of this item as epoch time
        :param stackable: 1 if this item is stackble, 0 if not
        :param weight: The weight of this item (float)
        """
        self.item_id = item_id
        self.name = name
        self.members = members
        self.alch_value = alch_value
        self.buy_limit = buy_limit
        self.release_date = release_date
        self.stackable = stackable
        self.equipable = equipable
        self.weight = weight

    def insert(self, cursor):
        values = (self.item_id, self.name, self.members, self.alch_value,
                  self.buy_limit, self.release_date, self.stackable, self.equipable, self.weight)
    # sql = f"INSERT INTO itemdb(item_id, name, members, alch_value, buy_limit, release_date, stackable, equipable, weight) " \
    #       f"VALUES({self.item_id}, '{self.name}\', {self.members}, {self.alch_value}, {self.buy_limit},
    #       {self.release_date}, {self.stackable}, {self.equipable}, {self.weight})"
        try:
            cursor.execute('''INSERT INTO itemdb(item_id, name, members, alch_value, buy_limit, release_date, stackable,
             equipable, weight) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', values)
        except:
            print("An exception occurred while attempting to insert the following values:")
            print(values)

    def update(self, cursor):
        values = []
        values_string = "UPDATE itemdb SET"
        if self.name is not None:
            values.append(self.name)
            values_string += f" name=?,"
        if self.alch_value is not None:
            values.append(self.alch_value)
            values_string += f"alch_value=?,"
        if self.members is not None:
            values.append(self.members)
            values_string += f"members=?,"
        if self.buy_limit is not None:
            values.append(self.buy_limit)
            values_string += f"buy_limit=?,"
        if self.release_date is not None:
            values.append(self.release_date)
            values_string += f"release_date=?,"
        if self.stackable is not None:
            values.append(self.stackable)
            values_string += f"stackable=?,"
        if self.equipable is not None:
            values.append(self.equipable)
            values_string += f"equipable=?,"
        if self.weight is not None:
            values.append(self.weight)
            values_string += f"weight=?,"
        values_string = values_string[:-1] + f" WHERE item_id=?"
        values.append(self.item_id)
        cursor.execute(values_string, values)

    def delete(self, cursor):
        sql = f"DELETE FROM itemdb WHERE item_id = {self.item_id}"
        cursor.execute(sql)

    def as_dict(self):
        return {
            'item_id': self.item_id,
            'item_name': self.name,
            'members': self.members == 1,
            'alch_value': self.alch_value,
            'buy_limit': self.buy_limit,
            'stackable': self.stackable == 1,
            'release_date': self.release_date,
            'equipable': self.equipable == 1}


class EntryTransactionDB:
    def __init__(self, transaction_id: int, item_id: int, timestamp: int, transaction_type: str, quantity: int,
                 price: int, status: int, tag: str = ""):
        """
        Create an entry object for transaction table
        :param transaction_id: A unique transaction ID
        :param item_id: The item ID of the item involved in the transaction
        :param timestamp: The timestamp of the transaction (epoch time)
        :param transaction_type: B if the item is bought, S if it is sold
        :param quantity: The amount of item(s) that are bought/sold
        :param price: The price for which the item is bought/sold
        :param status: Transaction completion status (0=planned, 1=in progress, 2=completed)
        :param tag: Tag that described how the transaction was created (Parsed=p/Manual=m/Correction=c/Transferred=t)
        """
        self.transaction_id = transaction_id
        self.item_id = item_id
        self.timestamp = timestamp
        self.transaction_type = transaction_type
        self.quantity = quantity
        self.price = price
        self.status = status
        self.tag = tag

    def insert(self, cursor):
        sql = f"INSERT INTO transactions(transaction_id, item_id, timestamp, transaction_type, quantity, price, status, tag) " \
              f"VALUES({self.transaction_id}, {self.item_id}, {self.timestamp}, '{self.transaction_type}', " \
              f"{self.quantity}, {self.price}, {self.status}, '{self.tag}')"
        cursor.execute(sql)

    def update(self, cursor):
        sql = f"UPDATE transactions SET"
        if self.item_id is not None:
            sql += f" item_id = {self.item_id},"
        if self.timestamp is not None:
            sql += f" timestamp = {self.timestamp},"
        if self.transaction_type is not None:
            sql += f" transaction_type = '{self.transaction_type}',"
        if self.quantity is not None:
            sql += f" quantity = {self.quantity},"
        if self.price is not None:
            sql += f" price = {self.price},"
        if self.status is not None:
            sql += f" status = {self.status},"
        if self.tag is not None:
            sql += f" tag = '{self.tag}',"
        sql = sql[:-1] + f" where transaction_id = {self.transaction_id}"
        cursor.execute(sql)

    def delete(self, cursor):
        sql = f"DELETE FROM transactions WHERE transaction_id = {self.transaction_id}"
        cursor.execute(sql)


class EntryInventoryDB:
    def __init__(self, item_id=None, buy_price=None, quantity=None):
        """
        An entry object for the inventory table
        :param item_id: The OSRS item ID
        :param buy_price: The average buy price for all item_id items in the inventory
        :param balance: The amount of item_id currently in the inventory
        """
        self.item_id = item_id
        self.buy_price = buy_price
        self.quantity = quantity

    def insert(self, cursor):
        sql = f"INSERT INTO inventory(inventory_id, item_id, buy_price, quantity) " \
              f"VALUES({self.item_id}, {self.item_id}, {self.buy_price}, {self.quantity})"
        cursor.execute(sql)

    def update(self, cursor):
        sql = f"UPDATE inventory SET"
        if self.buy_price is not None:
            sql += f" buy_price = {self.buy_price},"
        if self.quantity is not None:
            sql += f" quantity = '{self.quantity}',"
        sql = sql[:-1] + f" where inventory_id = {self.item_id}"
        cursor.execute(sql)

    def delete(self, cursor):
        sql = f"DELETE FROM inventory WHERE inventory_id = {self.item_id}"
        cursor.execute(sql)


class EntryUpdatelogDB:
    def __init__(self, item_id=None, osb180_ts=None, osb1440_ts=None, osb4320_ts=None, wiki_ts=None, osb180_url=None, osb1440_url=None, osb4320_url=None, wiki_url=None):
        """
        An entry object for the updatelog table
        :param item_id: OSRS Item ID
        :param osb180_ts: timestamp of last update time of OSB180 data for item_id
        :param osb1440_ts: timestamp of last update time of OSB1440 data for item_id
        :param osb4320_ts: timestamp of last update time of OSB4320 data for item_id
        :param wiki_ts: timestamp of last update time of Wiki data for item_id
        :param osb180_url:  URL for accessing online data from OSB180 source for item_id
        :param osb1440_url: URL for accessing online data from OSB1440 source for item_id
        :param osb4320_url: URL for accessing online data from OSB4320 source for item_id
        :param wiki_url: URL for accessing online data from Wiki source for item_id
        """
        self.item_id = item_id
        self.osb180_ts = osb180_ts
        self.osb180_url = osb180_url
        self.osb1440_ts = osb1440_ts
        self.osb1440_url = osb1440_url
        self.osb4320_ts = osb4320_ts
        self.osb4320_url = osb4320_url
        self.wiki_ts = wiki_ts
        self.wiki_url = wiki_url

    def insert(self, cursor):
        values = (self.item_id, self.item_id, self.osb180_ts, self.osb180_url, self.osb1440_ts, self.osb1440_url, self.osb4320_ts, self.osb4320_url, self.wiki_ts, self.wiki_url)
        sql = """INSERT INTO updatelog(entry_id, item_id, ts_updated_osb180, url_osb180, ts_updated_osb1440, url_osb1440, ts_updated_osb4320, url_osb4320, ts_updated_wiki, url_wiki) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        cursor.execute(sql, values)

    def update(self, cursor):
        sql = f"UPDATE updatelog SET"
        if self.osb180_ts is not None:
            sql += f" ts_updated_osb180 = {self.osb180_ts},"
        if self.osb180_url is not None:
            sql += f" url_osb180 = '{self.osb180_url}',"
        if self.osb1440_ts is not None:
            sql += f" ts_updated_osb1440 = {self.osb1440_ts},"
        if self.osb1440_url is not None:
            sql += f" url_osb1440 = '{self.osb1440_url}',"
        if self.osb4320_ts is not None:
            sql += f" ts_updated_osb4320 = {self.osb4320_ts},"
        if self.osb4320_url is not None:
            sql += f" url_osb4320 = '{self.osb4320_url}',"
        if self.wiki_ts is not None:
            sql += f" ts_updated_wiki = {self.wiki_ts},"
        if self.wiki_url is not None:
            sql += f" url_wiki = '{self.wiki_url}',"
        sql = sql[:-1] + f" where entry_id = {self.item_id}"
        cursor.execute(sql)

    def delete(self, cursor):
        sql = f"DELETE FROM updatelog WHERE entry_id = {self.item_id}"
        cursor.execute(sql)


class EntryWikiDB:
    def __init__(self, item_id, timestamp, price=None, volume=None):
        """
        Entry object for Wiki prices table, entry_id is defined as item_id + timestamp
        :param item_id: OSRS item ID
        :param timestamp: Timestamp of the specific entry as epoch time
        :param price: Price of item_id on timestamp
        :param volume: Volume of item_id on timestamp
        """
        self.entry_id = f'{item_id}{timestamp}'
        self.item_id = item_id
        self.timestamp = timestamp
        self.price = price
        self.volume = volume

    def insert(self, cursor):
        sql = f"INSERT INTO wiki(id, item_id, timestamp, price, volume) " \
              f"VALUES({self.entry_id}, {self.item_id}, {self.timestamp}, {self.price}, {self.volume})"
        cursor.execute(sql)

    def update(self, cursor):
        sql = f"UPDATE wiki SET id = {self.entry_id}, item_id = {self.item_id}, timestamp = {self.timestamp},"
        if self.price is not None:
            sql += f" price = {self.price},"
        if self.volume is not None:
            sql += f" volume = {self.volume},"
        sql = sql[:-1] + f" where id = {self.entry_id}"
        cursor.execute(sql)

    def delete(self, cursor):
        sql = f"DELETE FROM wiki WHERE id = {self.entry_id}"
        cursor.execute(sql)

    def as_df(self):
        output = pd.DataFrame({
            'entry_id': self.entry_id,
            'item_id': self.item_id,
            'timestamp': self.timestamp,
            'price': self.price,
            'volume': self.volume
        })
        return output


class EntryOSBDB:
    def __init__(self, table, item_id, timestamp, buy_price=None, buy_quantity=None, sell_price=None, sell_quantity=None):
        """
        Entry object for all OSB price tables, entry_id is defined as item_id + timestamp
        :param table: OSB source (osbsummary, osb180, osb1440, osb4320)
        :param item_id: OSRS item ID
        :param timestamp: Timestamp of the specific entry as epoch time
        :param buy_price: Buy price of specific entry as epoch time
        :param buy_quantity: Buy quantity of specific entry as epoch time
        :param sell_price: Sell price of specific entry as epoch time
        :param sell_quantity: Sell quantity of specific entry as epoch time
        """
        self.table = table
        self.entry_id = f'{item_id}{timestamp}'
        self.item_id = item_id
        self.timestamp = timestamp
        self.buy_price = buy_price
        self.buy_quantity = buy_quantity
        self.sell_price = sell_price
        self.sell_quantity = sell_quantity

    def insert(self, cursor):
        sql = f"INSERT INTO {self.table}(id, item_id, timestamp, buy_price, buy_quantity, sell_price, sell_quantity) " \
              f"VALUES({self.entry_id}, {self.item_id}, {self.timestamp}, {self.buy_price}, {self.buy_quantity}, {self.sell_price}, {self.sell_quantity})"
        cursor.execute(sql)

    def update(self, cursor):
        sql = f"UPDATE {self.table} SET id = {self.entry_id}, item_id = {self.item_id}, timestamp = {self.timestamp},"
        if self.buy_price is not None:
            sql += f" buy_price = {self.buy_price},"
        if self.buy_quantity is not None:
            sql += f" buy_quantity = {self.buy_quantity},"
        if self.sell_price is not None:
            sql += f" sell_price = {self.sell_price},"
        if self.sell_quantity is not None:
            sql += f" sell_quantity = {self.sell_quantity},"
        sql = sql[:-1] + f" where id = {self.entry_id}"
        cursor.execute(sql)

    def delete(self, cursor):
        sql = f"DELETE FROM {self.table} WHERE id = {self.entry_id}"
        cursor.execute(sql)


def unix_to_avg5m(unix_ts):
    return (unix_ts - 1615194000) / 300


def avg5m_to_unix(avg5m_ts):
    return avg5m_ts * 300 + 1615194000


def decode_avg5m_entry_id(index, unix_ts: bool = True):
    #       item_id             timestamp
    return int(index // 1000000), int(avg5m_to_unix(index % 1000000) if unix_ts else int(index % 1000000))


def encode_avg5m_entry_id(item_id: int or str, timestamp):
    return int(item_id) * 1000000 + unix_to_avg5m(timestamp)


class EntryAvg5mDB:
    def __init__(self, item_id, timestamp, buy_price=None, buy_volume=None, sell_price=None, sell_volume=None):
                 # res: Resources=None):
        """
        Entry object for Wiki prices table, entry_id is defined as item_id + timestamp
        :param item_id: OSRS item ID
        :param timestamp: Timestamp of the specific entry as epoch time
        :param buy_price: Price of item_id on timestamp
        :param buy_volume: Volume of item_id on timestamp
        :param sell_price: Price of item_id on timestamp
        :param sell_volume: Volume of item_id on timestamp
        """
        self.table_id = 1
        self.entry_id = encode_avg5m_entry_id(item_id, timestamp)
        self.item_id = item_id
        self.timestamp = timestamp
        self.buy_price = buy_price
        self.buy_volume = buy_volume
        self.sell_price = sell_price
        self.sell_volume = sell_volume
        self.added = False

    def insert(self, cursor):
        sql = f"INSERT INTO {self.table_id}(id, buy_price, buy_volume, sell_price, sell_volume) " \
              f"VALUES(?, ?, ?, ?, ?)"
        values = (self.entry_id, self.buy_price, self.buy_volume, self.sell_price, self.sell_volume)
        # print(sql)
        cursor.execute(sql, values)
        self.added = True

    def update(self, cursor):
        sql = f"UPDATE {self.table_id} SET id = {self.entry_id},"
        if self.buy_price is not None:
            sql += f" buy_price = {self.buy_price},"
        if self.buy_volume is not None:
            sql += f" buy_volume = {self.buy_volume},"
        if self.sell_price is not None:
            sql += f" buy_price = {self.sell_price},"
        if self.sell_volume is not None:
            sql += f" buy_volume = {self.sell_volume},"
        sql = sql[:-1] + f" where id = {self.entry_id}"
        cursor.execute(sql)
        self.added = True

    def delete(self, cursor):
        sql = f"DELETE FROM {self.table_id} WHERE id = {self.entry_id}"
        cursor.execute(sql)

    def as_df(self):
        columns = []
        # output = pd.DataFrame(data=None, columns=avg5mdb_columns)
        # print(output.columns)

        return

    def print_entry(self):
        item_id, timestamp = decode_avg5m_entry_id(self.entry_id)
        print(f"Entry {int(self.entry_id)} {item_id} {timestamp} {self.buy_price} {self.buy_volume} "
              f"{self.sell_price} {self.sell_volume} for {self.table_id}")


class EntryPriceGraphDB:
    def __init__(self, table_id, item_id, timestamp, price=None, volume=None):
        """
        Entry object for Wiki prices table, entry_id is defined as item_id + timestamp
        :param table_id: ID for the table in the database (can be wiki, realtimehigh5m
        :param item_id: OSRS item ID
        :param timestamp: Timestamp of the specific entry as epoch time
        :param price: Price of item_id on timestamp
        :param volume: Volume of item_id on timestamp
        """
        self.table_id = table_id
        self.entry_id = f'{item_id}{timestamp}'
        self.item_id = item_id
        self.timestamp = timestamp
        self.price = price
        self.volume = volume if volume is not None else -1
        self.added = False

    def insert(self, cursor):
        sql = f"INSERT INTO {self.table_id}(id, item_id, timestamp, price, volume) " \
              f"VALUES({self.entry_id}, {self.item_id}, {self.timestamp}, {self.price}, {self.volume})"
        # print(sql)
        cursor.execute(sql)
        self.added = True

    def update(self, cursor):
        sql = f"UPDATE {self.table_id} SET id = {self.entry_id}, item_id = {self.item_id}, timestamp = {self.timestamp},"
        if self.price is not None:
            sql += f" price = {self.price},"
        if self.volume is not None:
            sql += f" volume = {self.volume},"
        sql = sql[:-1] + f" where id = {self.entry_id}"
        cursor.execute(sql)
        self.added = True

    def delete(self, cursor):
        sql = f"DELETE FROM {self.table_id} WHERE id = {self.entry_id}"
        cursor.execute(sql)

    def print_entry(self):
        print(f"Entry {self.item_id} {self.timestamp} {self.price} {self.volume} for {self.table_id}")

    def decode_entry(self):
        return


EPGDB_constants ={
    'df_columns': ['entry_id', 'item_id', 'timestamp', 'price', 'submitted'],
    'df_dtypes': ['entry_id', 'UInt64', 'item_id', 'timestamp', 'price', 'submitted']
}


class EntryPriceGraphRTDB:
    def __init__(self, table_id, item_id, timestamp, price=None):
        """
        Entry object for Wiki prices table, entry_id is defined as item_id + timestamp
        :param table_id: ID for the table in the database (can be wiki, realtimehigh5m
        :param item_id: OSRS item ID
        :param timestamp: Timestamp of the specific entry as epoch time
        :param price: Price of item_id on timestamp
        :param volume: Volume of item_id on timestamp
        """
        self.table_id = table_id
        self.entry_id = int(f'1{item_id:0>5}{timestamp}')
        self.item_id = item_id
        self.timestamp = timestamp
        self.price = price
        self.added = False

    def insert(self, cursor):
        sql = f"INSERT INTO {self.table_id}(id, item_id, timestamp, price) " \
              f"VALUES({self.entry_id}, {self.item_id}, {self.timestamp}, {self.price})"
        # print(sql)
        cursor.execute(sql)
        self.added = True

    def extract(self, connection, cursor):
        connection.row_factory = sqlite3.Row
        cursor = connection.execute('PRAGMA table_info(T22Q2)')
        desc = cursor.fetchall()
        # getting names using list comprehension
        names = [fields[1] for fields in desc]
        connection.close()
        print(names)

    def update(self, cursor):
        sql = f"UPDATE {self.table_id} SET id = {self.entry_id}, item_id = {self.item_id}, timestamp = {self.timestamp},"
        if self.price is not None:
            sql += f" price = {self.price},"
        sql = sql[:-1] + f" where id = {self.entry_id}"
        cursor.execute(sql)
        self.added = True

    def delete(self, cursor):
        sql = f"DELETE FROM {self.table_id} WHERE id = {self.entry_id}"
        cursor.execute(sql)

    def print_entry(self):
        print(f"Entry {self.item_id} {self.timestamp} {self.price} for {self.table_id}")

    def export(self, pickle_filename: str = None, csv_filename: str = None):
        df = pd.DataFrame(data={
            'table_id': self.table_id,
            'entry_id': int(f'1{self.item_id:0>5}{self.timestamp}'),
            'item_id': self.item_id,
            'timestamp': self.timestamp,
            'price': self.price
        })
        # save_data(df, 'Data/df_test_raw.dat')
        print(df.dtypes)
        df = df.astype(dtype={'table_id': 'string', 'entry_id': 'UInt64', 'item_id': 'UInt16', 'timestamp': 'UInt32', 'price': 'UInt32'})
        print(df.dtypes)
        df.to_pickle('Data/df_test_reduced.dat')


if __name__ == "__main__":
    dataframe = pd.read_pickle('resources/realtime_prices_dataframe.dat')
    print(dataframe.dtypes)
    dataframe = dataframe.astype(dtype={'item_id': 'UInt16', 'timestamp': 'UInt32', 'price': 'UInt32', 'is_sale': 'bool'})
    print(dataframe.dtypes)
    print(len(dataframe))
    dataframe.to_pickle('resources/realtime_prices_dataframe_unsigned.dat')
    df_sell, df_buy = dataframe.loc[dataframe['is_sale'] == True], dataframe.loc[dataframe['is_sale'] == False]
    print(len(df_sell), len(df_buy))
    df_sell.to_pickle('Data/DataFrames/rt_sell.dat')
    df_buy.to_pickle('Data/DataFrames/rt_buy.dat')
