# OSRS Trade Ledger (Python implementation)
Tool for keeping track of trades and to help identify potentially useful items in Old School RuneScape.

This project is a rewritten version of the OSRS-GE-Ledger, with the goal to apply new knowledge and to improve the 
codebase. While refactoring the code, the additional emphasis was placed on;
- Object-Oriented design
- Separation of concepts imposed via file- and folder structure
  - Encapsulate said concepts
- Apply typically used, effective design patterns
- Strongly typed variables
- Properly documented code
- Leverage existing Python features effectively
- If possible, delegate implementations to C/SQLite/..., rather than Python


## Databases

### Timeseries database
- Database that consists of N tables, where N is the amount of tradable items in OSRS
- Database has a uniform row format;
  - timestamp - UNIX timestamp (INTEGER)
  - src - Source that describes what kind of data the row represents (INTEGER; 0=wiki, 1=runelite buy trade data, 
    averaged per 5 minutes, 2=runelite sell trade datam averaged per 5 minutes, 3=runelite realtime buy trade data, 
    4=runelite realtime sell trade data
  - price - Item price (INTEGER)
  - volume - Item volume (INTEGER)
![img.png](markdown_images/item00002_db_schema.png)
_item00002 table as displayed in the SQLite DBBrowser_

### Npy database
- Database that captures a subset of the available timeseries data, although this data combines available information 
    and computes aggregated values per row.
- Since the timestep differs per source, the rows are anchored at the avg5m rows, i.e. one row every 300 seconds. These 
  timestamps coincide with UNIX timestamp % 300 == 0.

#### Npy columns
_In some cases, multiple columns are combined to one row_
- **item_id**: INTEGER, OSRS item ID
- **timestamp**: INTEGER, Unix timestamp
- **minute**/**hour**/**day**/**month**/**year**: INTEGER, self-explanatory
- **day_of_week**: INTEGER, Day of week - Monday=0, Sunday=6
- **hour_id**/**day_id**/**week_id**: INTEGER, timestamp divided by 3600/86400/604800, respectively. Used to assign a unique id to each hour/day/week listed.
- **wiki_ts**: INTEGER, Timestamp of the most recent entry for src=0, relative to timestamp
- **wiki_price**: INTEGER, The price registered at said wiki entry
- **wiki_volume**: INTEGER, The volume registered at said wiki entry
- **wiki_value**: INTEGER, wiki_price * wiki_volume
- **wiki_volume_5m**: INTEGER, wiki_volume per 5 minutes
- **buy_price**/buy_volume: INTEGER, data from src=1 for this timestamp and item
- **buy_value**: INTEGER, buy_price * buy_volume
- **sell_price**/**sell_volume**: INTEGER, data from src=2 for this timestamp and item
- **sell_value**: INTEGER, sell_price * sell_volume
- **avg5m_price**/**avg5m_volume**/**avg5m_value**: INTEGER, Same as buy/sell, but uses averaged values from buy/sell price/volume
- **avg5m_margin**: INTEGER, sell_price - buy_price - tax
- **gap_bs**: REAL, (sell_price - buy_price) / wiki_price
- **gap_wb**: REAL, (buy_price - wiki_price) / wiki_price
- **gap_ws**: REAL, (wiki_price - sell_price) / wiki_price
- **rt_avg**: INTEGER, Averaged price, computed from all realtime prices in the timespan of this row
- **rt_min**: INTEGER, Lowest realtime price, computed from all realtime prices in the timespan of this row
- **rt_max**: INTEGER, Highest realtime price, computed from all realtime prices in the timespan of this row
- **n_rt**: INTEGER, Number of realtime datapoints (i.e. src=3/4) in the timespan of this row
- **realtime_margin**: INTEGER, Highest realtime price - lowest realtime price - tax
- **tax**: INTEGER, Tax applicable to this item (= MIN(5000000, FLOOR(item_price * .01)))
- **est_vol_per_char**: INTEGER, Estimated volume per character per day. Equal to 4 times the buy limit.
- **volume_coefficient**: REAL, Value that describes the buy limit relative to the wiki volume, should the former exceed the latter. If so, the value drops below 1.0

### Data collection and transfer
- Data is scraped using a Raspberry Pi 4
- Data transfer is initiated manually. Upon completing the task, the source data is deleted. 
- After transferring data, the Npy database is modified; expired rows are deleted, new data is computed and added
![img.png](markdown_images/data_transfer_output.png)
_Example output displayed while transferring data_


## GUI
- As of now, a more object-oriented groundwork has been laid for the GUI, it has yet to be implemented, though.
- All components appear to be working (gui directory)
- The GUI has to be recreated, though. The images displayed below are of the GUI from the old project (which remains in use for the time being)

### GUI: Inventory
![img_5.png](markdown_images/inventory.png)
_Inventory tab of the GUI, it displays information on current stock, as well as cumulative results. On the right, a 
price graph is shown of the item that is clicked on the top listbox. The bottom listbox displays transactions made that 
involve the item clicked on the top listbox_

### GUI: Results/day
![img_6.png](markdown_images/results_per_day.png)
![img_7.png](markdown_images/results_per_month.png)
![img_8.png](markdown_images/results_per_year.png)
_This section is used to display results per day, month or year. If an entry in the listbox is clicked, the bottom 
listbox will display transactions for that day (this functionality does not alter with per month/year display)_


### GUI: Prices/4h
![img_9.png](markdown_images/prices_per_4h.png)
_This section is used to explore new potentially interesting items to trade. It displays timeseries data on the right, 
while it shows a specifically ordered list on the left, starting with most recently traded items. If an entry from the 
top listbox is clicked, the graph changes to that particular item. 
The bottom listbox will be updated with data of that item as well; it shows summarized data, aggregated per 4-hour 
interval, allowing one to act on (this was actually the initial purpose of this tab, hence the name);
- s_24h_high shows the highest sell price across all 4h intervals for the past 24 hours
- s_24h_last shows this sell price for the most recent 4h interval
- b_x_y shows the recommended buy price for the interval spanning from xxh-yyh (e.g. 12:00-16:00). The 4-hour interval 
  coincides with the buy-limit, which resets 4 hours after the first item is bought


### GUI: Graphs
The graph on the right displays various graph types, each type is covered below. The timespan of the graph that is 
being displayed can also be altered.

#### Price graph
![img_10.png](markdown_images/price_graph.png)
The one that is most commonly used is the price graph, which 
displays the item price over time. 

#### Price/DoW graph
![img_11.png](markdown_images/price_per_day_of_week.png)
This graph shows the price per day of the week. The prices are converted to values that are relative to weekly averages.

#### Price/HoD
![img_12.png](markdown_images/price_per_hour_of_day.png)
Much like the price per day of week, only per hour of the day.



# Changes relative to OSRS-GE-Ledger

As of now, the following components have been successfully implemented;
### Folder structure
- Modules are now stored in separate directories, making the project much more tidy
- Exact categorization is a work in progress, though.


### Database redesign
- As the database kept growing in size, so did the query times. At some point, the query times increased disproportionally
    to the amount of extra rows.
- Sharding the database was a solution mentioned multiple times, which implies the database will be divided into several
    'shards' that together constitute the original database. It may introduce new issues if not done properly, however.
  - Given the query times, it was either that or switching to another type of database
- Database was sharded per item. That is, each individual item was given its own table (named "itemNNNNN", where N is a
  5-digit, 0-padded item_id).
  - The choice to shard per item was based on queries made. Data is always queried per item_id, which makes sense as you
    typically need timeseries data for one or more items.
  - Query times decreased as intended, writing times increased, although this is not much of an issue.
- Since data was stored per item, the format used to store data was stored was normalized. Rather than using a 
  specific table per source, all sources would have a uniform representation. This simplified representation made the 
    data easier to deal with, although it did require rethinking some of it;
  - In order to distinguish data from various sources; a new column was introduced; src. This is an INTEGER column that
    of which the value describes the kind of data it represents;
        0. Wiki data - scraped from https://api.weirdgloop.org/exchange/history/osrs/all?id=2 
        1. Avg5m buy data - Scraped from prices.runescape.wiki 
          (see https://oldschool.runescape.wiki/w/RuneScape:Real-time_Prices)
        2. Avg5m sell data - Scraped from prices.runescape.wiki 
          (see https://oldschool.runescape.wiki/w/RuneScape:Real-time_Prices)
        3. Realtime buy data - Scraped from https://prices.runescape.wiki/api/v1/osrs/latest
        4. Realtime sell data - Scraped from https://prices.runescape.wiki/api/v1/osrs/latest
  - Since avg5m data initially captured both buy and sell data, these rows were split in 2
  - Since there is no volume data for sources 3 and 4, this value is always 0
- While the sharding is somewhat extreme and does have some drawbacks, in practice it seemed like the right choice, 
    given the circumstances. The major drawbacks are;
  - Increased writing times
  - Aggregating data across <a subset of / all> items is more complicated and takes much longer. In practice, I have not
    encountered situations that required aggregating such data, however, since the data is always separated per item.

### Npy arrays -> sqlite database
- Npy arrays now have their own SQLite database. Its name lives on, as this database is still named the npy database.
- The updater protocol was migrated as well, with a massive decrease in runtime (from ~12-15 minutes to 2-3 minutes)
- The prices listbox was expanded to 56 days coverage, computing it was noticeably faster via SQLite
- Sqlite db made it much more intuitive to only add new rows while updating, rather than regenerating all data. The
  improved updater protocol made me revise the timespan of the npy database as well, which was increased from ~60 days 
    to 456 days
- Ideally, the npy database is computed using SQL.

### Object-oriented approach / dataclasses
- A more object-oriented approach was implemented in nearly all modules
- This has made database interactions more easy
- Attempted to implement modules in a mvc-like style
- Databases are also generated automatically based on the dataclasses, by parsing their attributes and converting it 
  into sql statements.
- GUI/Graph class is expected to benefit massively from OO approach, as well as the Inventory once implemented.

### Optimized basic methods / functions
- Basic methods / functions have been optimized by comparing multiple alternatives
- Furthermore, an approach that relies more on existing libraries was chosen, if possible
