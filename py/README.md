# OSRS Trade Ledger
Tool for keeping track of trades and to help identify potentially useful items in Old School RuneScape.

This project is a rewritten version of the OSRS-GE-Ledger, with the goal to improve/optimize its implementations, while 
trying to apply new libraries/modules in the process of doing so.



# Changes relative to OSRS-GE-Ledger

As of now, the following components have been successfully implemented;
### Folder structure
- Modules are now stored in separate directories, making the project much more tidy
- Exact categorization is a work in progress, though.


### Database redesign
- As the database kept growing in size, so did the query times. At some point, the query times increased disproportionally
    to the amount of extra rows.
- Sharding the database is a common solution. As a long-term solution, the database was sharded into one table per item.
    This reduced the amount of rows per table from 300-500M/table to 1.5-2M/table for old, frequently traded items.
- Query times decreased as intended, writing times increased, although this is not much of an issue.
- In spirit of the redesign, all rows have been merged to one standardized row that consists of 'src' (source), 
   'timestamp' (unix timestamp), 'price', 'volume'. The item_id is to be derived from the table name.
- The following sources are encoded by src; 0=wiki, 1=avg5m buy, 2=avg5m sell, 3=realtime buy, 4=realtime sell. Since
   realtime datapoints dont have a volume, its volume value is always equal to 0
- Although the sharding felt somewhat extreme, it is in line with queries made, as each query in nearly all cases
   specified an item_id, which is now implicitly present as table.

### Npy arrays -> sqlite database
- Npy Arrays have been migrated to an sqlite database. 
- The updater protocol was migrated as well, with a massive decrease in runtime (from ~12-15 minutes to 2-3 minutes)
- The prices listbox was expanded to 56 days coverage, shifting its implementation to sqlite resulted in a runtime that
   is about 300 times faster.
- Sqlite db made it much more intuitive to only add new rows while updating, rather than regenerating all data.
   Generating the initial database takes up quite some time, but subsequent updates have a much smaller scope, as only 
    new rows are to be generated.

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

### GUI (todo)


### Inventory (todo)


### Graph (todo)


### C# Implementation
- Inventory/GUI are probably going to be implemented in C# instead
