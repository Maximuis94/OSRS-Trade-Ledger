"""
Implementation of an extension of the timeseries tables.
This extension is defined as a VIEW that is primarily based on available timeseries data of the item the VIEW is 
related to.
The rows in this VIEW span an interval of 300 seconds.
Each timestamp occurs every 5th minute (i.e. timestamp%300==0)

This VIEW has completely substituted the npy database.
"""
import sqlite3

extended_timeseries_view_5m = """

CREATE VIEW __OUT_TABLE__ AS
WITH
  times AS (
    SELECT DISTINCT (timestamp / 300) * 300 AS ts
    FROM __TIMESERIES_TABLE__
    WHERE (timestamp % 300) = 0
      AND timestamp > (
        SELECT MIN(timestamp)
        FROM __TIMESERIES_TABLE__
        WHERE src > 0
      )
  ),
  realtime_agg AS (
    SELECT
      (timestamp / 300) * 300 AS ts,
      COUNT(*)           AS rt_count,
      MIN(price)         AS rt_price_min,
      MAX(price)         AS rt_price_max,
      AVG(price)         AS rt_price_avg
    FROM __TIMESERIES_TABLE__
    WHERE src > 2
    GROUP BY ts
  ),

  -- 2) join in all the raw columns plus compute sales_tax
  enriched AS (
    SELECT
      t.ts                                                      AS timestamp,

      -- calendar parts
      CAST(strftime('%Y', t.ts, 'unixepoch') AS INTEGER)       AS year,
      CAST(strftime('%m', t.ts, 'unixepoch') AS INTEGER)       AS month,
      CAST(strftime('%d', t.ts, 'unixepoch') AS INTEGER)       AS day,
      CAST(strftime('%H', t.ts, 'unixepoch') AS INTEGER)       AS hour,
      CAST(strftime('%M', t.ts, 'unixepoch') AS INTEGER)       AS minute,
      CAST(strftime('%S', t.ts, 'unixepoch') AS INTEGER)       AS second,
      CAST(strftime('%w', t.ts, 'unixepoch') AS INTEGER)       AS weekday,
      CAST(FLOOR(t.ts/3600) AS INTEGER)                        AS hour_id,
      CAST(FLOOR(t.ts/86400) AS INTEGER)                       AS day_id,
      CAST(FLOOR(t.ts/604800) AS INTEGER)                      AS week_id,

      -- buy (src=1) snapshot at the 5-min boundary
      buy.price                                                AS buy_price,
      buy.volume                                               AS buy_volume,
	  buy.price * buy.volume								   AS buy_value,

      -- sell (src=2) snapshot at the 5-min boundary
      sell.price                                               AS sell_price,
      sell.volume                                              AS sell_volume,
	  sell.price * sell.volume								   AS sell_value,

      CAST(ROUND((sell.price*sell.volume+buy.price*buy.volume)/(buy.volume+sell.volume), 0) AS INTEGER) AS avg5m_price,
      buy.volume + sell.volume                                 AS avg5m_volume,
	  buy.price*buy.volume+sell.price*sell.volume              AS avg5m_value,
	  
	  wiki.timestamp                                           AS wiki_timestamp,
	  wiki.price                                               AS wiki_price,
	  wiki.volume                                              AS wiki_volume,
	  wiki.price * wiki.volume                                 AS wiki_value,
	  
	  r.rt_count AS realtime_n,
      r.rt_price_min AS realtime_min,
      r.rt_price_max AS realtime_max,
      r.rt_price_avg AS realtime_avg,
	  
	  ROUND(CAST(NULLIF(sell.price,0)-NULLIF(buy.price,0) AS REAL)/wiki.price, 4)                        AS gap_bs,
	  ROUND(CAST(NULLIF(buy.price,0)-wiki.price AS REAL)/wiki.price, 4)                        AS gap_wb,
	  ROUND(CAST(NULLIF(sell.price,0)-wiki.price AS REAL)/wiki.price, 4)                        AS gap_ws,
	  

      -- compute sales_tax here once:
      CASE
        WHEN t.ts <= 1639047600
          THEN 0
        WHEN sell.price > 0
          THEN CAST(min(floor(sell.price * 0.01), 5000000) AS INTEGER)
        WHEN buy.price  > 0
          THEN CAST(min(floor(buy.price  * 0.01), 5000000) AS INTEGER)
        ELSE NULL
      END                                                       AS sales_tax

    FROM times AS t
    LEFT JOIN __TIMESERIES_TABLE__ AS buy
      ON buy.src       = 1
     AND buy.timestamp = t.ts
    LEFT JOIN __TIMESERIES_TABLE__ AS sell
      ON sell.src      = 2
     AND sell.timestamp = t.ts
    LEFT JOIN __TIMESERIES_TABLE__ AS wiki
     ON wiki.src       = 0
     AND wiki.timestamp = (
       SELECT MAX(inf2.timestamp)
       FROM __TIMESERIES_TABLE__ AS inf2
       WHERE inf2.src = 0
         AND inf2.timestamp <= t.ts
     )

    LEFT JOIN realtime_agg AS r
      ON r.ts = t.ts
     )

  

-- 3) now reference that precomputed sales_tax to get your final margin
SELECT
  timestamp,
  year, month, day, hour, minute, second, weekday, hour_id, day_id, week_id,
  wiki_timestamp,
  wiki_price,
  wiki_volume,
  wiki_value,
  -- CAST(FLOOR(wiki_volume/288) AS INTEGER) AS wiki_volume_5m,
  -- wiki_price * wiki_volume AS wiki_value,
  buy_price,
  buy_volume,
  buy_value,
  sell_price,
  sell_volume,
  sell_value,
  avg5m_price,
  avg5m_volume,
  avg5m_value,
  sales_tax,
  gap_bs,
  gap_wb,
  gap_ws,
  realtime_n,
  realtime_min,
  realtime_max,
  realtime_avg,


  -- subtract the tax from your raw (sell – buy)
  CASE
    WHEN sell_price > 0
      AND buy_price  > 0
    THEN (sell_price - buy_price) - sales_tax
    ELSE NULL
  END                                                         AS price_margin

FROM enriched
ORDER BY timestamp;
"""

def sql_extended_timeseries_table(item_id: int) -> str:
    """Inject the table name into the extended view SQL based on `item_id` and return it"""
    return (extended_timeseries_view_5m
            .replace('__TIMESERIES_TABLE__', f'"item{item_id:0>5}"')
            .replace('__OUT_TABLE__', f'"item{item_id:0>5}_5m_extended"'))


db = sqlite3.connect(r"C:\osrs\data\subset.db")
db.execute(sql_extended_timeseries_table(2))
# print(db.execute("SELECT sqlite_version() AS lib_version, sqlite_source_id() AS build_id ").fetchall())
# db.commit()
db.close()