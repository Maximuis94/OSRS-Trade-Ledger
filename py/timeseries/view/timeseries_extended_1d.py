"""
Implementation of an extension of the timeseries tables.
This extension is defined as a VIEW that is primarily based on available timeseries data of the item the VIEW is 
related to.
In this VIEW, data is aggregated per day.
Given the larger amount of datapoints per row, the data is defined as statistic


"""

extended_timeseries_view_1d = """
CREATE VIEW __OUT_TABLE__ AS
WITH
  times AS (
    SELECT DISTINCT (timestamp / 300) * 300 AS ts
    FROM __TIMESERIES_TABLE__
    WHERE (timestamp % 86400) = 0
      AND timestamp > (
        SELECT MIN(timestamp)
        FROM __TIMESERIES_TABLE__
        WHERE src > 0
      )
  ),

  -- 2) join in all the raw columns plus compute sales_tax
  enriched AS (
    SELECT
      t.ts                                                      AS timestamp,

      -- calendar parts
      CAST(strftime('%Y', t.ts, 'unixepoch') AS INTEGER)       AS year,
      CAST(strftime('%m', t.ts, 'unixepoch') AS INTEGER)       AS month,
      CAST(strftime('%d', t.ts, 'unixepoch') AS INTEGER)       AS day,
      CAST(strftime('%w', t.ts, 'unixepoch') AS INTEGER)       AS weekday,
      CAST(FLOOR(t.ts/86400) AS INTEGER)                       AS day_id,
      CAST(FLOOR(t.ts/604800) AS INTEGER)                      AS week_id,

      -- buy (src=1) snapshot at the 5-min boundary
      buy.price                                                AS buy_price,
      buy.volume                                               AS buy_volume,

      -- sell (src=2) snapshot at the 5-min boundary
      sell.price                                               AS sell_price,
      sell.volume                                              AS sell_volume,

      -- compute sales_tax here once:
      CASE
        WHEN sell.price > 0
          THEN min(floor(sell.price * 0.01), 5000000)
        WHEN buy.price  > 0
          THEN min(floor(buy.price  * 0.01), 5000000)
        ELSE NULL
      END                                                       AS sales_tax,

      -- most up-to-date src=0 (“wiki”) ≤ this ts
      (
        SELECT price
        FROM __TIMESERIES_TABLE__ AS inf
        WHERE inf.src = 0
          AND inf.timestamp <= t.ts
        ORDER BY inf.timestamp DESC
        LIMIT 1
      )                                                         AS wiki_price,

      (
        SELECT volume
        FROM __TIMESERIES_TABLE__ AS inf
        WHERE inf.src = 0
          AND inf.timestamp <= t.ts
        ORDER BY inf.timestamp DESC
        LIMIT 1
      )                                                         AS wiki_volume

    FROM times AS t
    LEFT JOIN __TIMESERIES_TABLE__ AS buy
      ON buy.src       = 1
     AND buy.timestamp = t.ts
    LEFT JOIN __TIMESERIES_TABLE__ AS sell
      ON sell.src      = 2
     AND sell.timestamp = t.ts
  )

-- 3) now reference that precomputed sales_tax to get your final margin
SELECT
  timestamp,
  year, month, day, hour, minute, second, weekday, hour_id, day_id, week_id,

  wiki_price,
  wiki_volume,
  buy_price,
  buy_volume,
  sell_price,
  sell_volume,
  sales_tax,

  -- subtract the tax from your raw (sell – buy)
  CASE
    WHEN sell_price > 0
      AND buy_price  > 0
    THEN (sell_price - buy_price) - sales_tax
    ELSE NULL
  END                                                         AS price_margin,

  -- summed_volume unchanged
  buy_volume + sell_volume                                   AS summed_volume

FROM enriched
ORDER BY timestamp;

"""
