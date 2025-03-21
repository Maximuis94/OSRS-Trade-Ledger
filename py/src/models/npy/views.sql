-- Daily price summary
CREATE VIEW v_daily_price_summary AS
SELECT 
    item_id,
    date(timestamp, 'unixepoch') as trade_date,
    avg(wiki_price) as avg_wiki_price,
    avg(avg5m_price) as avg_5m_price,
    sum(wiki_volume) as total_volume,
    avg(avg5m_margin) as avg_margin,
    count(*) as data_points
FROM npy_data
GROUP BY item_id, trade_date;

-- Hourly trading patterns
CREATE VIEW v_hourly_patterns AS
SELECT 
    item_id,
    hour,
    avg(buy_price) as avg_buy_price,
    avg(sell_price) as avg_sell_price,
    avg(avg5m_margin) as avg_margin,
    avg(volume_coefficient) as avg_volume_coef
FROM npy_data
GROUP BY item_id, hour; 