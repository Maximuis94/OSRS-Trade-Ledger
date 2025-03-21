-- Better schema with proper constraints and indices
CREATE TABLE IF NOT EXISTS npy_data (
    id INTEGER PRIMARY KEY,
    item_id INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    -- Time components for easier querying
    minute INTEGER NOT NULL CHECK (minute BETWEEN 0 AND 59),
    hour INTEGER NOT NULL CHECK (hour BETWEEN 0 AND 23),
    day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL CHECK (weekday BETWEEN 0 AND 6),
    
    -- Derived time fields
    hour_timestamp INTEGER NOT NULL,
    day_timestamp INTEGER NOT NULL,
    week_timestamp INTEGER NOT NULL,
    
    -- Wiki data
    wiki_timestamp INTEGER,
    wiki_price INTEGER,
    wiki_volume INTEGER,
    wiki_total_value INTEGER GENERATED ALWAYS AS (wiki_price * wiki_volume) STORED,
    wiki_avg_volume INTEGER GENERATED ALWAYS AS (wiki_volume / 288) STORED,
    
    -- Buy/Sell data
    buy_price INTEGER,
    buy_volume INTEGER,
    buy_total_value INTEGER GENERATED ALWAYS AS (buy_price * buy_volume) STORED,
    sell_price INTEGER,
    sell_volume INTEGER,
    sell_total_value INTEGER GENERATED ALWAYS AS (sell_price * sell_volume) STORED,
    
    -- Avg5m data
    avg5m_price INTEGER GENERATED ALWAYS AS (
        CASE 
            WHEN buy_price > 0 AND sell_price > 0 THEN (buy_price + sell_price) / 2
            ELSE COALESCE(buy_price, sell_price, 0)
        END
    ) STORED,
    avg5m_volume INTEGER GENERATED ALWAYS AS (buy_volume + sell_volume) STORED,
    avg5m_total_value INTEGER GENERATED ALWAYS AS (avg5m_price * avg5m_volume) STORED,
    avg5m_margin INTEGER GENERATED ALWAYS AS (
        CASE 
            WHEN buy_price > 0 AND sell_price > 0 THEN sell_price - buy_price - CAST(sell_price * 0.01 as INTEGER)
            ELSE 0
        END
    ) STORED,
    
    -- Price ratios
    price_ratio_sell_buy REAL GENERATED ALWAYS AS (
        CASE 
            WHEN buy_price > 0 AND wiki_price > 0 THEN CAST(sell_price - buy_price AS REAL) / wiki_price 
            ELSE 0 
        END
    ) STORED,
    price_ratio_buy_wiki REAL GENERATED ALWAYS AS (
        CASE 
            WHEN wiki_price > 0 THEN CAST(buy_price - wiki_price AS REAL) / wiki_price 
            ELSE 0 
        END
    ) STORED,
    price_ratio_sell_wiki REAL GENERATED ALWAYS AS (
        CASE 
            WHEN wiki_price > 0 THEN CAST(sell_price - wiki_price AS REAL) / wiki_price 
            ELSE 0 
        END
    ) STORED,
    
    -- Realtime data
    rt_avg_price INTEGER,
    rt_min_price INTEGER,
    rt_max_price INTEGER,
    rt_count INTEGER,
    rt_margin INTEGER GENERATED ALWAYS AS (
        CASE 
            WHEN rt_count > 0 THEN rt_max_price - rt_min_price - CAST(rt_avg_price * 0.01 as INTEGER)
            ELSE 0
        END
    ) STORED,
    
    -- Additional metrics
    tax INTEGER GENERATED ALWAYS AS (
        LEAST(5000000, CAST(COALESCE(sell_price, wiki_price, 0) * 0.01 as INTEGER))
    ) STORED,
    volume_coefficient REAL,

    UNIQUE (item_id, timestamp)
);

-- Indices for efficient querying
CREATE INDEX IF NOT EXISTS idx_npy_item_timestamp ON npy_data(item_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_npy_time_components ON npy_data(year, month, day, hour); 