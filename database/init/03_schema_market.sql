-- -----------------------------------------------------------------------------
-- File: 03_schema_market.sql
-- Purpose: Define time-series tables (Market Data, Financials, Macro).
--          These tables are optimized for high-volume time-series data.
-- -----------------------------------------------------------------------------

-- ==========================================
-- 1. Market Quotes (OHLCV)
-- ==========================================

-- Standard OHLCV table structure.
-- We use BIGINT for volume to accommodate crypto decimals (stored as smallest unit if needed)
-- or Double Precision for general usage. Here, DOUBLE PRECISION is used for versatility.
CREATE TABLE IF NOT EXISTS market_quotes (
    time            TIMESTAMPTZ NOT NULL,
    asset_id        INTEGER NOT NULL REFERENCES assets(id),
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,
    
    -- Optional: VWAP or adjusted close can be added here
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    -- Composite primary key (time + asset) is standard for TimescaleDB
    PRIMARY KEY (time, asset_id)
);

-- Convert standard table to TimescaleDB Hypertable.
-- Partitioning by 'time' allows efficient querying and data retention management.
SELECT create_hypertable('market_quotes', 'time', if_not_exists => TRUE);

-- Create index for faster queries by asset
CREATE INDEX idx_market_quotes_asset ON market_quotes (asset_id, time DESC);

-- ==========================================
-- 2. Financial Statements (Fundamental Data)
-- ==========================================

-- Stores quarterly/annual financial data (Balance Sheet, Income Statement).
-- While not high-frequency, keeping it time-indexed allows point-in-time analysis.
CREATE TABLE IF NOT EXISTS financial_statements (
    time            TIMESTAMPTZ NOT NULL,              -- Report publication date
    asset_id        INTEGER NOT NULL REFERENCES assets(id),
    period_end      DATE NOT NULL,                     -- Fiscal period end date
    period_type     VARCHAR(10) NOT NULL,              -- 'Q' (Quarterly), 'Y' (Yearly)
    
    -- Core metrics (Simplified for demo)
    revenue         DOUBLE PRECISION,
    net_income      DOUBLE PRECISION,
    eps             DOUBLE PRECISION,                  -- Earnings Per Share
    total_assets    DOUBLE PRECISION,
    total_liabilities DOUBLE PRECISION,
    
    -- JSONB column for flexibility to store extra fields without schema migration
    raw_data        JSONB,

    PRIMARY KEY (time, asset_id, period_type)
);

-- Convert to Hypertable (Optional, but good for uniformity)
SELECT create_hypertable('financial_statements', 'time', if_not_exists => TRUE);

-- ==========================================
-- 3. Macroeconomic Indicators
-- ==========================================

-- Stores global economic data like Interest Rates, Inflation (CPI), GDP.
CREATE TABLE IF NOT EXISTS macro_indicators (
    time            TIMESTAMPTZ NOT NULL,
    country         VARCHAR(3) NOT NULL,               -- ISO Code e.g., 'USA', 'THA'
    indicator       VARCHAR(50) NOT NULL,              -- e.g., 'CPI', 'GDP_GROWTH', 'FED_RATE'
    value           DOUBLE PRECISION NOT NULL,
    frequency       VARCHAR(20),                       -- 'MONTHLY', 'QUARTERLY'

    PRIMARY KEY (time, country, indicator)
);

-- Convert to Hypertable
SELECT create_hypertable('macro_indicators', 'time', if_not_exists => TRUE);