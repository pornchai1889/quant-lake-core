-- -----------------------------------------------------------------------------
-- File: 02_schema_master.sql
-- Purpose: Define master data tables (Assets, Categories, Metadata).
--          These tables store static or slowly changing dimensions.
-- -----------------------------------------------------------------------------

-- 1. Create ENUM type for Asset Classes to ensure data integrity.
--    This restricts values to only valid asset types.
CREATE TYPE asset_class_enum AS ENUM ('CRYPTO', 'STOCK', 'FOREX', 'COMMODITY');

-- 2. Create 'assets' table to store instrument definitions.
--    This acts as the central registry for all financial instruments.
CREATE TABLE IF NOT EXISTS assets (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(50) NOT NULL,              -- e.g., 'BTCUSDT', 'AAPL'
    asset_class     asset_class_enum NOT NULL,         -- e.g., 'CRYPTO', 'STOCK'
    exchange        VARCHAR(50) NOT NULL,              -- e.g., 'BINANCE', 'NASDAQ'
    name            VARCHAR(255),                      -- e.g., 'Bitcoin', 'Apple Inc.'
    description     TEXT,                              -- Additional details
    is_active       BOOLEAN DEFAULT TRUE,              -- Soft delete flag
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),

    -- Ensure unique combination of symbol and exchange to prevent duplicates
    CONSTRAINT uq_asset_symbol_exchange UNIQUE (symbol, exchange)
);

-- Create an index on symbol for faster lookups during ETL processes.
CREATE INDEX idx_assets_symbol ON assets (symbol);
CREATE INDEX idx_assets_class ON assets (asset_class);

-- Comment on table (Good practice for documentation)
COMMENT ON TABLE assets IS 'Central registry for all financial instruments (Stocks, Crypto, etc.)';