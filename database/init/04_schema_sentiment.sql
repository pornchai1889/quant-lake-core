-- -----------------------------------------------------------------------------
-- File: 04_schema_sentiment.sql
-- Purpose: Define tables for AI-driven sentiment analysis.
--          This schema stores quantitative scores derived from unstructured text
--          (News, Social Media) using LLMs.
-- Author: QuantLake-Core Team
-- -----------------------------------------------------------------------------

-- ==========================================
-- 4. Market Sentiment (AI Analysis)
-- ==========================================

-- Table: market_sentiment
-- Description: Stores the output of LLM analysis on news headlines.
--              It links unstructured text to structured metrics (scores).
CREATE TABLE IF NOT EXISTS market_sentiment (
    -- Time-series dimension (Critical for TimescaleDB)
    time            TIMESTAMPTZ NOT NULL,
    
    -- Dimensions
    asset_id        INTEGER NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    source          VARCHAR(50) NOT NULL,              -- e.g., 'CryptoPanic', 'Bloomberg', 'Twitter'
    
    -- Content Metadata
    -- Storing headline allows for debugging and re-processing with new models.
    headline        TEXT NOT NULL,
    
    -- AI Analysis Metrics (Quantitative)
    sentiment_score DOUBLE PRECISION NOT NULL,         -- Range: -1.0 (Neg) to 1.0 (Pos)
    impact_score    DOUBLE PRECISION NOT NULL,         -- Range: 0.0 (None) to 1.0 (High)
    confidence      DOUBLE PRECISION NOT NULL,         -- Range: 0.0 (Unsure) to 1.0 (Certain)
    
    -- Advanced Classification
    -- Using PostgreSQL Array type for tagging (e.g., ['REGULATION', 'EARNINGS'])
    topics          TEXT[],
    
    -- Audit fields
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    -- Composite Primary Key
    -- Ensures uniqueness: One asset cannot have duplicate news from the same source at the exact same time.
    PRIMARY KEY (time, asset_id, source),

    -- Data Integrity Constraints (Guardrails)
    -- These ensure that the AI model output is strictly within valid mathematical bounds.
    CONSTRAINT chk_sentiment_range CHECK (sentiment_score >= -1.0 AND sentiment_score <= 1.0),
    CONSTRAINT chk_impact_range CHECK (impact_score >= 0.0 AND impact_score <= 1.0),
    CONSTRAINT chk_confidence_range CHECK (confidence >= 0.0 AND confidence <= 1.0)
);

-- -----------------------------------------------------------------------------
-- TimescaleDB Hypertable Conversion
-- -----------------------------------------------------------------------------

-- Convert to Hypertable to enable automatic partitioning by time.
-- 'if_not_exists' ensures idempotency during CI/CD deployments.
SELECT create_hypertable('market_sentiment', 'time', if_not_exists => TRUE);

-- -----------------------------------------------------------------------------
-- Indexing Strategy
-- -----------------------------------------------------------------------------

-- 1. Asset Lookup Index
-- Optimized for queries like: "Get sentiment history for Bitcoin (asset_id=1)"
-- Order by time DESC for retrieving latest news first.
CREATE INDEX IF NOT EXISTS idx_market_sentiment_asset_time 
ON market_sentiment (asset_id, time DESC);

-- 2. Topic Search Index (GIN)
-- Optimized for searching within the array column.
-- Use Case: "Find all news related to 'REGULATION' across all assets."
CREATE INDEX IF NOT EXISTS idx_market_sentiment_topics 
ON market_sentiment USING GIN (topics);