-- -----------------------------------------------------------------------------
-- File: 01_extensions.sql
-- Purpose: Enable necessary extensions for the TimescaleDB instance.
-- Author: QuantLake-Core Team
-- -----------------------------------------------------------------------------

-- Enable TimescaleDB extension if not already enabled.
-- This is required to use Hypertables and time-series analytical functions.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- (Optional) Enable pgcrypto if we need UUID generation or encryption later.
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;