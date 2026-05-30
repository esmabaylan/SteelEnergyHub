-- TimescaleDB starting up

-- Enabling TimescaleDB extension for time-series data handling
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Enabling UUID generation extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enabling pgcrypto extension for cryptographic functions
CREATE EXTENSION IF NOT EXISTS pgcrypto;