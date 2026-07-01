-- Dedicated Client Database Schema
-- Run this script when provisioning a new client D1 database

CREATE TABLE IF NOT EXISTS listings (
    id TEXT PRIMARY KEY,
    board_id TEXT NOT NULL,
    status TEXT NOT NULL,
    price INTEGER,
    address TEXT,
    city TEXT,
    bedrooms INTEGER,
    bathrooms INTEGER,
    sqft INTEGER,
    raw_data TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_status ON listings(status);
CREATE INDEX IF NOT EXISTS idx_city ON listings(city);
