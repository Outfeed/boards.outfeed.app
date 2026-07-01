
-- Active MLS Boards (Lookup Table)
CREATE TABLE IF NOT EXISTS boards (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    short_name TEXT,
    reso_url TEXT
);

-- Active Clients
CREATE TABLE IF NOT EXISTS clients (
    id TEXT PRIMARY KEY,
    api_key TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    market_filter TEXT, -- JSON filter like {"city": "Bakersfield"}
    enabled_fields TEXT, -- JSON array like ["price", "address"]
    d1_database_id TEXT, -- The UUID of the client's dedicated D1
    status TEXT DEFAULT 'active',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_client_key ON clients(api_key);

-- Client Feeds (The ETLs)
CREATE TABLE IF NOT EXISTS client_feeds (
    id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    board_id TEXT NOT NULL,
    odata_filter TEXT,
    sync_interval_hours INTEGER DEFAULT 24,
    last_sync_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(client_id) REFERENCES clients(id),
    FOREIGN KEY(board_id) REFERENCES boards(id)
);

-- Temporary Mock Data (For local dev and init)
INSERT INTO boards (id, name, short_name, reso_url) VALUES 
('trreb', 'Toronto Regional', 'TRREB', 'https://api.treb.com/OData/'),
('crmls', 'California Regional', 'CRMLS', 'https://go.crmls.org/OData/'),
('armls', 'Arizona Regional', 'ARMLS', 'https://armls.com/OData/')
ON CONFLICT(id) DO UPDATE SET name=excluded.name, short_name=excluded.short_name, reso_url=excluded.reso_url;

-- Note: The IDs MUST start with a letter and contain only alphanumeric chars (no hyphens) 
-- so they can be valid Cloudflare Worker Environment bindings (e.g. env.c8f93a2b71e4)
INSERT INTO clients (id, api_key, name, market_filter) VALUES 
('c8f93a2b71e4', 'key_curaytor_123', 'Curaytor', '{"city": "Toronto"}'),
('m5d29b1c8a3f', 'key_marbo_456', 'Marbo AI', '{"agent_id": "A123"}'),
('w9e4c2f1b8d7', 'key_bakersfield_789', 'Bakersfield Realty', '{"city": "Bakersfield"}')
ON CONFLICT(id) DO UPDATE SET api_key=excluded.api_key, market_filter=excluded.market_filter;

INSERT INTO client_feeds (id, client_id, board_id, odata_filter, sync_interval_hours) VALUES
('feed_c8f93a2b71e4_trreb', 'c8f93a2b71e4', 'trreb', 'StandardStatus eq ''Active''', 24),
('feed_m5d29b1c8a3f_crmls', 'm5d29b1c8a3f', 'crmls', 'StandardStatus eq ''Active''', 12),
('feed_w9e4c2f1b8d7_trreb', 'w9e4c2f1b8d7', 'trreb', 'StandardStatus eq ''Active'' and City eq ''Toronto''', 1)
ON CONFLICT(id) DO NOTHING;

-- Global Listings Table (Control Plane Cache)
-- Boards worker pulls from RESO and dumps here.
CREATE TABLE IF NOT EXISTS listings (
    id TEXT PRIMARY KEY,
    board_id TEXT NOT NULL,
    status TEXT,
    price INTEGER,
    address TEXT,
    city TEXT,
    bedrooms INTEGER,
    bathrooms INTEGER,
    sqft INTEGER,
    agent_id TEXT,
    office_id TEXT,
    image_url TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(board_id) REFERENCES boards(id)
);
