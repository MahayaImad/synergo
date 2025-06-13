-- Tables de gestion de la synchronisation
CREATE TABLE synergo_sync.sync_tables (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) UNIQUE NOT NULL,
    hfsql_table VARCHAR(100) NOT NULL,
    sync_strategy VARCHAR(20) DEFAULT 'ID_BASED',
    is_active BOOLEAN DEFAULT true,
    sync_interval_minutes INTEGER DEFAULT 30,
    batch_size INTEGER DEFAULT 1000,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE synergo_sync.sync_state (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) REFERENCES synergo_sync.sync_tables(table_name),
    last_sync_id BIGINT DEFAULT 0,
    last_sync_timestamp TIMESTAMP,
    last_sync_date VARCHAR(8),
    last_sync_time VARCHAR(6),
    total_records BIGINT DEFAULT 0,
    last_sync_duration INTEGER,
    last_sync_status VARCHAR(20) DEFAULT 'PENDING',
    error_message TEXT,
    records_processed_last_sync INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE synergo_sync.sync_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    operation VARCHAR(20),
    hfsql_id BIGINT,
    postgres_id BIGINT,
    records_processed INTEGER DEFAULT 0,
    processing_time_ms INTEGER,
    error_details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour performance
CREATE INDEX idx_sync_log_table_created ON synergo_sync.sync_log(table_name, created_at);
CREATE INDEX idx_sync_state_table ON synergo_sync.sync_state(table_name);
CREATE INDEX idx_sync_log_operation ON synergo_sync.sync_log(operation, created_at);

-- Configuration initiale des tables à synchroniser
INSERT INTO synergo_sync.sync_tables (table_name, hfsql_table, sync_strategy, sync_interval_minutes, batch_size) VALUES
('sales_summary', 'sorties', 'ID_BASED', 30, 1000),
('products_catalog', 'nomenclature', 'ID_BASED', 60, 500),
('stock_movements', 'entrees_produits', 'ID_BASED', 30, 1000),
('purchase_orders', 'entrees', 'ID_BASED', 60, 500);

-- États initiaux de synchronisation
INSERT INTO synergo_sync.sync_state (table_name, last_sync_id, last_sync_status) 
SELECT table_name, 0, 'NEVER_SYNCED' 
FROM synergo_sync.sync_tables;