-- Optimized schema for hoarder_server with partitioning support

-- Main tables
CREATE TABLE IF NOT EXISTS device_data (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS latest_device_states (
    device_id TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS timestamped_data (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    data_timestamp TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ DEFAULT now(),
    data_type TEXT DEFAULT 'delta',
    is_offline BOOLEAN DEFAULT false,
    batch_id TEXT NULL
);

-- Archive table for old data
CREATE TABLE IF NOT EXISTS timestamped_data_archive (
    LIKE timestamped_data INCLUDING ALL
);

-- Indexes for main tables
CREATE INDEX IF NOT EXISTS idx_device_data_device_id ON device_data(device_id);
CREATE INDEX IF NOT EXISTS idx_device_data_received_at ON device_data(received_at DESC);

CREATE INDEX IF NOT EXISTS idx_timestamped_data_device_time ON timestamped_data(device_id, data_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_timestamped_data_timestamp ON timestamped_data(data_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_timestamped_data_device_id ON timestamped_data(device_id);
CREATE INDEX IF NOT EXISTS idx_timestamped_data_timestamp_device ON timestamped_data(data_timestamp DESC, device_id);

-- Indexes for archive table
CREATE INDEX IF NOT EXISTS idx_archive_device_time ON timestamped_data_archive(device_id, data_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_archive_timestamp ON timestamped_data_archive(data_timestamp DESC);

-- Materialized view for recent data
CREATE MATERIALIZED VIEW IF NOT EXISTS recent_device_history AS
SELECT device_id, payload, data_timestamp, data_type, is_offline
FROM timestamped_data
WHERE data_timestamp >= NOW() - INTERVAL '7 days'
ORDER BY device_id, data_timestamp DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_recent_history_device_time ON recent_device_history(device_id, data_timestamp);

-- Auto-refresh function for materialized view
CREATE OR REPLACE FUNCTION refresh_recent_device_history()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY recent_device_history;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS refresh_recent_history_trigger ON timestamped_data;

CREATE TRIGGER refresh_recent_history_trigger
AFTER INSERT OR UPDATE OR DELETE ON timestamped_data
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_recent_device_history();

-- Cleanup function for very old data
CREATE OR REPLACE FUNCTION cleanup_old_data()
RETURNS void AS $$
DECLARE
    cutoff_date timestamp;
BEGIN
    cutoff_date := NOW() - INTERVAL '365 days';
    
    -- Delete old data from archive
    DELETE FROM timestamped_data_archive
    WHERE data_timestamp < cutoff_date;
    
    -- Also check main table for any old data
    DELETE FROM timestamped_data
    WHERE data_timestamp < cutoff_date;
END;
$$ LANGUAGE plpgsql;

-- Set table statistics for better query planning
ALTER TABLE timestamped_data ALTER COLUMN device_id SET STATISTICS 1000;
ALTER TABLE timestamped_data ALTER COLUMN data_timestamp SET STATISTICS 1000;

-- Initial data maintenance
ANALYZE timestamped_data;
ANALYZE timestamped_data_archive;
