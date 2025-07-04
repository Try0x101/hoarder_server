CREATE TABLE IF NOT EXISTS device_deltas (
    id BIGSERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    delta_payload JSONB NOT NULL,
    data_timestamp TIMESTAMPTZ NOT NULL,
    prev_timestamp TIMESTAMPTZ,
    change_magnitude SMALLINT DEFAULT 1,
    data_type TEXT DEFAULT 'delta',
    is_offline BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT now()
) PARTITION BY RANGE (data_timestamp);

CREATE INDEX IF NOT EXISTS idx_device_deltas_device_time ON device_deltas(device_id, data_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_device_deltas_timestamp ON device_deltas(data_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_device_deltas_magnitude ON device_deltas(device_id, change_magnitude DESC, data_timestamp DESC);

CREATE TABLE IF NOT EXISTS device_history_cache (
    cache_key TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    query_params JSONB NOT NULL,
    result_data JSONB NOT NULL,
    record_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_history_cache_device ON device_history_cache(device_id, expires_at DESC);
CREATE INDEX IF NOT EXISTS idx_history_cache_expires ON device_history_cache(expires_at);

CREATE MATERIALIZED VIEW IF NOT EXISTS device_activity_summary AS
SELECT 
    device_id,
    DATE_TRUNC('hour', data_timestamp) as hour,
    COUNT(*) as record_count,
    MIN(data_timestamp) as first_record,
    MAX(data_timestamp) as last_record,
    COUNT(CASE WHEN change_magnitude >= 3 THEN 1 END) as significant_changes
FROM device_deltas
WHERE data_timestamp >= NOW() - INTERVAL '90 days'
GROUP BY device_id, DATE_TRUNC('hour', data_timestamp);

CREATE UNIQUE INDEX IF NOT EXISTS idx_activity_summary_device_hour 
ON device_activity_summary(device_id, hour DESC);

CREATE OR REPLACE FUNCTION create_delta_partition_for_date(target_date TIMESTAMPTZ)
RETURNS void AS $$
DECLARE
    partition_start TIMESTAMPTZ;
    partition_end TIMESTAMPTZ;
    partition_name TEXT;
BEGIN
    partition_start := DATE_TRUNC('day', target_date);
    partition_end := partition_start + INTERVAL '1 day';
    partition_name := 'device_deltas_' || TO_CHAR(partition_start, 'YYYYMMDD');
    
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = partition_name) THEN
        EXECUTE format('CREATE TABLE %I PARTITION OF device_deltas 
                       FOR VALUES FROM (%L) TO (%L)', 
                       partition_name, partition_start, partition_end);
        
        EXECUTE format('CREATE INDEX ON %I (device_id, data_timestamp DESC)', partition_name);
        EXECUTE format('CREATE INDEX ON %I (data_timestamp DESC)', partition_name);
        
        RAISE NOTICE 'Created partition %', partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION calculate_change_magnitude(new_payload JSONB, prev_payload JSONB)
RETURNS SMALLINT AS $$
DECLARE
    change_count INTEGER := 0;
    key TEXT;
    new_val JSONB;
    prev_val JSONB;
BEGIN
    FOR key IN SELECT jsonb_object_keys(new_payload)
    LOOP
        IF key NOT IN ('id', 'timestamp', 'source_ip', 'server_received_at', 'batch_id') THEN
            new_val := new_payload->key;
            prev_val := prev_payload->key;
            
            IF prev_val IS NULL OR new_val != prev_val THEN
                change_count := change_count + 1;
                
                IF key IN ('lat', 'lon', 'weather_temp', 'weather_code') THEN
                    change_count := change_count + 2;
                END IF;
            END IF;
        END IF;
    END LOOP;
    
    RETURN LEAST(change_count, 10);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cleanup_expired_cache()
RETURNS void AS $$
BEGIN
    DELETE FROM device_history_cache WHERE expires_at < NOW();
    
    DELETE FROM device_deltas 
    WHERE data_timestamp < NOW() - INTERVAL '365 days';
END;
$$ LANGUAGE plpgsql;

SELECT create_delta_partition_for_date(NOW());
SELECT create_delta_partition_for_date(NOW() + INTERVAL '1 day');
SELECT create_delta_partition_for_date(NOW() + INTERVAL '2 days');

ANALYZE device_deltas;
ANALYZE device_history_cache;
