#!/usr/bin/env python3
import asyncio
import asyncpg
import sys

DB_CONFIG = {"user":"admin","password":"admin","database":"database","host":"localhost"}

async def create_tables():
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        print("Creating database schema...")
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS device_data (
                id SERIAL PRIMARY KEY,
                device_id TEXT NOT NULL,
                payload JSONB NOT NULL,
                received_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_device_data_device_id ON device_data(device_id)
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_device_data_received_at ON device_data(received_at DESC)
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS latest_device_states (
                device_id TEXT PRIMARY KEY,
                payload JSONB NOT NULL,
                received_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS timestamped_data (
                device_id TEXT NOT NULL,
                payload JSONB NOT NULL,
                data_timestamp TIMESTAMPTZ NOT NULL,
                received_at TIMESTAMPTZ DEFAULT NOW(),
                data_type TEXT DEFAULT 'delta',
                is_offline BOOLEAN DEFAULT FALSE,
                batch_id TEXT
            ) PARTITION BY RANGE (data_timestamp)
        """)
        
        await conn.execute("""
            CREATE OR REPLACE FUNCTION jsonb_recursive_merge(a JSONB, b JSONB)
            RETURNS JSONB AS $$
            BEGIN
                IF a IS NULL THEN RETURN b; END IF;
                IF b IS NULL THEN RETURN a; END IF;
                RETURN a || b;
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        current_month = "2025-07-01 00:00:00+00"
        next_month = "2025-08-01 00:00:00+00"
        partition_name = "timestamped_data_y2025m07"
        
        try:
            await conn.execute(f"""
                CREATE TABLE {partition_name} PARTITION OF timestamped_data
                FOR VALUES FROM ('{current_month}') TO ('{next_month}')
            """)
            print(f"Created partition: {partition_name}")
        except asyncpg.exceptions.DuplicateTableError:
            print(f"Partition {partition_name} already exists")
        
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS {partition_name}_device_id_data_timestamp_idx 
            ON {partition_name} (device_id, data_timestamp DESC)
        """)
        
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS {partition_name}_data_timestamp_idx 
            ON {partition_name} (data_timestamp DESC)
        """)
        
        print("Database schema created successfully")
        
    except Exception as e:
        print(f"Error creating schema: {e}")
        sys.exit(1)
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(create_tables())
