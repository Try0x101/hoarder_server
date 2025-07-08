#!/usr/bin/env python3
import asyncio
import asyncpg
import sys

DB_CONFIG = {"user":"admin","password":"admin","database":"hoarder_api","host":"localhost"}

async def create_api_tables():
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        print("Creating API database schema...")
        
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
                data_type TEXT DEFAULT 'telemetry',
                is_offline BOOLEAN DEFAULT FALSE,
                batch_id TEXT
            ) PARTITION BY RANGE (data_timestamp)
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
        
        await conn.execute(f"CREATE INDEX IF NOT EXISTS {partition_name}_device_id_idx ON {partition_name} (device_id)")
        await conn.execute(f"CREATE INDEX IF NOT EXISTS {partition_name}_timestamp_idx ON {partition_name} (data_timestamp DESC)")
        
        print("API database schema created successfully")
        
    except Exception as e:
        print(f"Error creating API schema: {e}")
        sys.exit(1)
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(create_api_tables())
