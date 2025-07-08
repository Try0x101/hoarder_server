#!/usr/bin/env python3
import asyncio
import asyncpg
import sys
import datetime

DB_CONFIG = {"user":"admin","password":"admin","database":"database","host":"localhost"}

async def create_tables():
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        print("Creating database schema for new architecture...")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ingested_data (
                id BIGSERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed BOOLEAN NOT NULL DEFAULT FALSE,
                ingest_endpoint TEXT NOT NULL
            );
        """)
        print("Created 'ingested_data' table.")

        await conn.execute("CREATE INDEX IF NOT EXISTS idx_unprocessed_data ON ingested_data (received_at) WHERE processed = FALSE;")
        print("Created index on 'ingested_data'.")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS latest_device_states (
                device_id TEXT PRIMARY KEY,
                payload JSONB NOT NULL,
                received_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        print("Created 'latest_device_states' table.")

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
        print("Created 'timestamped_data' partitioned table.")

        await conn.execute("DROP TABLE IF EXISTS device_data;")
        print("Dropped redundant 'device_data' table.")

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
        print("Ensured 'jsonb_recursive_merge' function exists.")

        now = datetime.datetime.now(datetime.timezone.utc)
        partition_start_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        partition_end_dt = (partition_start_dt + datetime.timedelta(days=32)).replace(day=1)
        partition_name = f"timestamped_data_y{partition_start_dt.strftime('%Y')}m{partition_start_dt.strftime('%m')}"
        
        exists = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = $1)", partition_name)
        if not exists:
            await conn.execute(f"""
                CREATE TABLE {partition_name} PARTITION OF timestamped_data
                FOR VALUES FROM ('{partition_start_dt.isoformat()}') TO ('{partition_end_dt.isoformat()}')
            """)
            print(f"Created initial partition: {partition_name}")
            await conn.execute(f'CREATE INDEX ON {partition_name} (device_id, data_timestamp DESC);')
            await conn.execute(f'CREATE INDEX ON {partition_name} (data_timestamp DESC);')
            print(f"Created indexes on partition {partition_name}")
        else:
            print(f"Partition {partition_name} already exists.")

        print("Database schema created successfully for the new architecture.")

    except Exception as e:
        print(f"Error creating schema: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(create_tables())
