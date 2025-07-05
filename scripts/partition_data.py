#!/usr/bin/env python3
import asyncio
import asyncpg
import datetime
import argparse

DB_CONFIG = {"user":"admin","password":"admin","database":"database","host":"localhost"}

async def partition_old_data(days_threshold=90):
    """Move data older than threshold to archive table"""
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        print(f"Setting up partitioning for data older than {days_threshold} days...")
        
        # Create archive table if it doesn't exist
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS timestamped_data_archive (
                LIKE timestamped_data INCLUDING ALL
            )
        """)
        
        # Create index on archive table
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_archive_device_time
            ON timestamped_data_archive(device_id, data_timestamp DESC)
        """)
        
        # Calculate cutoff date
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_threshold)
        
        # Count records to be moved
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM timestamped_data WHERE data_timestamp < $1",
            cutoff_date
        )
        
        if count == 0:
            print(f"No records older than {days_threshold} days found.")
            return
            
        print(f"Moving {count} records to archive table...")
        
        # Begin transaction
        tr = conn.transaction()
        await tr.start()
        
        try:
            # Move data to archive
            moved = await conn.execute("""
                INSERT INTO timestamped_data_archive
                SELECT * FROM timestamped_data
                WHERE data_timestamp < $1
            """, cutoff_date)
            
            # Delete from main table
            deleted = await conn.execute("""
                DELETE FROM timestamped_data
                WHERE data_timestamp < $1
            """, cutoff_date)
            
            await tr.commit()
            print(f"Successfully moved records to archive table.")
            
        except Exception as e:
            await tr.rollback()
            print(f"Error during partitioning: {e}")
            
    finally:
        await conn.close()

async def setup_cleanup_function():
    """Set up a PostgreSQL function for automatic cleanup of very old data"""
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        # Create function to clean up very old data (older than 1 year)
        await conn.execute("""
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
        """)
        
        print("Created cleanup function for very old data")
        
    except Exception as e:
        print(f"Error setting up cleanup function: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Partition old IoT telemetry data")
    parser.add_argument("--days", type=int, default=90, help="Age threshold in days (default: 90)")
    args = parser.parse_args()
    
    asyncio.run(partition_old_data(args.days))
    asyncio.run(setup_cleanup_function())
