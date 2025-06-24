import asyncpg
import json
import datetime
import copy
import asyncio

from app.utils import transform_device_data, deep_merge

DB_CONFIG = {
    "user": "admin",
    "password": "admin",
    "database": "database",
    "host": "localhost"
}

pool = None
_init_lock = asyncio.Lock()
_initialized = False

async def init_db():
    global pool, _initialized
    
    # Используем блокировку для предотвращения параллельной инициализации
    async with _init_lock:
        if _initialized:
            return
            
        pool = await asyncpg.create_pool(**DB_CONFIG)
        
        async with pool.acquire() as conn:
            try:
                # Проверяем, существуют ли таблицы
                tables_exist = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name IN ('device_data', 'latest_device_states')
                    AND table_schema = 'public'
                """)
                
                # Пересоздаем таблицы только если они не существуют или если нужно принудительно
                if tables_exist < 2:
                    print(f"[{datetime.datetime.now()}] Creating database tables...")
                    
                    # Используем IF EXISTS для безопасного удаления
                    await conn.execute("DROP TABLE IF EXISTS device_data CASCADE")
                    await conn.execute("DROP TABLE IF EXISTS latest_device_states CASCADE")

                    await conn.execute("""
                    CREATE TABLE IF NOT EXISTS device_data (
                        id SERIAL PRIMARY KEY,
                        device_id TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        received_at TIMESTAMPTZ DEFAULT now()
                    )
                    """)
                    
                    await conn.execute("""
                    CREATE TABLE IF NOT EXISTS latest_device_states (
                        device_id TEXT PRIMARY KEY,
                        payload JSONB NOT NULL,
                        received_at TIMESTAMPTZ DEFAULT now()
                    )
                    """)
                    
                    print(f"[{datetime.datetime.now()}] Database tables created successfully.")
                else:
                    print(f"[{datetime.datetime.now()}] Database tables already exist, skipping creation.")
                
                _initialized = True
                
            except Exception as e:
                print(f"[{datetime.datetime.now()}] Error during database initialization: {e}")
                # Если произошла ошибка, не помечаем как инициализированную
                raise


async def save_data(data: dict):
    device_id = data.get("device_id") or data.get("id")
    if not device_id:
        # print(f"[{datetime.datetime.now()}] Warning: No device_id found in data: {data}")
        return

    async with pool.acquire() as conn:
        # Save historical data
        await conn.execute(
            "INSERT INTO device_data (device_id, payload) VALUES ($1, $2)",
            device_id, json.dumps(data)
        )
        # print(f"[{datetime.datetime.now()}] Device {device_id}: Historical data saved.")

        # Merge with latest state and update latest_device_states table
        existing_latest_state_row = await conn.fetchrow(
            "SELECT payload FROM latest_device_states WHERE device_id = $1", device_id
        )

        existing_payload_dict = {}
        if existing_latest_state_row and existing_latest_state_row['payload']:
            try:
                existing_payload_dict = json.loads(existing_latest_state_row['payload'])
                # print(f"[{datetime.datetime.now()}] Device {device_id}: Existing payload from DB: {existing_payload_dict}")
            except json.JSONDecodeError as e:
                # print(f"[{datetime.datetime.now()}] Device {device_id}: Error decoding existing payload from DB: {e}. Payload: {existing_latest_state_row['payload']}")
                existing_payload_dict = {}
        # else:
            # print(f"[{datetime.datetime.now()}] Device {device_id}: No existing latest state found in DB.")

        current_state_for_merge = copy.deepcopy(existing_payload_dict)

        merged_data = deep_merge(data, current_state_for_merge)
        # print(f"[{datetime.datetime.now()}] Device {device_id}: Incoming delta (source): {data}")
        # print(f"[{datetime.datetime.now()}] Device {device_id}: Merged result (destination): {merged_data}")


        await conn.execute(
            """
            INSERT INTO latest_device_states (device_id, payload, received_at)
            VALUES ($1, $2, now())
            ON CONFLICT (device_id) DO UPDATE SET
                payload = EXCLUDED.payload,
                received_at = EXCLUDED.received_at
            """,
            device_id, json.dumps(merged_data)
        )
        # print(f"[{datetime.datetime.now()}] Device {device_id}: Latest state updated in DB.")


async def get_latest_data():
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT device_id, payload, received_at FROM latest_device_states ORDER BY received_at DESC"
        )
        return [{"device_id": r["device_id"], "payload": transform_device_data(json.loads(r["payload"])), "time": r["received_at"].isoformat()} for r in rows]
