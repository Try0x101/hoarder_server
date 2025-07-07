import datetime
import json
import traceback
from app import db
from .status import update_status

async def perform_export(export_file: str, status_file: str, device_id: str | None):
    try:
        update_status(status_file, {"status": "in_progress", "message": "Connecting to database"})
        async with (await db.get_database_pool()).acquire() as conn:
            update_status(status_file, {"status": "in_progress", "message": "Fetching data"})
            data = {"metadata": {"export_date": datetime.datetime.now(datetime.timezone.utc).isoformat(), "device_id": device_id}, "data": {}}
            
            tables = ["device_data", "latest_device_states", "timestamped_data"]
            for table in tables:
                query = f"SELECT * FROM {table}"
                params = []
                if device_id:
                    query += " WHERE device_id = $1"
                    params.append(device_id)
                rows = await conn.fetch(query, *params)
                data["data"][table] = [dict(r) for r in rows]

            update_status(status_file, {"status": "in_progress", "message": "Formatting data"})
            for table_data in data["data"].values():
                for row in table_data:
                    for k, v in row.items():
                        if isinstance(v, datetime.datetime):
                            row[k] = v.isoformat()
            
            with open(export_file, "w") as f:
                json.dump(data, f)
            update_status(status_file, {"status": "completed", "message": "Export completed successfully"})
    except Exception as e:
        update_status(status_file, {"status": "failed", "error": str(e), "traceback": traceback.format_exc()})

async def perform_import(import_file: str, status_file: str, merge: bool):
    try:
        with open(import_file, "r") as f: data = json.load(f)
        update_status(status_file, {"status": "in_progress", "message": "Processing records"})
        
        async with (await db.get_database_pool()).acquire() as conn:
            if not merge:
                await conn.execute("TRUNCATE device_data, latest_device_states, timestamped_data")
            
            for table, rows in data["data"].items():
                for row in rows:
                    cols = ", ".join(row.keys())
                    vals = ", ".join([f"${i+1}" for i in range(len(row))])
                    query = f"INSERT INTO {table} ({cols}) VALUES ({vals}) ON CONFLICT DO NOTHING"
                    await conn.execute(query, *row.values())
        
        update_status(status_file, {"status": "completed", "message": "Import completed."})
    except Exception as e:
        update_status(status_file, {"status": "failed", "error": str(e), "traceback": traceback.format_exc()})
