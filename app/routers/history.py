import datetime
import json
from fastapi import APIRouter, Query, HTTPException, Request
from fastapi.responses import JSONResponse
from typing import Optional
from app import db
from app.db import get_timestamped_history

router = APIRouter()

def safe_int_param(value: str, default: int, min_val: int, max_val: int) -> int:
    try:
        result = int(value) if value else default
        return max(min_val, min(result, max_val))
    except (ValueError, TypeError):
        return default

def build_base_url(request: Request) -> str:
    return f"{request.url.scheme}://{request.url.netloc}"

def create_device_links(base_url: str, device_id: str, limit: int, days: int) -> dict:
    return {
        "self": f"{base_url}/data/history?device_id={device_id}&limit={limit}&days={days}",
        "latest": f"{base_url}/data/latest/{device_id}",
        "gaps": f"{base_url}/data/gaps?device_id={device_id}",
        "summary": f"{base_url}/data/summary?device_id={device_id}"
    }

async def get_active_devices(days: int):
    async with (await db.get_database_pool()).acquire() as conn:
        cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        return await conn.fetch(
            "SELECT device_id, received_at as last_active FROM latest_device_states WHERE received_at >= $1 ORDER BY last_active DESC",
            cutoff_date, timeout=15
        )

@router.get("/data/history")
async def get_history(
    request: Request,
    device_id: str = Query(None, description="Device ID to get history for"),
    limit: int = Query(256, description="Maximum records per page"),
    days: int = Query(30, description="Days of history to retrieve"),
    cursor: Optional[str] = Query(None, description="Timestamp cursor for pagination")
):
    limit = safe_int_param(str(limit), 256, 1, 1024)
    days = safe_int_param(str(days), 30, 1, 365)
    base_url = build_base_url(request)

    if not device_id:
        devices = await get_active_devices(days)
        device_links = [{
            "device_id": d["device_id"],
            "last_active": d["last_active"].isoformat(),
            "links": create_device_links(base_url, d["device_id"], limit, days)
        } for d in devices]

        return {
            "links": {
                "self": f"{base_url}{request.url.path}?{request.url.query}",
                "home": f"{base_url}/"
            },
            "server_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "period": f"{days}days",
            "total_devices": len(devices),
            "devices": device_links
        }

    results, next_cursor = await get_timestamped_history(
        device_id=device_id, days=days, limit=limit, last_timestamp=cursor
    )

    if not results:
        return JSONResponse(content={
            "data": [],
            "info": f"No data found for device '{device_id}' in the last {days} days",
            "links": {"up": f"{base_url}/data/history?days={days}"}
        })

    response = {
        "links": {
            "self": f"{base_url}{request.url.path}?{request.url.query}",
            "up": f"{base_url}/data/history?days={days}",
            **create_device_links(base_url, device_id, limit, days)
        },
        "device_id": device_id,
        "period": f"{days}days",
        "records_shown": len(results),
        "pagination": {"limit": limit, "next_cursor": next_cursor},
        "data": results
    }

    if next_cursor:
        response["links"]["next_page"] = f"{base_url}/data/history?device_id={device_id}&limit={limit}&days={days}&cursor={next_cursor}"

    return response

@router.get("/data/gaps")
async def get_data_gaps(request: Request, device_id: str = Query(..., description="Device ID to analyze")):
    base_url = build_base_url(request)
    cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=365)
    
    async with (await db.get_database_pool()).acquire() as conn:
        gaps_query = """
        WITH time_series AS (
            SELECT device_id, data_timestamp,
                   LEAD(data_timestamp) OVER (PARTITION BY device_id ORDER BY data_timestamp) as next_timestamp
            FROM timestamped_data 
            WHERE device_id = $1 AND data_timestamp >= $2
        )
        SELECT device_id, data_timestamp as gap_start, next_timestamp as gap_end,
               EXTRACT(EPOCH FROM (next_timestamp - data_timestamp))/60 as gap_minutes
        FROM time_series 
        WHERE EXTRACT(EPOCH FROM (next_timestamp - data_timestamp))/60 > 15 
        ORDER BY gap_minutes DESC LIMIT 100
        """
        gaps = await conn.fetch(gaps_query, device_id, cutoff_date, timeout=15)
        
        stats_query = """
        SELECT COUNT(*) as record_count, MIN(data_timestamp) as first_record,
               MAX(data_timestamp) as last_record,
               EXTRACT(EPOCH FROM (MAX(data_timestamp) - MIN(data_timestamp)))/3600 as hours_span
        FROM timestamped_data 
        WHERE device_id = $1 AND data_timestamp >= $2
        """
        stats = await conn.fetchrow(stats_query, device_id, cutoff_date, timeout=15)
        
        avg_interval = None
        if stats["record_count"] > 0 and stats["hours_span"]:
            avg_interval = (stats["hours_span"] * 60) / stats["record_count"]
        
        return {
            "links": {
                "self": f"{base_url}{request.url.path}?{request.url.query}",
                **create_device_links(base_url, device_id, 256, 30)
            },
            "device_id": device_id,
            "period": "1year",
            "statistics": {
                "total_records": stats["record_count"],
                "first_record": stats["first_record"].isoformat() if stats["first_record"] else None,
                "last_record": stats["last_record"].isoformat() if stats["last_record"] else None,
                "time_span_hours": stats["hours_span"],
                "average_record_interval_minutes": avg_interval
            },
            "gaps": [{
                "start": gap["gap_start"].isoformat(),
                "end": gap["gap_end"].isoformat() if gap["gap_end"] else None,
                "duration_minutes": gap["gap_minutes"]
            } for gap in gaps]
        }

@router.get("/data/summary")
async def get_data_summary(request: Request, device_id: str = Query(..., description="Device ID to analyze")):
    base_url = build_base_url(request)
    cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=365)
    
    async with (await db.get_database_pool()).acquire() as conn:
        activity_query = """
        WITH hourly_stats AS (
            SELECT device_id, DATE_TRUNC('hour', data_timestamp) as hour, COUNT(*) as records_per_hour
            FROM timestamped_data WHERE device_id = $1 AND data_timestamp >= $2
            GROUP BY device_id, hour ORDER BY hour
        )
        SELECT AVG(records_per_hour) as avg_records_per_hour, MAX(records_per_hour) as max_records_per_hour,
               MIN(records_per_hour) as min_records_per_hour, COUNT(DISTINCT hour) as active_hours,
               SUM(records_per_hour) as total_records
        FROM hourly_stats
        """
        activity = await conn.fetchrow(activity_query, device_id, cutoff_date, timeout=15)
        
        position_query = """
        SELECT COUNT(*) as position_count, AVG(CAST(payload->>'lat' AS FLOAT)) as avg_lat,
               AVG(CAST(payload->>'lon' AS FLOAT)) as avg_lon, MIN(CAST(payload->>'lat' AS FLOAT)) as min_lat,
               MAX(CAST(payload->>'lat' AS FLOAT)) as max_lat, MIN(CAST(payload->>'lon' AS FLOAT)) as min_lon,
               MAX(CAST(payload->>'lon' AS FLOAT)) as max_lon
        FROM timestamped_data 
        WHERE device_id = $1 AND data_timestamp >= $2 
        AND payload->>'lat' IS NOT NULL AND payload->>'lon' IS NOT NULL
        AND CAST(payload->>'lat' AS FLOAT) BETWEEN -90 AND 90
        AND CAST(payload->>'lon' AS FLOAT) BETWEEN -180 AND 180
        """
        position = await conn.fetchrow(position_query, device_id, cutoff_date, timeout=15)
        
        total_hours = (datetime.datetime.now(datetime.timezone.utc) - cutoff_date).total_seconds() / 3600
        activity_pct = (activity["active_hours"] / total_hours) * 100 if total_hours > 0 else 0
        
        return {
            "links": {
                "self": f"{base_url}{request.url.path}?{request.url.query}",
                **create_device_links(base_url, device_id, 256, 30)
            },
            "device_id": device_id,
            "period": "1year",
            "activity_statistics": {
                "total_records": activity["total_records"],
                "active_hours": activity["active_hours"],
                "total_hours_in_period": total_hours,
                "activity_percentage": activity_pct,
                "avg_records_per_hour": activity["avg_records_per_hour"],
                "max_records_per_hour": activity["max_records_per_hour"],
                "min_records_per_hour": activity["min_records_per_hour"]
            },
            "position_statistics": {
                "has_position_data": (position["position_count"] or 0) > 0,
                "position_count": position["position_count"] or 0,
                "center_position": {
                    "lat": position["avg_lat"], "lon": position["avg_lon"]
                } if position["position_count"] else None,
                "bounds": {
                    "min_lat": position["min_lat"], "max_lat": position["max_lat"],
                    "min_lon": position["min_lon"], "max_lon": position["max_lon"]
                } if position["position_count"] else None
            }
        }
