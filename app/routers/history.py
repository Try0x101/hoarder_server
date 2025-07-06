import datetime
from fastapi import APIRouter, Query, Request
from typing import Optional
from app.api.history.handlers import handle_device_list, handle_device_history
from app.api.history.queries import get_device_gaps_data, get_device_statistics, get_device_activity_stats, get_device_position_stats
from app.api.shared.url_helpers import build_base_url, safe_int_param, create_device_links

router = APIRouter()

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

    if not device_id:
        return await handle_device_list(request, limit, days)
    else:
        return await handle_device_history(request, device_id, limit, days, cursor)

@router.get("/data/gaps")
async def get_data_gaps(request: Request, device_id: str = Query(..., description="Device ID to analyze")):
    base_url = build_base_url(request)
    cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=365)
    
    gaps = await get_device_gaps_data(device_id, cutoff_date)
    stats = await get_device_statistics(device_id, cutoff_date)
    
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
    
    activity = await get_device_activity_stats(device_id, cutoff_date)
    position = await get_device_position_stats(device_id, cutoff_date)
    
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
