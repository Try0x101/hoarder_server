import datetime,json
from fastapi import APIRouter,Query,HTTPException, Request
from fastapi.responses import JSONResponse
from typing import Optional
from app import db
from app.db import get_timestamped_history

router=APIRouter()

@router.get("/data/history")
async def get_history(
    request: Request,
    device_id:str=Query(None,description="Device ID to get history for (optional)"),
    limit:int=Query(256,description="Maximum number of records to return per page. Max 1024."),
    days:int=Query(30,description="Number of days of history to retrieve"),
    cursor:Optional[str]=Query(None, description="Timestamp cursor for pagination")
):
    if limit > 1024:
        limit = 1024

    base_url = f"{request.url.scheme}://{request.url.netloc}"

    if not device_id:
        async with (await db.get_database_pool()).acquire() as conn:
            cutoff_date=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=days)

            devices_query = "SELECT device_id, received_at as last_active FROM latest_device_states WHERE received_at >= $1 ORDER BY last_active DESC"
            devices = await conn.fetch(devices_query, cutoff_date, timeout=15)

            total_devices = len(devices)

            device_links=[{
                "device_id": d["device_id"],
                "last_active": d["last_active"].isoformat(),
                "links": {
                    "self": f"{base_url}/data/history?device_id={d['device_id']}&limit={limit}&days={days}",
                    "latest": f"{base_url}/data/latest/{d['device_id']}",
                    "gaps": f"{base_url}/data/gaps?device_id={d['device_id']}",
                    "summary": f"{base_url}/data/summary?device_id={d['device_id']}"
                }
            } for d in devices]

            return {
                "links": {
                    "self": f"{base_url}{request.url.path}?{request.url.query}",
                    "home": f"{base_url}/"
                },
                "server_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "period": f"{days}days",
                "total_devices": total_devices,
                "devices": device_links
            }

    results, next_cursor = await get_timestamped_history(device_id=device_id, days=days, limit=limit, last_timestamp=cursor)

    if not results:
        return JSONResponse(content={"data":[],"info":f"No data found for device '{device_id}' in the last {days} days","links":{"up":f"{base_url}/data/history?days={days}"}})

    response={
       "links":{
           "self": f"{base_url}{request.url.path}?{request.url.query}",
           "up": f"{base_url}/data/history?days={days}",
           "latest": f"{base_url}/data/latest/{device_id}",
           "gaps":f"{base_url}/data/gaps?device_id={device_id}",
           "summary":f"{base_url}/data/summary?device_id={device_id}"
        },
       "device_id":device_id,
       "period":f"{days}days",
       "records_shown":len(results),
       "pagination":{
           "limit":limit,
           "next_cursor": next_cursor
        },
       "data": results
    }

    if next_cursor:
       response["links"]["next_page"] = f"{base_url}/data/history?device_id={device_id}&limit={limit}&days={days}&cursor={next_cursor}"

    return response

@router.get("/data/gaps")
async def get_data_gaps(request: Request, device_id:str=Query(...,description="Device ID to analyze")):
    base_url = f"{request.url.scheme}://{request.url.netloc}"
    cutoff_date=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=365)
    async with (await db.get_database_pool()).acquire() as conn:
        query="WITH time_series AS(SELECT device_id,data_timestamp,LEAD(data_timestamp)OVER(PARTITION BY device_id ORDER BY data_timestamp)as next_timestamp FROM timestamped_data WHERE device_id=$1 AND data_timestamp>=$2)SELECT device_id,data_timestamp as gap_start,next_timestamp as gap_end,EXTRACT(EPOCH FROM(next_timestamp-data_timestamp))/60 as gap_minutes FROM time_series WHERE EXTRACT(EPOCH FROM(next_timestamp-data_timestamp))/60>15 ORDER BY gap_minutes DESC LIMIT 100"
        gaps=await conn.fetch(query,device_id,cutoff_date, timeout=15)
        stats_query="SELECT COUNT(*)as record_count,MIN(data_timestamp)as first_record,MAX(data_timestamp)as last_record,EXTRACT(EPOCH FROM(MAX(data_timestamp)-MIN(data_timestamp)))/3600 as hours_span FROM timestamped_data WHERE device_id=$1 AND data_timestamp>=$2"
        stats=await conn.fetchrow(stats_query,device_id,cutoff_date, timeout=15)
        return {
            "links": {
                "self": f"{base_url}{request.url.path}?{request.url.query}",
                "up": f"{base_url}/data/history?device_id={device_id}",
                "latest": f"{base_url}/data/latest/{device_id}",
                "summary": f"{base_url}/data/summary?device_id={device_id}"
            },
            "device_id":device_id,
            "period":"1year",
            "statistics":{"total_records":stats["record_count"],"first_record":stats["first_record"].isoformat() if stats["first_record"] else None,"last_record":stats["last_record"].isoformat() if stats["last_record"] else None,"time_span_hours":stats["hours_span"],"average_record_interval_minutes":(stats["hours_span"]*60/stats["record_count"]) if stats["record_count"]>0 else None},
            "gaps":[{"start":gap["gap_start"].isoformat(),"end":gap["gap_end"].isoformat() if gap["gap_end"] else None,"duration_minutes":gap["gap_minutes"]}for gap in gaps]
        }

@router.get("/data/summary")
async def get_data_summary(request: Request, device_id:str=Query(...,description="Device ID to analyze")):
    base_url = f"{request.url.scheme}://{request.url.netloc}"
    cutoff_date=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=365)
    async with (await db.get_database_pool()).acquire() as conn:
        query="WITH hourly_stats AS(SELECT device_id,DATE_TRUNC('hour',data_timestamp)as hour,COUNT(*)as records_per_hour FROM timestamped_data WHERE device_id=$1 AND data_timestamp>=$2 GROUP BY device_id,hour ORDER BY hour)SELECT AVG(records_per_hour)as avg_records_per_hour,MAX(records_per_hour)as max_records_per_hour,MIN(records_per_hour)as min_records_per_hour,COUNT(DISTINCT hour)as active_hours,SUM(records_per_hour)as total_records FROM hourly_stats"
        stats=await conn.fetchrow(query,device_id,cutoff_date, timeout=15)
        position_query="WITH position_data AS(SELECT safe_cast_to_float(payload->>'lat') as lat, safe_cast_to_float(payload->>'lon') as lon FROM timestamped_data WHERE device_id=$1 AND data_timestamp>=$2 AND payload->>'lat' IS NOT NULL AND payload->>'lon' IS NOT NULL) SELECT COUNT(*) as position_count, AVG(lat) as avg_lat, AVG(lon) as avg_lon, MIN(lat) as min_lat, MAX(lat) as max_lat, MIN(lon) as min_lon, MAX(lon) as max_lon FROM position_data WHERE lat IS NOT NULL AND lon IS NOT NULL AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180"
        position_stats=await conn.fetchrow(position_query,device_id,cutoff_date, timeout=15)
        total_hours=(datetime.datetime.now(datetime.timezone.utc)-cutoff_date).total_seconds()/3600
        return {
            "links": {
                "self": f"{base_url}{request.url.path}?{request.url.query}",
                "up": f"{base_url}/data/history?device_id={device_id}",
                "latest": f"{base_url}/data/latest/{device_id}",
                "gaps": f"{base_url}/data/gaps?device_id={device_id}"
            },
            "device_id":device_id,
            "period":"1year",
            "activity_statistics":{"total_records":stats["total_records"],"active_hours":stats["active_hours"],"total_hours_in_period":total_hours,"activity_percentage":(stats["active_hours"]/total_hours)*100 if total_hours>0 else 0,"avg_records_per_hour":stats["avg_records_per_hour"],"max_records_per_hour":stats["max_records_per_hour"],"min_records_per_hour":stats["min_records_per_hour"]},
            "position_statistics":{"has_position_data":position_stats["position_count"]>0,"position_count":position_stats["position_count"],"center_position":{"lat":position_stats["avg_lat"],"lon":position_stats["avg_lon"]} if position_stats["position_count"]>0 else None,"bounds":{"min_lat":position_stats["min_lat"],"max_lat":position_stats["max_lat"],"min_lon":position_stats["min_lon"],"max_lon":position_stats["max_lon"]} if position_stats["position_count"]>0 else None}
        }
