import datetime,json
from fastapi import APIRouter,Query,HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
import asyncpg
from app.db import DB_CONFIG, get_timestamped_history

router=APIRouter()

@router.get("/data/history")
async def get_history(
    device_id:str=Query(None,description="Device ID to get history for (optional)"),
    limit:int=Query(256,description="Maximum number of records to return per page. Max 1024."),
    days:int=Query(30,description="Number of days of history to retrieve"),
    cursor:Optional[str]=Query(None, description="Timestamp cursor for pagination")
):
 try:
  if limit > 1024:
      limit = 1024
  
  conn=await asyncpg.connect(**DB_CONFIG)
  try:
   if not device_id:
    cutoff_date=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=365)
    devices=await conn.fetch("SELECT device_id,MAX(received_at) as last_active,COUNT(*) as record_count FROM device_data WHERE received_at>=$1 GROUP BY device_id ORDER BY last_active DESC",cutoff_date)
    total_records=await conn.fetchval("SELECT COUNT(*) FROM device_data WHERE received_at>=$1",cutoff_date)
    device_links=[{"device_id":d["device_id"],"last_active":d["last_active"].isoformat(),"record_count":d["record_count"],"links":{"history":f"/data/history?device_id={d['device_id']}&limit={limit}&days={days}","gaps":f"/data/gaps?device_id={d['device_id']}","summary":f"/data/summary?device_id={d['device_id']}"}}for d in devices]
    return{"server_time":datetime.datetime.now().isoformat(),"period":"1year","total_devices":len(devices),"total_records":total_records,"devices":device_links}
   
   results,record_count,next_cursor=await get_timestamped_history(device_id=device_id, days=days, limit=limit, last_timestamp=cursor)
   
   if record_count==0:
    return JSONResponse(content={"data":[],"info":f"No data found for device '{device_id}' in the last {days} days","links":{"back_to_devices":f"/data/history?days={days}"}})
   
   response={
       "device_id":device_id,
       "period":f"{days}days",
       "total_meaningful_changes":record_count,
       "records_shown":len(results),
       "pagination":{
           "limit":limit,
           "next_cursor": next_cursor
        },
       "links":{
           "back_to_devices":f"/data/history?days={days}",
           "gaps":f"/data/gaps?device_id={device_id}",
           "summary":f"/data/summary?device_id={device_id}"
        }
    }

   if next_cursor:
       response["links"]["next_page"] = f"/data/history?device_id={device_id}&limit={limit}&days={days}&cursor={next_cursor}"

   response["data"]=results
   return response
  finally:await conn.close()
 except Exception as e:
  print(f"[{datetime.datetime.now()}] ERROR in history endpoint: {str(e)}")
  import traceback
  print(f"[{datetime.datetime.now()}] {traceback.format_exc()}")
  return JSONResponse(status_code=500,content={"error":f"Internal server error: {str(e)}"})

@router.get("/data/gaps")
async def get_data_gaps(device_id:str=Query(...,description="Device ID to analyze")):
 try:
  cutoff_date=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=365)
  conn=await asyncpg.connect(**DB_CONFIG)
  try:
   query="WITH time_series AS(SELECT device_id,received_at,LEAD(received_at)OVER(PARTITION BY device_id ORDER BY received_at)as next_timestamp FROM device_data WHERE device_id=$1 AND received_at>=$2)SELECT device_id,received_at as gap_start,next_timestamp as gap_end,EXTRACT(EPOCH FROM(next_timestamp-received_at))/60 as gap_minutes FROM time_series WHERE EXTRACT(EPOCH FROM(next_timestamp-received_at))/60>15 ORDER BY gap_minutes DESC LIMIT 100"
   gaps=await conn.fetch(query,device_id,cutoff_date)
   stats_query="SELECT COUNT(*)as record_count,MIN(received_at)as first_record,MAX(received_at)as last_record,EXTRACT(EPOCH FROM(MAX(received_at)-MIN(received_at)))/3600 as hours_span FROM device_data WHERE device_id=$1 AND received_at>=$2"
   stats=await conn.fetchrow(stats_query,device_id,cutoff_date)
   return{"device_id":device_id,"period":"1year","statistics":{"total_records":stats["record_count"],"first_record":stats["first_record"].isoformat() if stats["first_record"] else None,"last_record":stats["last_record"].isoformat() if stats["last_record"] else None,"time_span_hours":stats["hours_span"],"average_record_interval_minutes":(stats["hours_span"]*60/stats["record_count"]) if stats["record_count"]>0 else None},"gaps":[{"start":gap["gap_start"].isoformat(),"end":gap["gap_end"].isoformat() if gap["gap_end"] else None,"duration_minutes":gap["gap_minutes"]}for gap in gaps]}
  finally:await conn.close()
 except Exception as e:
  print(f"[{datetime.datetime.now()}] ERROR in gaps endpoint: {str(e)}")
  import traceback
  print(f"[{datetime.datetime.now()}] {traceback.format_exc()}")
  return JSONResponse(status_code=500,content={"error":f"Internal server error: {str(e)}"})

@router.get("/data/summary")
async def get_data_summary(device_id:str=Query(...,description="Device ID to analyze")):
 try:
  cutoff_date=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=365)
  conn=await asyncpg.connect(**DB_CONFIG)
  try:
   query="WITH hourly_stats AS(SELECT device_id,DATE_TRUNC('hour',received_at)as hour,COUNT(*)as records_per_hour FROM device_data WHERE device_id=$1 AND received_at>=$2 GROUP BY device_id,hour ORDER BY hour)SELECT AVG(records_per_hour)as avg_records_per_hour,MAX(records_per_hour)as max_records_per_hour,MIN(records_per_hour)as min_records_per_hour,COUNT(DISTINCT hour)as active_hours,SUM(records_per_hour)as total_records FROM hourly_stats"
   stats=await conn.fetchrow(query,device_id,cutoff_date)
   position_query="WITH position_data AS(SELECT(payload->>'lat')::float as lat,(payload->>'lon')::float as lon FROM device_data WHERE device_id=$1 AND received_at>=$2 AND payload->>'lat' IS NOT NULL AND payload->>'lon' IS NOT NULL AND(payload->>'lat')::float BETWEEN-90 AND 90 AND(payload->>'lon')::float BETWEEN-180 AND 180)SELECT COUNT(*)as position_count,AVG(lat)as avg_lat,AVG(lon)as avg_lon,MIN(lat)as min_lat,MAX(lat)as max_lat,MIN(lon)as min_lon,MAX(lon)as max_lon FROM position_data"
   position_stats=await conn.fetchrow(position_query,device_id,cutoff_date)
   total_hours=(datetime.datetime.now(datetime.timezone.utc)-cutoff_date).total_seconds()/3600
   return{"device_id":device_id,"period":"1year","activity_statistics":{"total_records":stats["total_records"],"active_hours":stats["active_hours"],"total_hours_in_period":total_hours,"activity_percentage":(stats["active_hours"]/total_hours)*100 if total_hours>0 else 0,"avg_records_per_hour":stats["avg_records_per_hour"],"max_records_per_hour":stats["max_records_per_hour"],"min_records_per_hour":stats["min_records_per_hour"]},"position_statistics":{"has_position_data":position_stats["position_count"]>0,"position_count":position_stats["position_count"],"center_position":{"lat":position_stats["avg_lat"],"lon":position_stats["avg_lon"]} if position_stats["position_count"]>0 else None,"bounds":{"min_lat":position_stats["min_lat"],"max_lat":position_stats["max_lat"],"min_lon":position_stats["min_lon"],"max_lon":position_stats["max_lon"]} if position_stats["position_count"]>0 else None}}
  finally:await conn.close()
 except Exception as e:
  print(f"[{datetime.datetime.now()}] ERROR in summary endpoint: {str(e)}")
  import traceback
  print(f"[{datetime.datetime.now()}] {traceback.format_exc()}")
  return JSONResponse(status_code=500,content={"error":f"Internal server error: {str(e)}"})
