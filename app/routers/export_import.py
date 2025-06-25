import datetime,json,os,tempfile,uuid
import asyncpg
from fastapi import APIRouter,HTTPException,Depends,File,UploadFile,Form,BackgroundTasks
from fastapi.responses import JSONResponse,FileResponse
from typing import Optional
from app.db import DB_CONFIG
router=APIRouter()
EXPORT_DIR="/tmp/hoarder_exports"
IMPORT_DIR="/tmp/hoarder_imports"
if not os.path.exists(EXPORT_DIR):os.makedirs(EXPORT_DIR)
if not os.path.exists(IMPORT_DIR):os.makedirs(IMPORT_DIR)
@router.get("/export/database")
async def export_database(device_id:Optional[str]=None,background_tasks:BackgroundTasks=None):
 try:
  export_id=str(uuid.uuid4())
  export_file=os.path.join(EXPORT_DIR,f"export_{export_id}.json")
  status_file=os.path.join(EXPORT_DIR,f"status_{export_id}.json")
  with open(status_file,"w") as f:json.dump({"status":"started","timestamp":datetime.datetime.now().isoformat()},f)
  background_tasks.add_task(perform_export,export_file,status_file,device_id)
  return{"export_id":export_id,"status":"started","check_status_url":f"/export/status/{export_id}"}
 except Exception as e:
  print(f"[{datetime.datetime.now()}] Export error: {str(e)}")
  raise HTTPException(status_code=500,detail=f"Export failed: {str(e)}")
@router.get("/export/status/{export_id}")
async def export_status(export_id:str):
 status_file=os.path.join(EXPORT_DIR,f"status_{export_id}.json")
 export_file=os.path.join(EXPORT_DIR,f"export_{export_id}.json")
 if not os.path.exists(status_file):return{"status":"not_found","message":"Export ID not found"}
 with open(status_file,"r") as f:status=json.load(f)
 if status.get("status")=="completed" and os.path.exists(export_file):
  status["download_url"]=f"/export/download/{export_id}"
  status["file_size"]=os.path.getsize(export_file)
 return status
@router.get("/export/download/{export_id}")
async def download_export(export_id:str):
 export_file=os.path.join(EXPORT_DIR,f"export_{export_id}.json")
 if not os.path.exists(export_file):raise HTTPException(status_code=404,detail="Export file not found")
 return FileResponse(export_file,filename=f"hoarder_export_{export_id}.json")
@router.post("/import/database")
async def import_database(background_tasks:BackgroundTasks,file:UploadFile=File(...),merge:bool=Form(True)):
 try:
  import_id=str(uuid.uuid4())
  import_file=os.path.join(IMPORT_DIR,f"import_{import_id}.json")
  status_file=os.path.join(IMPORT_DIR,f"status_{import_id}.json")
  with open(import_file,"wb") as f:
   content=await file.read()
   f.write(content)
  with open(status_file,"w") as f:json.dump({"status":"started","timestamp":datetime.datetime.now().isoformat()},f)
  background_tasks.add_task(perform_import,import_file,status_file,merge)
  return{"import_id":import_id,"status":"started","check_status_url":f"/import/status/{import_id}"}
 except Exception as e:
  print(f"[{datetime.datetime.now()}] Import error: {str(e)}")
  raise HTTPException(status_code=500,detail=f"Import failed: {str(e)}")
@router.get("/import/status/{import_id}")
async def import_status(import_id:str):
 status_file=os.path.join(IMPORT_DIR,f"status_{import_id}.json")
 if not os.path.exists(status_file):return{"status":"not_found","message":"Import ID not found"}
 with open(status_file,"r") as f:status=json.load(f)
 return status
async def perform_export(export_file:str,status_file:str,device_id:Optional[str]=None):
 conn=None
 try:
  update_status(status_file,{"status":"in_progress","message":"Connecting to database"})
  conn=await asyncpg.connect(**DB_CONFIG)
  update_status(status_file,{"status":"in_progress","message":"Fetching data"})
  data={"metadata":{"export_date":datetime.datetime.now().isoformat(),"device_id":device_id},"data":{}}
  if device_id:
   device_data=await conn.fetch("SELECT id,device_id,payload,received_at FROM device_data WHERE device_id=$1 ORDER BY received_at",device_id)
   latest_state=await conn.fetchrow("SELECT device_id,payload,received_at FROM latest_device_states WHERE device_id=$1",device_id)
   timestamped_data=await conn.fetch("SELECT id,device_id,payload,data_timestamp,received_at,data_type,is_offline,batch_id FROM timestamped_data WHERE device_id=$1 ORDER BY data_timestamp",device_id)
   update_status(status_file,{"status":"in_progress","message":f"Found {len(device_data)} records for device {device_id}"})
  else:
   device_data=await conn.fetch("SELECT id,device_id,payload,received_at FROM device_data ORDER BY device_id,received_at")
   latest_state=await conn.fetch("SELECT device_id,payload,received_at FROM latest_device_states ORDER BY device_id")
   timestamped_data=await conn.fetch("SELECT id,device_id,payload,data_timestamp,received_at,data_type,is_offline,batch_id FROM timestamped_data ORDER BY device_id,data_timestamp")
   update_status(status_file,{"status":"in_progress","message":f"Found {len(device_data)} total records across all devices"})
  data["data"]["device_data"]=[dict(row) for row in device_data]
  data["data"]["latest_device_states"]=[dict(row) for row in latest_state] if not isinstance(latest_state,list) else [dict(row) for row in latest_state]
  data["data"]["timestamped_data"]=[dict(row) for row in timestamped_data]
  update_status(status_file,{"status":"in_progress","message":"Formatting data"})
  for table in data["data"]:
   for row in data["data"][table]:
    for k,v in row.items():
     if isinstance(v,datetime.datetime):row[k]=v.isoformat()
  update_status(status_file,{"status":"in_progress","message":"Writing to file"})
  with open(export_file,"w") as f:json.dump(data,f)
  update_status(status_file,{"status":"completed","message":"Export completed successfully","records_exported":len(device_data),"completion_time":datetime.datetime.now().isoformat()})
 except Exception as e:
  print(f"[{datetime.datetime.now()}] Export task failed: {str(e)}")
  update_status(status_file,{"status":"failed","error":str(e)})
 finally:
  if conn:await conn.close()
async def perform_import(import_file:str,status_file:str,merge:bool=True):
 conn=None
 try:
  update_status(status_file,{"status":"in_progress","message":"Reading import file"})
  with open(import_file,"r") as f:data=json.load(f)
  device_data=data["data"]["device_data"]
  latest_states=data["data"]["latest_device_states"]
  timestamped_data=data["data"]["timestamped_data"]
  total_records=len(device_data)+len(latest_states)+len(timestamped_data)
  update_status(status_file,{"status":"in_progress","message":f"Processing {total_records} records","total_records":total_records,"progress":0})
  conn=await asyncpg.connect(**DB_CONFIG)
  processed=0
  if not merge:
   update_status(status_file,{"status":"in_progress","message":"Clearing existing data"})
   await conn.execute("TRUNCATE device_data, latest_device_states, timestamped_data")
  update_status(status_file,{"status":"in_progress","message":"Importing device_data"})
  for row in device_data:
   try:
    if merge:
     exists=await conn.fetchval("SELECT EXISTS(SELECT 1 FROM device_data WHERE id=$1)",row["id"])
     if not exists:
      received_at=datetime.datetime.fromisoformat(row["received_at"]) if isinstance(row["received_at"],str) else row["received_at"]
      await conn.execute("INSERT INTO device_data(id,device_id,payload,received_at)VALUES($1,$2,$3,$4)",row["id"],row["device_id"],row["payload"],received_at)
    else:
     received_at=datetime.datetime.fromisoformat(row["received_at"]) if isinstance(row["received_at"],str) else row["received_at"]
     await conn.execute("INSERT INTO device_data(id,device_id,payload,received_at)VALUES($1,$2,$3,$4)",row["id"],row["device_id"],row["payload"],received_at)
   except Exception as e:print(f"[{datetime.datetime.now()}] Error importing device_data row: {str(e)}")
   processed+=1
   if processed%100==0:
    progress=int(processed/total_records*100)
    update_status(status_file,{"status":"in_progress","message":f"Imported {processed}/{total_records} records","progress":progress})
  update_status(status_file,{"status":"in_progress","message":"Importing latest_device_states"})
  for row in latest_states:
   try:
    received_at=datetime.datetime.fromisoformat(row["received_at"]) if isinstance(row["received_at"],str) else row["received_at"]
    if merge:
     await conn.execute("INSERT INTO latest_device_states(device_id,payload,received_at)VALUES($1,$2,$3)ON CONFLICT(device_id)DO UPDATE SET payload=CASE WHEN latest_device_states.received_at<$3 THEN $2 ELSE latest_device_states.payload END,received_at=GREATEST(latest_device_states.received_at,$3)",row["device_id"],row["payload"],received_at)
    else:await conn.execute("INSERT INTO latest_device_states(device_id,payload,received_at)VALUES($1,$2,$3)",row["device_id"],row["payload"],received_at)
   except Exception as e:print(f"[{datetime.datetime.now()}] Error importing latest_device_states row: {str(e)}")
   processed+=1
   if processed%100==0:
    progress=int(processed/total_records*100)
    update_status(status_file,{"status":"in_progress","message":f"Imported {processed}/{total_records} records","progress":progress})
  update_status(status_file,{"status":"in_progress","message":"Importing timestamped_data"})
  for row in timestamped_data:
   try:
    if merge:
     exists=await conn.fetchval("SELECT EXISTS(SELECT 1 FROM timestamped_data WHERE id=$1)",row["id"])
     if not exists:
      data_timestamp=datetime.datetime.fromisoformat(row["data_timestamp"]) if isinstance(row["data_timestamp"],str) else row["data_timestamp"]
      received_at=datetime.datetime.fromisoformat(row["received_at"]) if isinstance(row["received_at"],str) else row["received_at"]
      await conn.execute("INSERT INTO timestamped_data(id,device_id,payload,data_timestamp,received_at,data_type,is_offline,batch_id)VALUES($1,$2,$3,$4,$5,$6,$7,$8)",row["id"],row["device_id"],row["payload"],data_timestamp,received_at,row.get("data_type","telemetry"),row.get("is_offline",False),row.get("batch_id"))
    else:
     data_timestamp=datetime.datetime.fromisoformat(row["data_timestamp"]) if isinstance(row["data_timestamp"],str) else row["data_timestamp"]
     received_at=datetime.datetime.fromisoformat(row["received_at"]) if isinstance(row["received_at"],str) else row["received_at"]
     await conn.execute("INSERT INTO timestamped_data(id,device_id,payload,data_timestamp,received_at,data_type,is_offline,batch_id)VALUES($1,$2,$3,$4,$5,$6,$7,$8)",row["id"],row["device_id"],row["payload"],data_timestamp,received_at,row.get("data_type","telemetry"),row.get("is_offline",False),row.get("batch_id"))
   except Exception as e:print(f"[{datetime.datetime.now()}] Error importing timestamped_data row: {str(e)}")
   processed+=1
   if processed%100==0:
    progress=int(processed/total_records*100)
    update_status(status_file,{"status":"in_progress","message":f"Imported {processed}/{total_records} records","progress":progress})
  update_status(status_file,{"status":"completed","message":f"Import completed. {processed} records imported.","records_imported":processed,"completion_time":datetime.datetime.now().isoformat(),"progress":100})
 except Exception as e:
  print(f"[{datetime.datetime.now()}] Import task failed: {str(e)}")
  update_status(status_file,{"status":"failed","error":str(e)})
 finally:
  if conn:await conn.close()
def update_status(status_file:str,update:dict):
 try:
  current={}
  if os.path.exists(status_file):
   with open(status_file,"r") as f:current=json.load(f)
  current.update(update)
  current["last_updated"]=datetime.datetime.now().isoformat()
  with open(status_file,"w") as f:json.dump(current,f)
 except Exception as e:print(f"[{datetime.datetime.now()}] Error updating status: {str(e)}")