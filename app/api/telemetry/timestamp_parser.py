import datetime

def parse_device_timestamp(data: dict) -> datetime.datetime:
    data_timestamp = datetime.datetime.now(datetime.timezone.utc)
    
    timestamp_fields = ['timestamp', 'ts', 'time', 'datetime']
    
    for field in timestamp_fields:
        if field in data and data[field]:
            try:
                ts_value = data[field]
                
                if isinstance(ts_value, (int, float)):
                    if ts_value > 1000000000000:
                        ts_value = ts_value / 1000
                    data_timestamp = datetime.datetime.fromtimestamp(ts_value, tz=datetime.timezone.utc)
                    break
                
                if isinstance(ts_value, str):
                    ts_value = ts_value.strip()
                    
                    if ts_value.replace('.', '').isdigit():
                        unix_ts = float(ts_value)
                        if unix_ts > 1000000000000:
                            unix_ts = unix_ts / 1000
                        data_timestamp = datetime.datetime.fromtimestamp(unix_ts, tz=datetime.timezone.utc)
                        break
                    
                    dt = datetime.datetime.fromisoformat(ts_value.replace('Z', '+00:00'))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=datetime.timezone.utc)
                    data_timestamp = dt
                    break
                    
            except (ValueError, TypeError, OSError):
                continue
    
    return data_timestamp
