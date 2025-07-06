import re
from typing import Dict, Any

def normalize_coordinate_value(coord_val):
    if coord_val is None:
        return None
    
    if isinstance(coord_val, (int, float)):
        return float(coord_val)
    
    str_val = str(coord_val).strip()
    if not str_val or str_val.lower() in ['null', 'none', 'undefined', 'n/a', '']:
        return None
    
    str_val = re.sub(r'[^\d\-\+\.]', '', str_val)
    
    try:
        return float(str_val)
    except (ValueError, TypeError):
        return None

def validate_coordinates(lat, lon):
    lat_float = normalize_coordinate_value(lat)
    lon_float = normalize_coordinate_value(lon)
    
    if lat_float is None and lon_float is None:
        return True, "No coordinates provided"
    
    if lat_float is None or lon_float is None:
        return False, "Incomplete coordinates (missing lat or lon)"

    if not (-90 <= lat_float <= 90):
        return False, f"Latitude {lat_float} out of range (-90 to 90)"

    if not (-180 <= lon_float <= 180):
        return False, f"Longitude {lon_float} out of range (-180 to 180)"

    return True, "Valid coordinates"

def validate_device_data(data: dict):
    errors = []
    warnings = []

    if not isinstance(data, dict):
        errors.append("Data must be a JSON object")
        return {'is_valid': False, 'errors': errors, 'warnings': warnings}

    device_id = data.get('id') or data.get('device_id')
    if not device_id:
        warnings.append("Missing device identifier (id or device_id) - will generate automatic ID")

    lat = data.get('lat')
    lon = data.get('lon')
    if lat is not None or lon is not None:
        is_valid, message = validate_coordinates(lat, lon)
        if not is_valid:
            warnings.append(f"Coordinate validation: {message}")

    numeric_fields = {
        'perc': (0, 100, "Battery percentage"),
        'cap': (0, 50000, "Battery capacity (mAh)"),
        'rssi': (-150, 0, "Signal strength (dBm)"),
        'acc': (0, 10000, "GPS accuracy (m)"),
        'spd': (0, 500, "Speed (km/h)"),
        'alt': (-1000, 10000, "Altitude (m)")
    }
    
    for field, (min_val, max_val, desc) in numeric_fields.items():
        if field in data:
            try:
                val = float(data[field])
                if not (min_val <= val <= max_val):
                    warnings.append(f"{desc} value {val} outside expected range ({min_val}-{max_val})")
            except (ValueError, TypeError):
                warnings.append(f"Invalid {desc} format: {data[field]}")

    return {
        'is_valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings
    }
