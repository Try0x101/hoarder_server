import re
from typing import Dict, Any, Optional, Tuple

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

def robust_coordinate_validation(lat, lon) -> Tuple[Optional[float], Optional[float], str]:
    def normalize_coordinate(coord_val, coord_type="coordinate"):
        if coord_val is None:
            return None, f"Missing {coord_type}"
        if isinstance(coord_val, (int, float)):
            if coord_val != coord_val:
                return None, f"NaN {coord_type}"
            if abs(coord_val) == float('inf'):
                return None, f"Infinite {coord_type}"
            return float(coord_val), "valid"
        str_val = str(coord_val).strip()
        if not str_val or str_val.lower() in ['null', 'none', 'undefined', 'n/a', '', '0.0', '0']:
            return None, f"Empty/null {coord_type}"
        str_val = re.sub(r'[^\d\-\+\.]', '', str_val)
        if not str_val or str_val in ['-', '+', '.']:
            return None, f"Invalid {coord_type} format"
        try:
            val = float(str_val)
            if val != val or abs(val) == float('inf'):
                return None, f"Invalid {coord_type} value"
            return val, "valid"
        except (ValueError, TypeError, OverflowError):
            return None, f"Parse error for {coord_type}"
    
    lat_val, lat_msg = normalize_coordinate(lat, "latitude")
    lon_val, lon_msg = normalize_coordinate(lon, "longitude")
    if lat_val is None and lon_val is None:
        return None, None, "No coordinates provided"
    if lat_val is None:
        return None, None, lat_msg
    if lon_val is None:
        return None, None, lon_msg
    if not (-90 <= lat_val <= 90):
        return None, None, f"Latitude {lat_val} out of range (-90 to 90)"
    if not (-180 <= lon_val <= 180):
        return None, None, f"Longitude {lon_val} out of range (-180 to 180)"
    if lat_val == 0.0 and lon_val == 0.0:
        return None, None, "Null Island coordinates (0,0) not valid"
    return lat_val, lon_val, "valid"

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
    return {'is_valid': len(errors) == 0, 'errors': errors, 'warnings': warnings}
