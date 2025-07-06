import struct
import lzma
import asyncio
from typing import Dict, Any, Optional

class DecompressionError(Exception):
    pass

OPERATORS_REVERSE = {
    0x01: "Verizon", 0x02: "AT&T", 0x03: "T-Mobile", 0x04: "Sprint", 0x05: "Unknown"
}

NETWORK_TYPES_REVERSE = {
    0x01: "LTE", 0x02: "5G NR", 0x03: "HSPA", 0x04: "GSM", 0x05: "CDMA", 0x06: "Unknown"
}

DEVICE_MODELS_REVERSE = {
    0x01: "Pixel 7", 0x02: "iPhone 14", 0x03: "Galaxy S23", 0x04: "OnePlus 11", 0x05: "Unknown"
}

def safe_decompress_lzma(compressed_data: bytes) -> bytes:
    if not compressed_data:
        raise DecompressionError("Empty compressed data")
    
    if len(compressed_data) < 5:
        raise DecompressionError("Data too short for LZMA format")
    
    try:
        return lzma.decompress(compressed_data)
    except lzma.LZMAError as e:
        raise DecompressionError(f"LZMA decompression failed: {e}")
    except Exception as e:
        raise DecompressionError(f"Unexpected decompression error: {e}")

def safe_struct_unpack(fmt: str, data: bytes, offset: int, field_name: str = "field"):
    try:
        size = struct.calcsize(fmt)
        if offset + size > len(data):
            raise DecompressionError(f"Not enough data for {field_name}: need {size} bytes at offset {offset}, but only {len(data) - offset} available")
        return struct.unpack(fmt, data[offset:offset + size])
    except struct.error as e:
        raise DecompressionError(f"Struct unpack error for {field_name}: {e}")

def validate_coordinate_range(lat_raw: int, lon_raw: int) -> tuple:
    lat_float = lat_raw / 1000000.0
    lon_float = lon_raw / 1000000.0
    
    if not (-90.0 <= lat_float <= 90.0):
        raise DecompressionError(f"Latitude {lat_float} out of valid range (-90 to 90)")
    
    if not (-180.0 <= lon_float <= 180.0):
        raise DecompressionError(f"Longitude {lon_float} out of valid range (-180 to 180)")
    
    return lat_float, lon_float

def decode_binary_protocol(binary_data: bytes) -> Dict[str, Any]:
    if len(binary_data) < 20:
        raise DecompressionError(f"Binary data too short: {len(binary_data)} bytes (minimum 20 required)")
    
    data = {}
    offset = 0

    try:
        device_hash, = safe_struct_unpack('>H', binary_data, offset, "device_hash")
        data['id'] = f"dev_{device_hash:04x}"
        offset += 2

        lat_raw, = safe_struct_unpack('>i', binary_data, offset, "latitude")
        offset += 4
        
        lon_raw, = safe_struct_unpack('>i', binary_data, offset, "longitude")
        offset += 4
        
        lat_float, lon_float = validate_coordinate_range(lat_raw, lon_raw)
        data['lat'] = f"{lat_float:.6f}"
        data['lon'] = f"{lon_float:.6f}"

        altitude, = safe_struct_unpack('>h', binary_data, offset, "altitude")
        data['alt'] = altitude
        offset += 2

        battery_perc = binary_data[offset]
        data['perc'] = battery_perc if 0 <= battery_perc <= 100 else None
        offset += 1

        rssi_offset = binary_data[offset]
        rssi_value = rssi_offset - 150
        data['rssi'] = str(rssi_value) if -150 <= rssi_value <= 0 else None
        offset += 1

        speed = binary_data[offset]
        data['spd'] = speed if speed <= 500 else None
        offset += 1

        accuracy = binary_data[offset]
        data['acc'] = accuracy if accuracy <= 10000 else None
        offset += 1

        operator_code = binary_data[offset]
        data['op'] = OPERATORS_REVERSE.get(operator_code, "Unknown")
        offset += 1

        network_code = binary_data[offset]
        data['nt'] = NETWORK_TYPES_REVERSE.get(network_code, "Unknown")
        offset += 1

        model_code = binary_data[offset]
        data['n'] = DEVICE_MODELS_REVERSE.get(model_code, "Unknown")
        offset += 1

        if offset >= len(binary_data):
            return data
            
        flags = binary_data[offset]
        offset += 1

        has_wifi = bool(flags & 0x01)
        has_battery_cap = bool(flags & 0x02)
        has_cell_id = bool(flags & 0x04)
        has_network_speeds = bool(flags & 0x08)

        data['cap'] = None
        if has_battery_cap and offset + 2 <= len(binary_data):
            cap_val, = safe_struct_unpack('>H', binary_data, offset, "battery_capacity")
            data['cap'] = cap_val if 100 <= cap_val <= 50000 else None
            offset += 2

        data['bssid'] = "0"
        if has_wifi and offset + 6 <= len(binary_data):
            bssid_bytes = binary_data[offset:offset+6]
            bssid_str = ''.join(f'{b:02x}' for b in bssid_bytes)
            data['bssid'] = bssid_str if bssid_str != "000000000000" else "0"
            offset += 6

        data['ci'] = "N/A"
        if has_cell_id and offset + 4 <= len(binary_data):
            cell_id, = safe_struct_unpack('>I', binary_data, offset, "cell_id")
            data['ci'] = str(cell_id) if cell_id > 0 else "N/A"
            offset += 4

        data['tac'] = None
        data['mcc'] = None
        data['mnc'] = None

        data['dn'] = "0"
        data['up'] = "0"
        if has_network_speeds and offset + 4 <= len(binary_data):
            down_raw, up_raw = safe_struct_unpack('>HH', binary_data, offset, "network_speeds")
            data['dn'] = f"{down_raw / 10.0:.1f}" if down_raw <= 10000 else "0"
            data['up'] = f"{up_raw / 10.0:.1f}" if up_raw <= 5000 else "0"
            offset += 4

        return data

    except DecompressionError:
        raise
    except Exception as e:
        raise DecompressionError(f"Unexpected error parsing binary protocol: {e}")

async def decode_maximum_compression(compressed_data: bytes) -> dict:
    if not compressed_data or len(compressed_data) == 0:
        return {"error": "Empty compressed data"}
    
    try:
        def decompress_and_decode():
            binary_data = safe_decompress_lzma(compressed_data)
            return decode_binary_protocol(binary_data)

        telemetry_dict = await asyncio.to_thread(decompress_and_decode)
        
        validation_errors = []
        if not telemetry_dict.get('id'):
            validation_errors.append("Missing device ID")
        if not telemetry_dict.get('lat') or not telemetry_dict.get('lon'):
            validation_errors.append("Missing coordinates")
        
        if validation_errors:
            telemetry_dict['validation_warnings'] = validation_errors
            
        return telemetry_dict
        
    except DecompressionError as e:
        return {"error": "Decompression failed", "details": str(e)}
    except Exception as e:
        return {"error": "Unexpected decode error", "details": str(e)}
