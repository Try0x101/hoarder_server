import struct, lzma, asyncio
from typing import Dict, Any

class DecompressionError(Exception): pass

OPERATORS_REVERSE = {0x01: "Verizon", 0x02: "AT&T", 0x03: "T-Mobile", 0x04: "Sprint"}
NETWORK_TYPES_REVERSE = {0x01: "LTE", 0x02: "5G NR", 0x03: "HSPA", 0x04: "GSM", 0x05: "CDMA"}
DEVICE_MODELS_REVERSE = {0x01: "Pixel 7", 0x02: "iPhone 14", 0x03: "Galaxy S23", 0x04: "OnePlus 11"}

def _unpack_and_advance(fmt: str, data: bytes, offset: int) -> tuple:
    size = struct.calcsize(fmt)
    if offset + size > len(data): raise DecompressionError(f"Not enough data at offset {offset}")
    return struct.unpack(fmt, data[offset:offset + size]), offset + size

def decode_binary_protocol(binary_data: bytes) -> Dict[str, Any]:
    if len(binary_data) < 18: raise DecompressionError(f"Binary data too short: {len(binary_data)} bytes")
    data, offset = {}, 0
    
    (val,), offset = _unpack_and_advance('>H', binary_data, offset); data['id'] = f"dev_{val:04x}"
    (lat_raw,), offset = _unpack_and_advance('>i', binary_data, offset)
    (lon_raw,), offset = _unpack_and_advance('>i', binary_data, offset)
    lat, lon = lat_raw / 1e6, lon_raw / 1e6
    if not (-90 <= lat <= 90 and -180 <= lon <= 180): raise DecompressionError("Invalid coordinates")
    data['lat'], data['lon'] = f"{lat:.6f}", f"{lon:.6f}"
    
    (val,), offset = _unpack_and_advance('>h', binary_data, offset); data['alt'] = val
    (val,), offset = _unpack_and_advance('>B', binary_data, offset); data['perc'] = val if 0 <= val <= 100 else None
    (val,), offset = _unpack_and_advance('>B', binary_data, offset); data['rssi'] = str(val - 150)
    (val,), offset = _unpack_and_advance('>B', binary_data, offset); data['spd'] = val
    (val,), offset = _unpack_and_advance('>B', binary_data, offset); data['acc'] = val
    (val,), offset = _unpack_and_advance('>B', binary_data, offset); data['op'] = OPERATORS_REVERSE.get(val)
    (val,), offset = _unpack_and_advance('>B', binary_data, offset); data['nt'] = NETWORK_TYPES_REVERSE.get(val)
    (val,), offset = _unpack_and_advance('>B', binary_data, offset); data['n'] = DEVICE_MODELS_REVERSE.get(val)
    
    if offset < len(binary_data):
        (flags,), offset = _unpack_and_advance('>B', binary_data, offset)
        if flags & 0x01: (val,), offset = _unpack_and_advance('>H', binary_data, offset); data['cap'] = val
        if flags & 0x02: (val,), offset = _unpack_and_advance('6s', binary_data, offset); data['bssid'] = val.hex()
        if flags & 0x04: (val,), offset = _unpack_and_advance('>I', binary_data, offset); data['ci'] = str(val)
        if flags & 0x08: (d,u), offset = _unpack_and_advance('>HH', binary_data, offset); data.update({'dn':f"{d/10.0:.1f}", 'up':f"{u/10.0:.1f}"})
    return data

async def decode_maximum_compression(compressed_data: bytes) -> dict:
    if not compressed_data: return {"error": "Empty compressed data"}
    try:
        binary_data = await asyncio.to_thread(lzma.decompress, compressed_data)
        telemetry_dict = decode_binary_protocol(binary_data)
        return telemetry_dict
    except (DecompressionError, lzma.LZMAError, struct.error) as e:
        return {"error": "Decompression failed", "details": str(e)}
    except Exception as e:
        return {"error": "Unexpected decode error", "details": str(e)}
