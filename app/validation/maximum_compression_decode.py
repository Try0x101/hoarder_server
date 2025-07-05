import struct
import lzma
import asyncio
from typing import Dict, Any, Optional

class DecompressionError(Exception):
    pass

OPERATORS_REVERSE = {
    0x01: "Verizon", 0x02: "AT&T", 0x03: "T-Mobile",
}

NETWORK_TYPES_REVERSE = {
    0x01: "LTE", 0x02: "5G NR", 0x03: "HSPA",
}

DEVICE_MODELS_REVERSE = {
    0x01: "Pixel 7", 0x02: "iPhone 14", 0x03: "Galaxy S23",
}

def decompress_lzma(compressed_data: bytes) -> bytes:
    try:
        return lzma.decompress(compressed_data)
    except lzma.LZMAError as e:
        raise DecompressionError(f"LZMA decompression failed: {e}")

def decode_binary_protocol(binary_data: bytes) -> Dict[str, Any]:
    data = {}
    offset = 0

    try:
        device_hash = struct.unpack('>H', binary_data[offset:offset+2])[0]
        data['id'] = f"dev_{device_hash:04x}"
        offset += 2

        lat_fixed = struct.unpack('>i', binary_data[offset:offset+4])[0]
        data['lat'] = f"{lat_fixed / 1000000.0:.6f}"
        offset += 4

        lon_fixed = struct.unpack('>i', binary_data[offset:offset+4])[0]
        data['lon'] = f"{lon_fixed / 1000000.0:.6f}"
        offset += 4

        data['alt'] = struct.unpack('>h', binary_data[offset:offset+2])[0]
        offset += 2

        data['perc'] = binary_data[offset]
        offset += 1

        rssi_offset = binary_data[offset]
        data['rssi'] = str(rssi_offset - 150)
        offset += 1

        data['spd'] = binary_data[offset]
        offset += 1

        data['acc'] = binary_data[offset]
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

        flags = binary_data[offset]
        offset += 1

        has_wifi = bool(flags & 0x01)
        has_battery_cap = bool(flags & 0x02)
        has_cell_id = bool(flags & 0x04)
        has_network_speeds = bool(flags & 0x08)

        data['cap'] = None
        if has_battery_cap:
            data['cap'] = struct.unpack('>H', binary_data[offset:offset+2])[0]
            offset += 2

        data['bssid'] = "0"
        if has_wifi:
            bssid_bytes = binary_data[offset:offset+6]
            data['bssid'] = ''.join(f'{b:02x}' for b in bssid_bytes)
            offset += 6

        data['ci'] = "N/A"
        if has_cell_id:
            data['ci'] = str(struct.unpack('>I', binary_data[offset:offset+4])[0])
            offset += 4
        
        data['tac'] = None
        data['mcc'] = None
        data['mnc'] = None

        data['dn'] = "0"
        data['up'] = "0"
        if has_network_speeds:
            down_speed_raw = struct.unpack('>H', binary_data[offset:offset+2])[0]
            up_speed_raw = struct.unpack('>H', binary_data[offset+2:offset+4])[0]
            data['dn'] = str(down_speed_raw / 10.0)
            data['up'] = str(up_speed_raw / 10.0)
            offset += 4

        return data

    except (struct.error, IndexError) as e:
        raise DecompressionError(f"Failed to parse binary protocol: {e}")


async def decode_maximum_compression(compressed_data: bytes) -> dict:
    try:
        def decompress_and_decode():
            binary_data = decompress_lzma(compressed_data)
            return decode_binary_protocol(binary_data)

        telemetry_dict = await asyncio.to_thread(decompress_and_decode)
        return telemetry_dict
    except DecompressionError as e:
        return {"error": "Failed to decode maximum compression", "details": str(e)}
