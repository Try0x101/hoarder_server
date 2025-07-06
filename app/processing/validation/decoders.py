import zlib
import gzip
import json
import io
import asyncio

async def decode_raw_data(raw: bytes) -> dict:
    if not raw:
        return {"error": "Empty payload"}
    
    try:
        decompressed_data = await asyncio.to_thread(zlib.decompress, raw, wbits=-15)
        return await asyncio.to_thread(json.loads, decompressed_data)
    except (zlib.error, json.JSONDecodeError):
        pass

    try:
        def decompress_and_load_gzip():
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as f:
                decompressed_data = f.read()
            return json.loads(decompressed_data)
        return await asyncio.to_thread(decompress_and_load_gzip)
    except (OSError, json.JSONDecodeError):
        pass

    try:
        return await asyncio.to_thread(json.loads, raw)
    except json.JSONDecodeError:
        pass
    
    try:
        text_data = raw.decode('utf-8', errors='ignore').strip()
        if text_data.startswith('{') and text_data.endswith('}'):
            return await asyncio.to_thread(json.loads, text_data)
    except Exception:
        pass

    return {"error": "Failed to decode", "raw_size": len(raw), "raw_preview": raw[:100].hex()}
