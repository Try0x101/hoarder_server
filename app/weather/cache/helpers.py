import os
import hashlib
from typing import Tuple
from .constants import CACHE_DIR

def round_coordinates(lat: float, lon: float, precision: int = 3) -> Tuple[float, float]:
    return round(lat, precision), round(lon, precision)

def ensure_cache_dir():
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR, exist_ok=True)

def get_cache_key(lat: float, lon: float) -> str:
    rounded_lat, rounded_lon = round_coordinates(lat, lon)
    return hashlib.md5(f"{rounded_lat}_{rounded_lon}".encode()).hexdigest()