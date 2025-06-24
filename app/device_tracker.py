"""
Файл: app/device_tracker.py (новый файл)
Система отслеживания перемещений устройств для оптимизации запросов погоды
"""
import json
import os
import datetime
import math
from typing import Optional, Dict, Tuple

DEVICE_POSITIONS_FILE = "/tmp/device_positions.json"
MOVEMENT_THRESHOLD_KM = 1.0  # Порог для принудительного обновления погоды

def calculate_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Вычисляет расстояние между двумя точками в километрах (формула Haversine)"""
    R = 6371.0  # Радиус Земли в км
    
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

def load_device_positions() -> Dict[str, Dict]:
    """Загружает последние позиции устройств из файла"""
    try:
        if os.path.exists(DEVICE_POSITIONS_FILE):
            with open(DEVICE_POSITIONS_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"[{datetime.datetime.now()}] DEBUG: Error loading device positions: {e}")
    
    return {}

def save_device_positions(positions: Dict[str, Dict]):
    """Сохраняет позиции устройств в файл"""
    try:
        with open(DEVICE_POSITIONS_FILE, 'w') as f:
            json.dump(positions, f, indent=2)
    except Exception as e:
        print(f"[{datetime.datetime.now()}] DEBUG: Error saving device positions: {e}")

def should_force_weather_update(device_id: str, current_lat: float, current_lon: float) -> Tuple[bool, str]:
    """
    Определяет, нужно ли принудительно обновить погоду для устройства
    
    Returns:
        (bool, str): (нужно_ли_обновить, причина)
    """
    positions = load_device_positions()
    
    device_key = str(device_id)
    
    # Если устройства нет в записях - это первый запрос
    if device_key not in positions:
        print(f"[{datetime.datetime.now()}] DEBUG: Device {device_id} - first weather request")
        
        # Сохраняем текущую позицию
        positions[device_key] = {
            'lat': current_lat,
            'lon': current_lon,
            'last_weather_update': datetime.datetime.now().isoformat(),
            'weather_update_count': 1
        }
        save_device_positions(positions)
        
        return True, "first_request"
    
    # Получаем последнюю позицию устройства
    last_position = positions[device_key]
    last_lat = last_position.get('lat')
    last_lon = last_position.get('lon')
    last_update = last_position.get('last_weather_update')
    
    if last_lat is None or last_lon is None:
        print(f"[{datetime.datetime.now()}] DEBUG: Device {device_id} - invalid last position")
        
        # Обновляем позицию
        positions[device_key].update({
            'lat': current_lat,
            'lon': current_lon,
            'last_weather_update': datetime.datetime.now().isoformat(),
            'weather_update_count': positions[device_key].get('weather_update_count', 0) + 1
        })
        save_device_positions(positions)
        
        return True, "invalid_last_position"
    
    # Вычисляем расстояние от последней позиции с обновлением погоды
    distance = calculate_distance_km(current_lat, current_lon, last_lat, last_lon)
    
    print(f"[{datetime.datetime.now()}] DEBUG: Device {device_id} - distance from last weather update: {distance:.2f}km")
    
    # Проверяем, превышен ли порог расстояния
    if distance >= MOVEMENT_THRESHOLD_KM:
        print(f"[{datetime.datetime.now()}] DEBUG: Device {device_id} - significant movement detected ({distance:.2f}km)")
        
        # Обновляем позицию
        positions[device_key].update({
            'lat': current_lat,
            'lon': current_lon,
            'last_weather_update': datetime.datetime.now().isoformat(),
            'weather_update_count': positions[device_key].get('weather_update_count', 0) + 1
        })
        save_device_positions(positions)
        
        return True, f"moved_{distance:.2f}km"
    
    # Проверяем время последнего обновления (принудительное обновление раз в час)
    if last_update:
        try:
            last_update_time = datetime.datetime.fromisoformat(last_update)
            time_since_update = datetime.datetime.now() - last_update_time
            
            if time_since_update.total_seconds() > 3600:  # 1 час
                print(f"[{datetime.datetime.now()}] DEBUG: Device {device_id} - weather data expired ({time_since_update})")
                
                # Обновляем время (координаты могут не измениться)
                positions[device_key].update({
                    'lat': current_lat,
                    'lon': current_lon,
                    'last_weather_update': datetime.datetime.now().isoformat(),
                    'weather_update_count': positions[device_key].get('weather_update_count', 0) + 1
                })
                save_device_positions(positions)
                
                return True, f"expired_{int(time_since_update.total_seconds())}s"
        except Exception as e:
            print(f"[{datetime.datetime.now()}] DEBUG: Error parsing last update time: {e}")
    
    # Обновляем текущую позицию (но не время обновления погоды)
    positions[device_key].update({
        'current_lat': current_lat,
        'current_lon': current_lon,
        'last_seen': datetime.datetime.now().isoformat()
    })
    save_device_positions(positions)
    
    print(f"[{datetime.datetime.now()}] DEBUG: Device {device_id} - using cached weather (distance: {distance:.2f}km)")
    return False, f"cached_distance_{distance:.2f}km"

def get_device_stats() -> Dict[str, any]:
    """Возвращает статистику по устройствам"""
    positions = load_device_positions()
    
    stats = {
        'total_devices': len(positions),
        'devices': {}
    }
    
    for device_id, position in positions.items():
        stats['devices'][device_id] = {
            'last_lat': position.get('lat'),
            'last_lon': position.get('lon'),
            'current_lat': position.get('current_lat'),
            'current_lon': position.get('current_lon'),
            'last_weather_update': position.get('last_weather_update'),
            'weather_update_count': position.get('weather_update_count', 0),
            'last_seen': position.get('last_seen')
        }
    
    return stats

def cleanup_old_device_data(days_threshold: int = 7):
    """Очищает данные устройств, которые не были активны более N дней"""
    try:
        positions = load_device_positions()
        cutoff_time = datetime.datetime.now() - datetime.timedelta(days=days_threshold)
        
        devices_to_remove = []
        
        for device_id, position in positions.items():
            last_seen = position.get('last_seen') or position.get('last_weather_update')
            
            if last_seen:
                try:
                    last_seen_time = datetime.datetime.fromisoformat(last_seen)
                    if last_seen_time < cutoff_time:
                        devices_to_remove.append(device_id)
                except:
                    # Если не можем распарсить время - удаляем устройство
                    devices_to_remove.append(device_id)
            else:
                # Если нет данных о времени - удаляем устройство
                devices_to_remove.append(device_id)
        
        for device_id in devices_to_remove:
            del positions[device_id]
            print(f"[{datetime.datetime.now()}] DEBUG: Removed old device data for {device_id}")
        
        if devices_to_remove:
            save_device_positions(positions)
            print(f"[{datetime.datetime.now()}] DEBUG: Cleaned up {len(devices_to_remove)} old devices")
        
    except Exception as e:
        print(f"[{datetime.datetime.now()}] DEBUG: Error during cleanup: {e}")
