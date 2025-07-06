import asyncio
from typing import Optional, List, Dict

_last_cleanup: float = 0
_last_disk_check: float = 0
_cleanup_lock = asyncio.Lock()
_emergency_mode: bool = False
_disk_stats: Dict[str, float] = {'total_mb': 0, 'available_mb': 0, 'used_mb': 0}

_file_list_cache: Optional[List[str]] = None
_file_list_cache_time: float = 0
_file_list_lock = asyncio.Lock()