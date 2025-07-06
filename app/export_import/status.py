import os
import json
import datetime

def update_status(status_file: str, update: dict):
    try:
        current = {}
        if os.path.exists(status_file):
            with open(status_file, "r") as f:
                current = json.load(f)
        current.update(update)
        current["last_updated"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with open(status_file, "w") as f:
            json.dump(current, f)
    except Exception:
        pass
