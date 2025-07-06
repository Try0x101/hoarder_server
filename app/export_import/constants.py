import os

EXPORT_DIR = "/tmp/hoarder_exports"
IMPORT_DIR = "/tmp/hoarder_imports"

def ensure_dirs():
    os.makedirs(EXPORT_DIR, exist_ok=True)
    os.makedirs(IMPORT_DIR, exist_ok=True)

ensure_dirs()
