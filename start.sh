#!/bin/bash
cd /root/hoarder_server
source venv/bin/activate
uvicorn app.main:socket_app --host 0.0.0.0 --port 5000 --workers 1
