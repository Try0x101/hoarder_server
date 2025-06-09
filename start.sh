#!/bin/bash
<<<<<<< HEAD
cd /root/hoarder_server&&source venv/bin/activate&&python3 -m uvicorn app.main:socket_app --host 0.0.0.0 --port 5000 --workers 1
=======
cd /root/hoarder_server
# Уменьшаем количество воркеров до 2 для избежания конфликтов при инициализации БД
uvicorn app.main:app --host 0.0.0.0 --port 5000 --workers 2
>>>>>>> c0f02561b62130cdb8e6492ea11157bf7aa103f9
