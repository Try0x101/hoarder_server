#!/bin/bash
cd /root/hoarder_server
# Уменьшаем количество воркеров до 2 для избежания конфликтов при инициализации БД
uvicorn app.main:app --host 0.0.0.0 --port 5000 --workers 2