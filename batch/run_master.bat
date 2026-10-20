@echo off
.venv\Scripts\python.exe -m uvicorn master.app:app --host 0.0.0.0 --port 8000
