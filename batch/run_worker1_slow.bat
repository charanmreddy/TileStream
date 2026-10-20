@echo off
REM Slow mode (400 ms extra per tile) - for demos and the crash-recovery test
.venv\Scripts\python.exe -m worker.worker --id worker-1 --delay-ms 400
