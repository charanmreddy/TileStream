@echo off
REM One-time setup: creates .venv and installs dependencies
python --version || (echo Python not found. Install Python 3.10-3.12 from python.org & exit /b 1)
python -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -r requirements.txt
echo.
echo Setup complete.
