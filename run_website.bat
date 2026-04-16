@echo off
chcp 65001 >nul
cd /d "%~dp0"
set PYTHONPATH=%CD%
echo Starting website from: %CD%
echo Open in browser: http://localhost:8051
echo Press Ctrl+C to stop the server.
py -m streamlit run website/app.py --server.port 8051
pause
