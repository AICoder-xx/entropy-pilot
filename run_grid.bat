@echo off
setlocal
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1
if not exist ".venv\Scripts\python.exe" goto need_install
".venv\Scripts\python.exe" main.py --config config.grid.json
pause
exit /b %errorlevel%
:need_install
echo Run install.bat first.
pause
exit /b 1

