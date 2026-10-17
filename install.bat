@echo off
setlocal
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1
if exist ".venv\Scripts\python.exe" goto install
where py >nul 2>nul
if errorlevel 1 goto try_python
py -3 -m venv .venv
goto install
:try_python
where python >nul 2>nul
if errorlevel 1 goto no_python
python -m venv .venv
:install
".venv\Scripts\python.exe" -m pip install --upgrade pip
".venv\Scripts\python.exe" -m pip install -r requirements-live.txt
if errorlevel 1 goto failed
echo Install complete.
pause
exit /b 0
:no_python
echo Python 3 was not found.
pause
exit /b 1
:failed
echo Install failed. Review the output above.
pause
exit /b 1

