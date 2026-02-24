@echo off
REM ============================================================
REM run_etl.bat - ETL Pipeline Runner (Windows)
REM Perusahaan XYZ
REM ============================================================
setlocal

REM Auto-detect project directory (where this .bat lives)
set PROJECT_DIR=%~dp0
set VENV_DIR=%PROJECT_DIR%venv
set LOG_DIR=%PROJECT_DIR%logs

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Activate venv if exists
cd /d "%PROJECT_DIR%"
if exist "%VENV_DIR%\Scripts\activate.bat" (
    call "%VENV_DIR%\Scripts\activate.bat"
)

REM Run ETL Pipeline
echo [%date% %time%] ETL Pipeline Started >> "%LOG_DIR%\etl_latest.log"
python main.py >> "%LOG_DIR%\etl_latest.log" 2>&1
set EXIT_CODE=%ERRORLEVEL%

if %EXIT_CODE% EQU 0 (
    echo [%date% %time%] ETL Pipeline SUCCESS >> "%LOG_DIR%\etl_latest.log"
) else (
    echo [%date% %time%] ETL Pipeline FAILED >> "%LOG_DIR%\etl_latest.log"
)

call deactivate 2>nul
exit /b %EXIT_CODE%