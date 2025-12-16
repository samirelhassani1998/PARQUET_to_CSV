@echo off
REM Build script for Parquet to CSV Windows executable
REM Alternative to build_windows.ps1 for users without PowerShell
REM
REM Output: dist\ParquetToCSV\ParquetToCSV.exe

setlocal enabledelayedexpansion

echo ==============================================================
echo   Parquet -^> CSV : Build Windows
echo ==============================================================
echo.

REM Get project root (parent of scripts folder)
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "PROJECT_ROOT=%CD%"
echo Project root: %PROJECT_ROOT%

REM Check Python
where python >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Python not found in PATH
    pause
    exit /b 1
)

REM Create venv if needed
set "VENV_PATH=%PROJECT_ROOT%\build_venv"
set "VENV_PYTHON=%VENV_PATH%\Scripts\python.exe"

if not exist "%VENV_PYTHON%" (
    echo.
    echo Creating virtual environment...
    python -m venv "%VENV_PATH%"
    if %ERRORLEVEL% neq 0 (
        echo ERROR: Failed to create venv
        pause
        exit /b 1
    )
)

echo.
echo Installing dependencies...
"%VENV_PYTHON%" -m pip install --upgrade pip
"%VENV_PYTHON%" -m pip install -r "%PROJECT_ROOT%\requirements.txt"
"%VENV_PYTHON%" -m pip install pyinstaller
if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)

echo.
echo Building with PyInstaller...
"%VENV_PYTHON%" -m PyInstaller --clean --noconfirm "%PROJECT_ROOT%\ParquetToCSV.spec"
if %ERRORLEVEL% neq 0 (
    echo ERROR: PyInstaller build failed
    pause
    exit /b 1
)

REM Verify output
set "EXE_PATH=%PROJECT_ROOT%\dist\ParquetToCSV\ParquetToCSV.exe"
if exist "%EXE_PATH%" (
    echo.
    echo ==============================================================
    echo   BUILD SUCCESSFUL
    echo ==============================================================
    echo.
    echo Executable: %EXE_PATH%
    echo.
    echo To run: .\dist\ParquetToCSV\ParquetToCSV.exe
    echo.
) else (
    echo ERROR: Build output not found
    pause
    exit /b 1
)

pause
