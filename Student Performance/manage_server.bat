@echo off
setlocal enabledelayedexpansion

:: Check python/pythonw availability
where pythonw >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] pythonw was not found in your system PATH.
    echo Please make sure Python is installed and added to system variables.
    exit /b 1
)

set "STARTUP_DIR=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
set "VBS_FILE=%STARTUP_DIR%\start_student_performance_backend.vbs"
set "SCRIPT_PATH=%~dp0src\main.py"

if "%~1"=="" goto help
if "%~1"=="start" goto start_server
if "%~1"=="stop" goto stop_server
if "%~1"=="status" goto status_server
if "%~1"=="restart" goto restart_server
if "%~1"=="install-startup" goto install_startup
if "%~1"=="uninstall-startup" goto uninstall_startup
if "%~1"=="open-ui" goto open_ui

:help
echo Student Performance Analysis Server Manager
echo.
echo Usage: manage_server.bat [command]
echo.
echo Commands:
echo   start              Starts the Flask server in the background using pythonw.
echo   stop               Stops the Flask server by finding and killing the process on port 5000.
echo   status             Checks if the Flask server is running on port 5000.
echo   restart            Stops and restarts the Flask server.
echo   install-startup    Registers the server to run in background automatically on Windows startup.
echo   uninstall-startup  Deregisters the server from Windows startup.
echo   open-ui            Opens the application frontend in your default browser.
echo.
exit /b 0

:status_server
echo Checking server status on port 5000...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :5000 ^| findstr LISTENING') do (
    echo [RUNNING] Flask server is running under process ID %%a
    exit /b 0
)
echo [STOPPED] Flask server is not running on port 5000.
exit /b 1

:start_server
echo Checking if server is already running...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :5000 ^| findstr LISTENING') do (
    echo [WARNING] Flask server is already running with PID %%a
    exit /b 0
)
echo Starting Flask backend server in background...
start "" pythonw.exe "!SCRIPT_PATH!"
ping 127.0.0.1 -n 6 >nul
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :5000 ^| findstr LISTENING') do (
    echo [SUCCESS] Flask server started successfully in the background with PID %%a
    exit /b 0
)
echo [ERROR] Failed to start server in background. Try running manually: python "!SCRIPT_PATH!"
exit /b 1

:stop_server
echo Stopping Flask backend server...
set "found=0"
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :5000 ^| findstr LISTENING') do (
    taskkill /f /pid %%a >nul 2>nul
    echo [SUCCESS] Stopped Flask server with PID %%a
    set "found=1"
)
if "%found%"=="0" (
    echo [INFO] No server process was found running on port 5000.
)
exit /b 0

:restart_server
call :stop_server
ping 127.0.0.1 -n 2 >nul
call :start_server
exit /b 0

:install_startup
echo Registering Flask server for automatic startup...
(
echo Set WshShell = CreateObject^("WScript.Shell"^)
echo WshShell.CurrentDirectory = "%~dp0"
echo WshShell.Run "pythonw.exe ""!SCRIPT_PATH!""", 0, False
) > "!VBS_FILE!"
if exist "!VBS_FILE!" (
    echo [SUCCESS] Registered backend to start automatically on Windows Startup!
    echo Startup VBScript created at: !VBS_FILE!
) else (
    echo [ERROR] Failed to create Startup VBScript. Check folder permissions.
)
exit /b 0

:uninstall_startup
echo Removing Flask server from automatic startup...
if exist "!VBS_FILE!" (
    del /f /q "!VBS_FILE!"
    echo [SUCCESS] Deregistered backend from Windows Startup.
) else (
    echo [INFO] No startup script found to uninstall.
)
exit /b 0

:open_ui
echo Opening Student Performance Dashboard...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :5000 ^| findstr LISTENING') do (
    start "" "http://127.0.0.1:5000/"
    exit /b 0
)
start "" "%~dp0ui\index.html"
exit /b 0
