@echo off
title Vodafone Idea - Auto-Session Linker
echo ========================================================
echo   1. Starting Edge with Remote Debugging (Port 9222)...
echo ========================================================
taskkill /F /IM msedge.exe 2>nul
ping 127.0.0.1 -n 2 >nul
start "" "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" --remote-debugging-port=9222 --user-data-dir="H:\goodness\edge_profile" "https://cpos3.vodafoneidea.com/cPOSWeb"

echo.
echo ========================================================
echo   2. Starting Vodafone Ghost Background Engine...
echo      (Waiting for you to log in in the browser)
echo ========================================================
echo.
cd /d "H:\goodness"
.\target\release\vodafone_ghost.exe
pause
