@echo off
taskkill /F /IM msedge.exe 2>nul
ping 127.0.0.1 -n 2 >nul
start "" "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" --remote-debugging-port=9222 --user-data-dir="H:\goodness\edge_profile" "https://cpos3.vodafoneidea.com/cPOSWeb"
echo Edge launched with port 9222.
