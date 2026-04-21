@echo off
echo Shutting down entire gRPC Cluster...

:: The * acts as a wildcard, so it catches "Node 5001", "Node 5015", etc.
taskkill /F /FI "WINDOWTITLE eq Node 50*" /T > NUL 2>&1
taskkill /F /FI "WINDOWTITLE eq Client 50*" /T > NUL 2>&1

echo Cluster successfully closed!
timeout /t 2 > NUL