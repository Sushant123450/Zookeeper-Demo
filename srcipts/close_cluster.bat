@echo off
echo Shutting down gRPC Cluster...

:: The /FI flag filters by the exact Window Titles we set in your run script
:: The /T flag (Tree) kills both the cmd.exe window AND the C++ .exe running inside it
:: "> NUL 2>&1" hides the messy output text from the terminal

taskkill /F /FI "WINDOWTITLE eq Node 5001*" /T > NUL 2>&1
taskkill /F /FI "WINDOWTITLE eq Node 5002*" /T > NUL 2>&1
taskkill /F /FI "WINDOWTITLE eq Node 5003*" /T > NUL 2>&1

taskkill /F /FI "WINDOWTITLE eq Client 5001*" /T > NUL 2>&1
taskkill /F /FI "WINDOWTITLE eq Client 5002*" /T > NUL 2>&1
taskkill /F /FI "WINDOWTITLE eq Client 5003*" /T > NUL 2>&1

echo Cluster successfully closed!
:: Pause keeps this window open for a second so you can confirm it worked
timeout /t 2 > NUL