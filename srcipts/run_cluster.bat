@echo off
setlocal enabledelayedexpansion

cd C:\Users\susha\Desktop\ZKeeper\build\Debug

set /p NUM_INSTANCES="How many nodes do you want to start? "

set /A END_PORT=5000 + %NUM_INSTANCES%

echo.
echo Starting %NUM_INSTANCES% Nodes...

REM Start nodes with peer list
FOR /L %%P IN (5001,1,%END_PORT%) DO (
    set "PEERS="

    FOR /L %%Q IN (5001,1,%END_PORT%) DO (
        IF NOT %%P==%%Q (
            set "PEERS=!PEERS! localhost:%%Q"
        )
    )

    start "Node %%P" cmd /k "node.exe %%P !PEERS!"
)

echo Waiting for nodes to initialize...
timeout /t 2 /nobreak > NUL

echo Starting Clients...

REM Start clients
FOR /L %%P IN (5001,1,%END_PORT%) DO (
    start "Client %%P" cmd /k "client.exe localhost:%%P"
)

echo Cluster is running!