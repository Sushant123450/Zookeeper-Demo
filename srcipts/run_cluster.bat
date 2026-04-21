@echo off
cd C:\Users\susha\Desktop\ZKeeper\build\Debug

set /p NUM_INSTANCES="How many nodes do you want to start? "

set /A END_PORT=5000 + %NUM_INSTANCES%

echo.
echo Starting %NUM_INSTANCES% Nodes...

FOR /L %%P IN (5001,1,%END_PORT%) DO (
    setlocal enabledelayedexpansion

        )
    )

    start "Node %%P" cmd /k "node.exe %%P !PEERS!"

    endlocal
)
    start "Client %%P" cmd /k "client.exe localhost:%%P"
)

echo Cluster is running!