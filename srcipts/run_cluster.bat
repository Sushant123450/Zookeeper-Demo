@echo off
setlocal enabledelayedexpansion

cd C:\Users\susha\Desktop\ZKeeper\build\Debug

set /p NUM_INSTANCES="How many ZooKeeper servers do you want to start? "
set /A ZK_END_PORT=2180 + %NUM_INSTANCES%

echo.
echo ==============================
echo Starting ZooKeeper Ensemble
echo ==============================

for /L %%P in (2181,1,%ZK_END_PORT%) do (
    set "ZK_PEERS="

    for /L %%Q in (2181,1,%ZK_END_PORT%) do (
        if not %%P==%%Q (
            set "ZK_PEERS=!ZK_PEERS! localhost:%%Q"
        )
    )

    echo Starting ZooKeeper %%P with peers: !ZK_PEERS!
    start "ZooKeeper %%P" cmd /k "zookeeper.exe %%P !ZK_PEERS!"
)

echo.
echo Waiting for ZooKeeper ensemble to initialize...
timeout /t 3 /nobreak > NUL

set "BOOTSTRAP_ARGS="
for /L %%P in (2181,1,%ZK_END_PORT%) do (
    set "BOOTSTRAP_ARGS=!BOOTSTRAP_ARGS! localhost:%%P"
)

echo.
echo ==============================
echo Starting Client
echo ==============================
echo Bootstrapping client with:%BOOTSTRAP_ARGS%

start "Client" cmd /k "client.exe %BOOTSTRAP_ARGS%"

echo.
echo ==============================
echo Ensemble is running!
echo ==============================
