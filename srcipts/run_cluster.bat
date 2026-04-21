@echo off
cd C:\Users\susha\Desktop\ZKeeper\build\Debug

echo Starting Nodes...
:: The cmd /k ensures the window stays open so you can see logs or crash errors
start "Node 5001" cmd /k "node.exe 5001"
start "Node 5002" cmd /k "node.exe 5002"
start "Node 5003" cmd /k "node.exe 5003"

echo Waiting for nodes to initialize...
timeout /t 2 /nobreak > NUL

echo Starting Clients...
:: The cmd /k is crucial here so you can read the "Greeter received: ..." message
start "Client 5001" cmd /k "client.exe localhost:5001"
start "Client 5002" cmd /k "client.exe localhost:5002"
start "Client 5003" cmd /k "client.exe localhost:5003"