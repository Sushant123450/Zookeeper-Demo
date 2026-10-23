@echo off
echo Shutting down ZooKeeper ensemble...

taskkill /F /FI "WINDOWTITLE eq ZooKeeper 21*" /T > NUL 2>&1
taskkill /F /FI "WINDOWTITLE eq Client*" /T > NUL 2>&1

echo Cluster successfully closed!
timeout /t 2 > NUL
