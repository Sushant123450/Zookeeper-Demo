@echo off
echo Compiling ZKeeper...

:: The /d flag ensures it switches to the C: drive even if you run this from a USB or another drive
cd /d C:\Users\susha\Desktop\ZKeeper\build

:: Run the build command
cmake --build .

echo.
echo Build complete!
