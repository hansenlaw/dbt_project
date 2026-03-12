@echo off
setlocal

cd /d "C:\Users\hanse\coursera\advance_analytic_engineer\io_testing"

REM Create a logs folder if it doesn't exist
if not exist "logs" mkdir "logs"

REM Build a timestamped log file name: logs\dbt_build_YYYYMMDD_HHMMSS.log
for /f "tokens=1-4 delims=/ " %%a in ('date /t') do set TODAY=%%d%%b%%c
for /f "tokens=1-2 delims=: " %%a in ('time /t') do set NOW=%%a%%b
set LOGFILE=logs\dbt_build_%TODAY%_%NOW%.log

echo Writing output to %LOGFILE%
echo ============================================ > "%LOGFILE%"
echo   dbt build run - %DATE% %TIME%            >> "%LOGFILE%"
echo ============================================ >> "%LOGFILE%"
echo.                                            >> "%LOGFILE%"

dbt build >> "%LOGFILE%" 2>&1

echo.
echo ============================================
echo   dbt build finished.
echo   Log file: %LOGFILE%
echo   Exit code: %ERRORLEVEL%
echo ============================================
echo.

pause
endlocal
