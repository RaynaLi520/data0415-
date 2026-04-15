@echo off
chcp 65001 >nul
echo ========================================
echo   FAAM 自动化系统 - 立即执行
echo ========================================
echo.

set SCRIPT_DIR=%~dp0
set SCHEDULER_SCRIPT=%SCRIPT_DIR%auto_scheduler.py

echo 正在执行自动化任务...
echo 时间: %date% %time%
echo.

python "%SCHEDULER_SCRIPT%"

if errorlevel 1 (
    echo.
    echo [失败] 任务执行失败,请查看日志文件
    echo 日志位置: %SCRIPT_DIR%logs\
) else (
    echo.
    echo [成功] 任务执行完成!
)

echo.
pause
