@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo ========================================
echo   FAAM 自动化任务安装程序
echo   设置每天北京时间18:30自动执行
echo ========================================
echo.

REM 获取当前目录
set SCRIPT_DIR=%~dp0
set SCHEDULER_SCRIPT=%SCRIPT_DIR%auto_scheduler.py
set PYTHON_PATH=python

echo [1/4] 检查Python环境...
%PYTHON_PATH% --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未找到Python,请确保Python已安装并添加到PATH
    pause
    exit /b 1
)
echo [成功] Python环境正常
echo.

echo [2/4] 检查必要的脚本文件...
if not exist "%SCHEDULER_SCRIPT%" (
    echo [错误] 找不到 auto_scheduler.py
    pause
    exit /b 1
)
echo [成功] 脚本文件存在
echo.

echo [3/4] 删除旧的任务计划(如果存在)...
schtasks /delete /tn "FAAM_AutoCrawler" /f >nul 2>&1
echo [完成] 清理旧任务
echo.

echo [4/4] 创建新的任务计划...
echo 任务名称: FAAM_AutoCrawler
echo 执行时间: 每天 18:30 (北京时间)
echo 执行脚本: %SCHEDULER_SCRIPT%
echo.

REM 创建任务计划
schtasks /create ^
    /tn "FAAM_AutoCrawler" ^
    /tr "\"%PYTHON_PATH%\" \"%SCHEDULER_SCRIPT%\"" ^
    /sc daily ^
    /st 18:30 ^
    /rl highest ^
    /ru SYSTEM ^
    /f

if errorlevel 1 (
    echo.
    echo [失败] 创建任务计划失败
    echo 请以管理员身份运行此脚本
    pause
    exit /b 1
)

echo.
echo [成功] 任务计划创建成功!
echo.
echo ========================================
echo   安装完成!
echo ========================================
echo.
echo 任务详情:
echo   - 任务名称: FAAM_AutoCrawler
echo   - 执行频率: 每天一次
echo   - 执行时间: 18:30 (北京时间)
echo   - 日志位置: %SCRIPT_DIR%logs\
echo.
echo 管理命令:
echo   查看任务: schtasks /query /tn "FAAM_AutoCrawler"
echo   禁用任务: schtasks /change /tn "FAAM_AutoCrawler" /disable
echo   启用任务: schtasks /change /tn "FAAM_AutoCrawler" /enable
echo   删除任务: schtasks /delete /tn "FAAM_AutoCrawler" /f
echo   手动运行: schtasks /run /tn "FAAM_AutoCrawler"
echo.
echo ========================================

pause
