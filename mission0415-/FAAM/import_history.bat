@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo ========================================
echo   FAAM 历史数据批量导入工具
echo ========================================
echo.

set SCRIPT_DIR=%~dp0

echo 请选择导入方式:
echo.
echo 1. 导入当前目录及子目录下的所有Excel文件
echo 2. 指定目录导入
echo.
set /p choice="请输入选项 (1 或 2, 直接回车选择1): "

if "%choice%"=="2" (
    echo.
    set /p target_dir="请输入Excel文件所在目录: "
    if "!target_dir!"=="" (
        echo [错误] 目录不能为空
        pause
        exit /b 1
    )
    python "%SCRIPT_DIR%import_history.py" "!target_dir!"
) else (
    echo.
    echo 正在扫描当前目录...
    python "%SCRIPT_DIR%import_history.py" "%SCRIPT_DIR%"
)

if errorlevel 1 (
    echo.
    echo [失败] 导入失败,请查看日志
    echo 日志位置: %SCRIPT_DIR%logs\
) else (
    echo.
    echo [成功] 导入完成!
    echo.
    echo 提示: 请访问 http://localhost:5000 查看更新后的数据
)

echo.
pause
