@echo off
chcp 65001 >nul
echo ========================================
echo    FAAM 女装信息平台 - 启动脚本
echo ========================================
echo.

REM 检查Python是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未检测到Python,请先安装Python 3.8+
    pause
    exit /b 1
)

echo [1/3] 检查依赖...
python -c "import flask" >nul 2>&1
if errorlevel 1 (
    echo [提示] 正在安装Flask...
    pip install Flask
)

python -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo [提示] 正在安装pandas...
    pip install pandas
)

python -c "import openpyxl" >nul 2>&1
if errorlevel 1 (
    echo [提示] 正在安装openpyxl...
    pip install openpyxl
)

echo.
echo [2/3] 初始化数据库...
if not exist "faam_products.db" (
    python import_data.py
) else (
    echo 数据库已存在,跳过初始化
)

echo.
echo [3/3] 启动Web应用...
echo.
echo ========================================
echo  应用已启动!
echo  请在浏览器中访问: http://localhost:5000
echo  按 Ctrl+C 停止应用
echo ========================================
echo.

python app.py

pause
