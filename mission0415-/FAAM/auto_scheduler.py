"""
FAAM 自动化调度系统
每天北京时间18:30自动执行爬取和导入任务
"""
import os
import sys
import subprocess
import logging
from datetime import datetime
import traceback

# 日志配置
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f'scheduler_{datetime.now().strftime("%Y%m%d")}.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CRAWLER_SCRIPT = os.path.join(SCRIPT_DIR, 'auto_crawler.py')
IMPORT_SCRIPT = os.path.join(SCRIPT_DIR, 'auto_import.py')
PYTHON_EXECUTABLE = sys.executable

def run_script(script_path, script_name):
    """运行Python脚本"""
    logger.info(f"开始执行: {script_name}")
    logger.info(f"脚本路径: {script_path}")

    try:
        result = subprocess.run(
            [PYTHON_EXECUTABLE, script_path],
            capture_output=True,
            text=True,
            timeout=3600,  # 1小时超时
            encoding='utf-8',
            errors='ignore'
        )

        if result.stdout:
            logger.info(f"{script_name} 输出:\n{result.stdout}")

        if result.stderr:
            logger.warning(f"{script_name} 错误输出:\n{result.stderr}")

        if result.returncode == 0:
            logger.info(f"✓ {script_name} 执行成功")
            return True
        else:
            logger.error(f"✗ {script_name} 执行失败,返回码: {result.returncode}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"✗ {script_name} 执行超时")
        return False
    except Exception as e:
        logger.error(f"✗ {script_name} 执行异常: {e}")
        logger.error(traceback.format_exc())
        return False

def main():
    """主调度函数"""
    logger.info("="*60)
    logger.info("FAAM 自动化调度器启动")
    logger.info(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Python路径: {PYTHON_EXECUTABLE}")
    logger.info("="*60)

    start_time = datetime.now()

    # 步骤1: 执行爬虫
    logger.info("\n" + "="*60)
    logger.info("步骤 1/2: 执行数据爬取")
    logger.info("="*60)

    crawler_success = run_script(CRAWLER_SCRIPT, "自动爬虫")

    if not crawler_success:
        logger.error("爬虫执行失败,终止后续任务")
        return False

    # 步骤2: 导入数据到数据库
    logger.info("\n" + "="*60)
    logger.info("步骤 2/2: 执行数据导入")
    logger.info("="*60)

    import_success = run_script(IMPORT_SCRIPT, "数据导入")

    if not import_success:
        logger.error("数据导入失败")
        return False

    # 完成
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("\n" + "="*60)
    logger.info("✓ 所有任务执行完成!")
    logger.info(f"总耗时: {elapsed:.2f}秒")
    logger.info("="*60)

    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"调度器异常: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
