"""
FAAM 自动数据导入和新品标记系统
自动检测最新爬取的数据文件,导入数据库并标记新品
"""
import os
import sys
import glob
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging
import traceback
import shutil

# 日志配置
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f'import_{datetime.now().strftime("%Y%m%d")}.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), 'faam_products.db')
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

class FAAMDataImporter:
    """FAAM数据导入器"""

    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect_db(self):
        """连接数据库"""
        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()
        logger.info("数据库连接成功")

    def close_db(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")

    def init_database(self):
        """初始化数据库表结构"""
        logger.info("初始化数据库...")

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tcin TEXT UNIQUE,
                title TEXT,
                brand TEXT,
                price REAL,
                retail_price REAL,
                original_price REAL,
                has_promotion TEXT,
                savings_amount TEXT,
                discount_percentage TEXT,
                max_discount REAL,
                is_clearance TEXT,
                material TEXT,
                sales_count INTEGER,
                delivery_date TEXT,
                is_new TEXT,
                rating REAL,
                review_count INTEGER,
                secondary_ratings TEXT,
                color_summary TEXT,
                color TEXT,
                size_summary TEXT,
                bullet_points TEXT,
                image_url TEXT,
                product_url TEXT,
                item_type TEXT,
                date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_new_arrivals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tcin TEXT,
                title TEXT,
                brand TEXT,
                price REAL,
                image_url TEXT,
                product_url TEXT,
                date_detected DATE,
                is_processed INTEGER DEFAULT 0,
                FOREIGN KEY (tcin) REFERENCES products(tcin)
            )
        ''')

        # 创建索引
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_brand ON products(brand)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_new ON products(is_new)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_date_added ON products(date_added)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_tcin ON products(tcin)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_new_arrivals(date_detected)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_tcin ON daily_new_arrivals(tcin)')

        self.conn.commit()
        logger.info("数据库初始化完成")

    def find_latest_file(self):
        """查找最新的数据文件"""
        pattern = os.path.join(DATA_DIR, "FAAM_Data_*.xlsx")
        files = glob.glob(pattern)

        if not files:
            logger.error(f"在 {DATA_DIR} 目录下未找到FAAM数据文件")
            return None

        # 按修改时间排序,获取最新的文件
        files.sort(key=os.path.getmtime, reverse=True)
        latest_file = files[0]

        logger.info(f"找到最新数据文件: {os.path.basename(latest_file)}")
        return latest_file

    def identify_new_products(self, new_tcin_set):
        """识别新品(之前数据库中不存在的TCIN)"""
        logger.info("正在识别新品...")

        # 获取数据库中所有现有的TCIN
        existing_tcin = set()
        self.cursor.execute('SELECT tcin FROM products')
        for row in self.cursor.fetchall():
            existing_tcin.add(row[0])

        # 新品 = 新数据中有但数据库中没有的TCIN
        new_products = new_tcin_set - existing_tcin

        logger.info(f"数据库中原有商品: {len(existing_tcin)} 个")
        logger.info(f"本次新增商品: {len(new_products)} 个")

        return new_products

    def import_data(self, file_path):
        """导入Excel数据到数据库"""
        logger.info(f"开始导入文件: {file_path}")

        try:
            # 读取Excel文件
            df = pd.read_excel(file_path, engine='openpyxl')
            logger.info(f"读取到 {len(df)} 条记录")

            if df.empty:
                logger.warning("数据文件为空")
                return False

            # 获取所有TCIN用于新品识别
            new_tcin_set = set(df['TCIN'].astype(str).tolist())

            # 识别新品
            new_product_tcins = self.identify_new_products(new_tcin_set)

            # 导入/更新数据
            imported_count = 0
            updated_count = 0
            new_count = 0

            today = datetime.now().strftime('%Y-%m-%d')

            for index, row in df.iterrows():
                try:
                    tcin = str(row.get('TCIN', '')).strip()
                    if not tcin:
                        continue

                    # 检查是否为新商品
                    is_new_product = tcin in new_product_tcins

                    # 构建更新SQL
                    self.cursor.execute('''
                        INSERT INTO products
                        (tcin, title, brand, price, retail_price, original_price,
                         has_promotion, savings_amount, discount_percentage, max_discount,
                         is_clearance, material, sales_count, delivery_date, is_new,
                         rating, review_count, secondary_ratings, color_summary, color,
                         size_summary, bullet_points, image_url, product_url, item_type,
                         date_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(tcin) DO UPDATE SET
                            title=excluded.title,
                            brand=excluded.brand,
                            price=excluded.price,
                            retail_price=excluded.retail_price,
                            original_price=excluded.original_price,
                            has_promotion=excluded.has_promotion,
                            is_clearance=excluded.is_clearance,
                            rating=excluded.rating,
                            review_count=excluded.review_count,
                            color_summary=excluded.color_summary,
                            color=excluded.color,
                            size_summary=excluded.size_summary,
                            image_url=excluded.image_url,
                            product_url=excluded.product_url,
                            item_type=excluded.item_type,
                            is_new=CASE WHEN products.is_new = 'Yes' THEN 'Yes' ELSE excluded.is_new END,
                            date_updated=excluded.date_updated
                    ''', (
                        tcin,
                        str(row.get('名称', '')),
                        str(row.get('品牌', '')),
                        float(row.get('价格', 0)) if pd.notna(row.get('价格')) else None,
                        float(row.get('零售价', 0)) if pd.notna(row.get('零售价')) else None,
                        float(row.get('原价', 0)) if pd.notna(row.get('原价')) else None,
                        str(row.get('促销活动', 'No')),
                        str(row.get('节省金额', '')),
                        str(row.get('折扣比例', '')),
                        float(row.get('最大折扣', 0)) if pd.notna(row.get('最大折扣')) else None,
                        str(row.get('清仓状态', 'No')),
                        str(row.get('材质(面料)', '')),
                        int(row.get('购买人数', 0)) if pd.notna(row.get('购买人数')) else None,
                        str(row.get('预计送达', '')),
                        'Yes' if is_new_product else str(row.get('商品标签', 'No')),
                        float(row.get('评分', 0)) if pd.notna(row.get('评分')) else None,
                        int(row.get('评论数量', 0)) if pd.notna(row.get('评论数量')) else None,
                        str(row.get('次要评分', '')),
                        str(row.get('颜色汇总', '')),
                        str(row.get('颜色', '')),
                        str(row.get('尺码汇总', '')),
                        str(row.get('商品要点', '')),
                        str(row.get('图片链接', '')),
                        str(row.get('购买链接', '')),
                        str(row.get('商品分类', '')),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ))

                    if self.cursor.rowcount > 0:
                        if is_new_product:
                            new_count += 1
                            # 记录到每日新品表
                            self.cursor.execute('''
                                INSERT INTO daily_new_arrivals
                                (tcin, title, brand, price, image_url, product_url, date_detected)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                tcin,
                                str(row.get('名称', '')),
                                str(row.get('品牌', '')),
                                float(row.get('价格', 0)) if pd.notna(row.get('价格')) else None,
                                str(row.get('图片链接', '')),
                                str(row.get('购买链接', '')),
                                today
                            ))
                        else:
                            updated_count += 1

                    imported_count += 1

                    if (index + 1) % 100 == 0:
                        logger.info(f"已处理 {index + 1}/{len(df)} 条记录")

                except Exception as e:
                    logger.error(f"导入第{index}行时出错: {e}")
                    continue

            self.conn.commit()

            logger.info("="*60)
            logger.info("数据导入完成!")
            logger.info(f"总记录数: {imported_count}")
            logger.info(f"新增商品: {new_count}")
            logger.info(f"更新商品: {updated_count}")
            logger.info("="*60)

            # 移动已处理的文件到archive目录
            self.archive_file(file_path)

            return True

        except Exception as e:
            logger.error(f"导入数据时出错: {e}")
            logger.error(traceback.format_exc())
            return False

    def archive_file(self, file_path):
        """归档已处理的文件"""
        archive_dir = os.path.join(DATA_DIR, 'archive')
        os.makedirs(archive_dir, exist_ok=True)

        filename = os.path.basename(file_path)
        dest = os.path.join(archive_dir, filename)

        try:
            shutil.move(file_path, dest)
            logger.info(f"文件已归档: {dest}")
        except Exception as e:
            logger.warning(f"归档文件失败: {e}")

    def generate_report(self):
        """生成导入报告"""
        logger.info("生成统计报告...")

        # 总商品数
        self.cursor.execute('SELECT COUNT(*) FROM products')
        total = self.cursor.fetchone()[0]

        # 各品牌商品数
        self.cursor.execute('''
            SELECT brand, COUNT(*) as count
            FROM products
            WHERE brand IN ('A New Day', 'Wild Fable')
            GROUP BY brand
        ''')
        brand_stats = self.cursor.fetchall()

        # 今日新品数
        today = datetime.now().strftime('%Y-%m-%d')
        self.cursor.execute('''
            SELECT COUNT(*) FROM daily_new_arrivals
            WHERE date_detected = ?
        ''', (today,))
        today_new = self.cursor.fetchone()[0]

        # 总新品数
        self.cursor.execute('SELECT COUNT(*) FROM products WHERE is_new = "Yes"')
        total_new = self.cursor.fetchone()[0]

        report = f"""
{'='*60}
FAAM 数据导入报告
时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*60}

数据库统计:
  - 总商品数: {total}
  - 新品总数: {total_new}
  - 今日新品: {today_new}

品牌分布:
"""
        for brand, count in brand_stats:
            report += f"  - {brand}: {count}\n"

        report += "="*60

        logger.info(report)

        # 保存报告到文件
        report_file = os.path.join(LOG_DIR, f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)

        logger.info(f"报告已保存到: {report_file}")

    def run(self):
        """执行完整的导入流程"""
        logger.info("="*60)
        logger.info("FAAM 自动数据导入系统启动")
        logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*60)

        try:
            # 1. 连接数据库
            self.connect_db()

            # 2. 初始化数据库
            self.init_database()

            # 3. 查找最新数据文件
            latest_file = self.find_latest_file()
            if not latest_file:
                logger.error("未找到数据文件,退出")
                return False

            # 4. 导入数据
            success = self.import_data(latest_file)
            if not success:
                logger.error("数据导入失败")
                return False

            # 5. 生成报告
            self.generate_report()

            logger.info("✓ 导入任务成功完成")
            return True

        except Exception as e:
            logger.error(f"导入过程出错: {e}")
            logger.error(traceback.format_exc())
            return False
        finally:
            self.close_db()


if __name__ == "__main__":
    try:
        importer = FAAMDataImporter()
        success = importer.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"程序异常: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
