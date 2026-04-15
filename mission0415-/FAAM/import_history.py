"""
FAAM 历史数据批量导入工具
支持批量导入多个Excel文件到数据库
"""
import os
import sys
import glob
import sqlite3
import pandas as pd
from datetime import datetime
import logging
import traceback

# 日志配置
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f'history_import_{datetime.now().strftime("%Y%m%d")}.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), 'faam_products.db')

class HistoryDataImporter:
    """历史数据导入器"""

    def __init__(self):
        self.conn = None
        self.cursor = None
        self.stats = {
            'total_files': 0,
            'success_files': 0,
            'failed_files': 0,
            'total_records': 0,
            'new_records': 0,
            'updated_records': 0,
            'skipped_records': 0
        }

    def connect_db(self):
        """连接数据库"""
        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()
        logger.info("✓ 数据库连接成功")

    def close_db(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.commit()
            self.conn.close()
            logger.info("✓ 数据库连接已关闭")

    def init_database(self):
        """初始化数据库表结构"""
        logger.info("正在检查数据库结构...")

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
                is_new TEXT DEFAULT 'No',
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

        self.conn.commit()
        logger.info("✓ 数据库结构检查完成")

    def find_excel_files(self, directory):
        """查找指定目录下的所有Excel文件"""
        patterns = [
            os.path.join(directory, "**/*.xlsx"),
            os.path.join(directory, "**/*.xls")
        ]

        excel_files = []
        for pattern in patterns:
            files = glob.glob(pattern, recursive=True)
            # 排除临时文件
            files = [f for f in files if not os.path.basename(f).startswith('~$')]
            excel_files.extend(files)

        # 去重并排序
        excel_files = sorted(list(set(excel_files)))

        return excel_files

    def detect_sheet_name(self, file_path):
        """检测Excel文件中的工作表名称"""
        try:
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            sheet_names = excel_file.sheet_names

            # 优先使用的工作表名称
            preferred_sheets = ['商品详情', 'Products', 'Sheet1', 'Sheet']

            for sheet in preferred_sheets:
                if sheet in sheet_names:
                    return sheet

            # 如果没有找到首选的,返回第一个工作表
            if sheet_names:
                return sheet_names[0]

            return None
        except Exception as e:
            logger.error(f"读取Excel文件工作表失败: {e}")
            return None

    def map_columns(self, df):
        """智能映射列名"""
        # 常见的列名映射
        column_mapping = {
            # TCIN
            'TCIN': 'TCIN',
            'tcin': 'TCIN',
            '商品TCIN': 'TCIN',

            # 名称
            '名称': '名称',
            '标题': '名称',
            'title': '名称',
            'Title': '名称',
            '商品名称': '名称',

            # 品牌
            '品牌': '品牌',
            'brand': '品牌',
            'Brand': '品牌',

            # 价格
            '价格': '价格',
            'price': '价格',
            'Price': '价格',
            '当前价格': '价格',

            # 原价
            '原价': '原价',
            'original_price': '原价',
            'Original Price': '原价',
            '零售价': '原价',

            # 评分
            '评分': '评分',
            'rating': '评分',
            'Rating': '评分',
            '平均分': '评分',

            # 评论数量
            '评论数量': '评论数量',
            'review_count': '评论数量',
            'Review Count': '评论数量',
            '评价数': '评论数量',

            # 颜色
            '颜色': '颜色',
            'color': '颜色',
            'Color': '颜色',

            # 颜色汇总
            '颜色汇总': '颜色汇总',
            'color_summary': '颜色汇总',

            # 尺码汇总
            '尺码汇总': '尺码汇总',
            'size_summary': '尺码汇总',

            # 图片链接
            '图片链接': '图片链接',
            'image_url': '图片链接',
            'Image URL': '图片链接',
            '图片URL': '图片链接',

            # 购买链接
            '购买链接': '购买链接',
            'product_url': '购买链接',
            'Product URL': '购买链接',
            '链接': '购买链接',
            'URL': '购买链接',

            # 商品标签
            '商品标签': '商品标签',
            'is_new': '商品标签',
            'Is New': '商品标签',
            '标签': '商品标签',

            # 清仓状态
            '清仓状态': '清仓状态',
            'is_clearance': '清仓状态',

            # 促销活动
            '促销活动': '促销活动',
            'has_promotion': '促销活动',

            # 材质
            '材质(面料)': '材质(面料)',
            'material': '材质(面料)',
            '材质': '材质(面料)',

            # 购买人数
            '购买人数': '购买人数',
            'sales_count': '购买人数',

            # 预计送达
            '预计送达': '预计送达',
            'delivery_date': '预计送达',

            # 商品分类
            '商品分类': '商品分类',
            'item_type': '商品分类',
            'category': '商品分类',

            # 其他
            '次要评分': '次要评分',
            '商品要点': '商品要点',
            '节省金额': '节省金额',
            '折扣比例': '折扣比例',
            '最大折扣': '最大折扣',
            '零售价': '零售价',
        }

        # 创建新的列名映射
        new_columns = {}
        for col in df.columns:
            if col in column_mapping:
                new_columns[col] = column_mapping[col]
            else:
                # 尝试去除空格后匹配
                col_stripped = col.strip()
                if col_stripped in column_mapping:
                    new_columns[col] = column_mapping[col_stripped]
                else:
                    new_columns[col] = col

        df = df.rename(columns=new_columns)
        return df

    def import_single_file(self, file_path):
        """导入单个Excel文件"""
        logger.info(f"\n{'='*60}")
        logger.info(f"处理文件: {os.path.basename(file_path)}")
        logger.info(f"{'='*60}")

        try:
            # 检测工作表
            sheet_name = self.detect_sheet_name(file_path)
            if not sheet_name:
                logger.error(f"✗ 无法检测到工作表: {file_path}")
                self.stats['failed_files'] += 1
                return False

            logger.info(f"使用工作表: {sheet_name}")

            # 读取Excel
            df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')

            if df.empty:
                logger.warning(f"⚠ 文件为空: {file_path}")
                self.stats['failed_files'] += 1
                return False

            logger.info(f"读取到 {len(df)} 条记录")

            # 映射列名
            df = self.map_columns(df)

            # 检查必需的列
            if 'TCIN' not in df.columns:
                logger.error(f"✗ 文件中缺少TCIN列")
                self.stats['failed_files'] += 1
                return False

            # 获取现有TCIN列表
            existing_tcins = set()
            self.cursor.execute('SELECT tcin FROM products')
            for row in self.cursor.fetchall():
                existing_tcins.add(row[0])

            # 导入数据
            file_new_count = 0
            file_update_count = 0
            file_skip_count = 0

            for index, row in df.iterrows():
                try:
                    tcin = str(row.get('TCIN', '')).strip()
                    if not tcin or tcin == 'nan':
                        file_skip_count += 1
                        continue

                    # 准备数据
                    data = {
                        'tcin': tcin,
                        'title': str(row.get('名称', '')) if pd.notna(row.get('名称')) else '',
                        'brand': str(row.get('品牌', '')) if pd.notna(row.get('品牌')) else '',
                        'price': float(row.get('价格', 0)) if pd.notna(row.get('价格')) else None,
                        'retail_price': float(row.get('零售价', 0)) if pd.notna(row.get('零售价')) else None,
                        'original_price': float(row.get('原价', 0)) if pd.notna(row.get('原价')) else None,
                        'has_promotion': str(row.get('促销活动', 'No')) if pd.notna(row.get('促销活动')) else 'No',
                        'savings_amount': str(row.get('节省金额', '')) if pd.notna(row.get('节省金额')) else '',
                        'discount_percentage': str(row.get('折扣比例', '')) if pd.notna(row.get('折扣比例')) else '',
                        'max_discount': float(row.get('最大折扣', 0)) if pd.notna(row.get('最大折扣')) else None,
                        'is_clearance': str(row.get('清仓状态', 'No')) if pd.notna(row.get('清仓状态')) else 'No',
                        'material': str(row.get('材质(面料)', '')) if pd.notna(row.get('材质(面料)')) else '',
                        'sales_count': int(row.get('购买人数', 0)) if pd.notna(row.get('购买人数')) else None,
                        'delivery_date': str(row.get('预计送达', '')) if pd.notna(row.get('预计送达')) else '',
                        'is_new': 'No',  # 历史数据默认不是新品
                        'rating': float(row.get('评分', 0)) if pd.notna(row.get('评分')) else None,
                        'review_count': int(row.get('评论数量', 0)) if pd.notna(row.get('评论数量')) else None,
                        'secondary_ratings': str(row.get('次要评分', '')) if pd.notna(row.get('次要评分')) else '',
                        'color_summary': str(row.get('颜色汇总', '')) if pd.notna(row.get('颜色汇总')) else '',
                        'color': str(row.get('颜色', '')) if pd.notna(row.get('颜色')) else '',
                        'size_summary': str(row.get('尺码汇总', '')) if pd.notna(row.get('尺码汇总')) else '',
                        'bullet_points': str(row.get('商品要点', '')) if pd.notna(row.get('商品要点')) else '',
                        'image_url': str(row.get('图片链接', '')) if pd.notna(row.get('图片链接')) else '',
                        'product_url': str(row.get('购买链接', '')) if pd.notna(row.get('购买链接')) else '',
                        'item_type': str(row.get('商品分类', '')) if pd.notna(row.get('商品分类')) else '',
                        'date_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }

                    # 判断是新增还是更新
                    if tcin in existing_tcins:
                        # 更新现有记录
                        self.cursor.execute('''
                            UPDATE products SET
                                title=?, brand=?, price=?, retail_price=?, original_price=?,
                                has_promotion=?, savings_amount=?, discount_percentage=?, max_discount=?,
                                is_clearance=?, material=?, sales_count=?, delivery_date=?,
                                rating=?, review_count=?, secondary_ratings=?,
                                color_summary=?, color=?, size_summary=?, bullet_points=?,
                                image_url=?, product_url=?, item_type=?, date_updated=?
                            WHERE tcin=?
                        ''', (
                            data['title'], data['brand'], data['price'], data['retail_price'],
                            data['original_price'], data['has_promotion'], data['savings_amount'],
                            data['discount_percentage'], data['max_discount'], data['is_clearance'],
                            data['material'], data['sales_count'], data['delivery_date'],
                            data['rating'], data['review_count'], data['secondary_ratings'],
                            data['color_summary'], data['color'], data['size_summary'],
                            data['bullet_points'], data['image_url'], data['product_url'],
                            data['item_type'], data['date_updated'], tcin
                        ))
                        file_update_count += 1
                    else:
                        # 插入新记录
                        data['date_added'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        self.cursor.execute('''
                            INSERT INTO products
                            (tcin, title, brand, price, retail_price, original_price,
                             has_promotion, savings_amount, discount_percentage, max_discount,
                             is_clearance, material, sales_count, delivery_date, is_new,
                             rating, review_count, secondary_ratings, color_summary, color,
                             size_summary, bullet_points, image_url, product_url, item_type,
                             date_added, date_updated)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            data['tcin'], data['title'], data['brand'], data['price'],
                            data['retail_price'], data['original_price'], data['has_promotion'],
                            data['savings_amount'], data['discount_percentage'], data['max_discount'],
                            data['is_clearance'], data['material'], data['sales_count'],
                            data['delivery_date'], data['is_new'], data['rating'],
                            data['review_count'], data['secondary_ratings'], data['color_summary'],
                            data['color'], data['size_summary'], data['bullet_points'],
                            data['image_url'], data['product_url'], data['item_type'],
                            data['date_added'], data['date_updated']
                        ))
                        existing_tcins.add(tcin)
                        file_new_count += 1

                except Exception as e:
                    logger.error(f"处理第{index+1}行时出错: {e}")
                    file_skip_count += 1
                    continue

            self.conn.commit()

            logger.info(f"✓ 文件处理完成:")
            logger.info(f"  - 新增: {file_new_count} 条")
            logger.info(f"  - 更新: {file_update_count} 条")
            logger.info(f"  - 跳过: {file_skip_count} 条")

            self.stats['success_files'] += 1
            self.stats['new_records'] += file_new_count
            self.stats['updated_records'] += file_update_count
            self.stats['skipped_records'] += file_skip_count
            self.stats['total_records'] += len(df)

            return True

        except Exception as e:
            logger.error(f"✗ 处理文件失败: {e}")
            logger.error(traceback.format_exc())
            self.stats['failed_files'] += 1
            return False

    def generate_report(self):
        """生成导入报告"""
        logger.info("\n" + "="*60)
        logger.info("历史数据导入报告")
        logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*60)
        logger.info(f"总文件数: {self.stats['total_files']}")
        logger.info(f"成功文件: {self.stats['success_files']}")
        logger.info(f"失败文件: {self.stats['failed_files']}")
        logger.info(f"总记录数: {self.stats['total_records']}")
        logger.info(f"新增记录: {self.stats['new_records']}")
        logger.info(f"更新记录: {self.stats['updated_records']}")
        logger.info(f"跳过记录: {self.stats['skipped_records']}")
        logger.info("="*60)

        # 获取数据库统计
        self.cursor.execute('SELECT COUNT(*) FROM products')
        total_products = self.cursor.fetchone()[0]

        self.cursor.execute('''
            SELECT brand, COUNT(*) as count
            FROM products
            WHERE brand IN ('A New Day', 'Wild Fable')
            GROUP BY brand
        ''')
        brand_stats = self.cursor.fetchall()

        logger.info(f"\n数据库当前状态:")
        logger.info(f"  - 总商品数: {total_products}")
        for brand, count in brand_stats:
            logger.info(f"  - {brand}: {count}")

        # 保存报告
        report = f"""
{'='*60}
FAAM 历史数据导入报告
时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*60}

导入统计:
  总文件数: {self.stats['total_files']}
  成功文件: {self.stats['success_files']}
  失败文件: {self.stats['failed_files']}
  总记录数: {self.stats['total_records']}
  新增记录: {self.stats['new_records']}
  更新记录: {self.stats['updated_records']}
  跳过记录: {self.stats['skipped_records']}

数据库状态:
  总商品数: {total_products}
"""
        for brand, count in brand_stats:
            report += f"  {brand}: {count}\n"

        report += "="*60

        report_file = os.path.join(LOG_DIR, f"history_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)

        logger.info(f"\n报告已保存到: {report_file}")

    def run(self, directory=None):
        """执行批量导入"""
        logger.info("="*60)
        logger.info("FAAM 历史数据批量导入工具")
        logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*60)

        try:
            # 1. 连接数据库
            self.connect_db()

            # 2. 初始化数据库
            self.init_database()

            # 3. 查找Excel文件
            if not directory:
                directory = input("\n请输入Excel文件所在目录 (直接回车扫描整个FAAM目录): ").strip()
                if not directory:
                    directory = os.path.dirname(__file__)

            if not os.path.exists(directory):
                logger.error(f"目录不存在: {directory}")
                return False

            logger.info(f"\n正在扫描目录: {directory}")
            excel_files = self.find_excel_files(directory)

            if not excel_files:
                logger.error("未找到任何Excel文件")
                return False

            self.stats['total_files'] = len(excel_files)
            logger.info(f"找到 {len(excel_files)} 个Excel文件\n")

            # 4. 逐个导入
            for i, file_path in enumerate(excel_files, 1):
                logger.info(f"\n[{i}/{len(excel_files)}]")
                self.import_single_file(file_path)

            # 5. 生成报告
            self.generate_report()

            logger.info("\n✓ 批量导入完成!")
            return True

        except Exception as e:
            logger.error(f"导入过程出错: {e}")
            logger.error(traceback.format_exc())
            return False
        finally:
            self.close_db()


if __name__ == "__main__":
    try:
        importer = HistoryDataImporter()

        # 可以通过命令行参数指定目录
        directory = sys.argv[1] if len(sys.argv) > 1 else None

        success = importer.run(directory)
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("\n\n用户中断操作")
        sys.exit(1)
    except Exception as e:
        logger.error(f"程序异常: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
