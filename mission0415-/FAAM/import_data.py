"""
FAAM数据导入脚本
用于将爬虫数据导入到SQLite数据库中
"""
import pandas as pd
import sqlite3
import os
import glob
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), 'faam_products.db')

def init_db():
    """初始化数据库表"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
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

    cursor.execute('''
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

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_brand ON products(brand)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_new ON products(is_new)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date_added ON products(date_added)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_new_arrivals(date_detected)')

    conn.commit()
    conn.close()
    print("数据库初始化完成")

def import_excel_file(file_path):
    """从Excel文件导入数据"""
    if not os.path.exists(file_path):
        print(f"文件不存在: {file_path}")
        return 0

    try:
        print(f"正在读取文件: {file_path}")
        df = pd.read_excel(file_path, sheet_name="商品详情")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        imported_count = 0
        for index, row in df.iterrows():
            try:
                tcin = str(row.get('TCIN', '')).strip()
                if not tcin:
                    continue

                cursor.execute('''
                    INSERT OR REPLACE INTO products
                    (tcin, title, brand, price, retail_price, original_price,
                     has_promotion, savings_amount, discount_percentage, max_discount,
                     is_clearance, material, sales_count, delivery_date, is_new,
                     rating, review_count, secondary_ratings, color_summary, color,
                     size_summary, bullet_points, image_url, product_url, item_type,
                     date_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    tcin,
                    str(row.get('名称', '')),
                    str(row.get('品牌', '')),
                    float(row.get('价格', 0)) if pd.notna(row.get('价格')) else None,
                    float(row.get('零售价', 0)) if pd.notna(row.get('零售价')) else None,
                    float(row.get('原价', 0)) if pd.notna(row.get('原价')) else None,
                    str(row.get('促销活动', '')),
                    str(row.get('节省金额', '')),
                    str(row.get('折扣比例', '')),
                    float(row.get('最大折扣', 0)) if pd.notna(row.get('最大折扣')) else None,
                    str(row.get('清仓状态', '')),
                    str(row.get('材质(面料)', '')),
                    int(row.get('购买人数', 0)) if pd.notna(row.get('购买人数')) else None,
                    str(row.get('预计送达', '')),
                    str(row.get('商品标签', '')),
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
                imported_count += 1
            except Exception as e:
                print(f"导入第{index}行时出错: {e}")
                continue

        conn.commit()
        conn.close()

        print(f"成功导入 {imported_count} 条商品记录")
        return imported_count

    except Exception as e:
        print(f"导入文件时出错: {e}")
        return 0

def find_and_import_files(data_dir):
    """查找并导入所有Excel文件"""
    if not os.path.exists(data_dir):
        print(f"目录不存在: {data_dir}")
        return

    excel_files = glob.glob(os.path.join(data_dir, "**/*.xlsx"), recursive=True)
    excel_files = [f for f in excel_files if not os.path.basename(f).startswith('~$')]

    if not excel_files:
        print("未找到Excel文件")
        return

    print(f"找到 {len(excel_files)} 个Excel文件")

    total_imported = 0
    for file_path in excel_files:
        print(f"\n处理文件: {os.path.basename(file_path)}")
        count = import_excel_file(file_path)
        total_imported += count

    print(f"\n总计导入 {total_imported} 条记录")

if __name__ == "__main__":
    print("="*60)
    print("FAAM 数据导入工具")
    print("="*60)

    # 初始化数据库
    init_db()

    # 询问数据目录
    data_dir = input("\n请输入数据文件所在目录 (直接回车使用当前目录): ").strip()
    if not data_dir:
        data_dir = os.getcwd()

    # 导入数据
    find_and_import_files(data_dir)

    print("\n导入完成!")
    input("按回车键退出...")
