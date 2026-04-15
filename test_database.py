#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库功能测试脚本
用于验证 SQLite 数据库的初始化和基本操作
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import sqlite3
import os
from datetime import datetime

DATABASE_NAME = "target_products.db"

def test_database():
    """测试数据库功能"""
    print("="*60)
    print("🧪 Target 爬虫数据库功能测试")
    print("="*60)
    
    # 1. 测试数据库初始化
    print("\n[1/4] 测试数据库初始化...")
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # 创建表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            tcin TEXT PRIMARY KEY,
            title TEXT,
            brand TEXT,
            price REAL,
            retail_price REAL,
            original_price REAL,
            has_promotion TEXT,
            savings_amount REAL,
            discount_percentage REAL,
            max_discount REAL,
            is_clearance TEXT,
            material_summary TEXT,
            purchase_count INTEGER,
            delivery_date TEXT,
            is_new TEXT,
            rating REAL,
            rating_count INTEGER,
            secondary_ratings TEXT,
            color_summary TEXT,
            color TEXT,
            size_summary TEXT,
            concise_selling_points TEXT,
            product_type TEXT,
            image_url TEXT,
            buy_url TEXT,
            origin_brand TEXT,
            origin_category TEXT,
            is_new_arrival INTEGER DEFAULT 0,
            first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 创建索引
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_brand ON products(brand)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_product_type ON products(product_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_new ON products(is_new)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_new_arrival ON products(is_new_arrival)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_last_updated ON products(last_updated_at)')
    
    conn.commit()
    print("✅ 数据库表创建成功")
    
    # 2. 测试插入数据
    print("\n[2/4] 测试插入测试数据...")
    test_data = {
        'tcin': 'TEST001',
        'title': 'Test Product™ - Red M',
        'brand': 'Test Brand',
        'price': 29.99,
        'retail_price': 29.99,
        'original_price': 39.99,
        'has_promotion': 'Yes',
        'savings_amount': 10.00,
        'discount_percentage': 0.25,
        'max_discount': 0.25,
        'is_clearance': 'No',
        'material_summary': 'Material: Cotton',
        'purchase_count': 500,
        'delivery_date': 'Apr 16-18',
        'is_new': 'Yes',
        'rating': 4.5,
        'rating_count': 120,
        'secondary_ratings': 'Quality: 4.5 | Value: 4.0',
        'color_summary': '[共3种] Red, Blue, Green',
        'color': 'Red',
        'size_summary': '[共4种] S, M, L, XL',
        'concise_selling_points': 'Soft fabric | Comfortable fit',
        'product_type': 'T-Shirts',
        'image_url': 'https://example.com/image.jpg',
        'buy_url': 'https://www.target.com/p/-/A-TEST001',
        'origin_brand': 'Test Brand',
        'origin_category': 'Shirts'
    }
    
    cursor.execute('''
        INSERT INTO products (
            tcin, title, brand, price, retail_price, original_price,
            has_promotion, savings_amount, discount_percentage, max_discount,
            is_clearance, material_summary, purchase_count, delivery_date,
            is_new, rating, rating_count, secondary_ratings,
            color_summary, color, size_summary, concise_selling_points,
            product_type, image_url, buy_url,
            origin_brand, origin_category,
            is_new_arrival,
            first_seen_at, last_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ''', (
        test_data['tcin'], test_data['title'], test_data['brand'],
        test_data['price'], test_data['retail_price'], test_data['original_price'],
        test_data['has_promotion'], test_data['savings_amount'],
        test_data['discount_percentage'], test_data['max_discount'],
        test_data['is_clearance'], test_data['material_summary'],
        test_data['purchase_count'], test_data['delivery_date'],
        test_data['is_new'], test_data['rating'], test_data['rating_count'],
        test_data['secondary_ratings'], test_data['color_summary'],
        test_data['color'], test_data['size_summary'],
        test_data['concise_selling_points'], test_data['product_type'],
        test_data['image_url'], test_data['buy_url'],
        test_data['origin_brand'], test_data['origin_category']
    ))
    
    conn.commit()
    print("✅ 测试数据插入成功")
    
    # 3. 测试查询数据
    print("\n[3/4] 测试查询数据...")
    cursor.execute('SELECT COUNT(*) FROM products')
    total_count = cursor.fetchone()[0]
    print(f"   - 总记录数: {total_count}")
    
    cursor.execute('SELECT tcin, title, brand, price FROM products LIMIT 5')
    rows = cursor.fetchall()
    print(f"   - 示例数据:")
    for row in rows:
        print(f"     TCIN: {row[0]}, 标题: {row[1][:30]}..., 品牌: {row[2]}, 价格: ${row[3]}")
    
    # 4. 测试更新数据
    print("\n[4/4] 测试更新数据...")
    cursor.execute('''
        UPDATE products 
        SET price = ?, last_updated_at = CURRENT_TIMESTAMP 
        WHERE tcin = ?
    ''', (24.99, 'TEST001'))
    conn.commit()
    
    cursor.execute('SELECT price FROM products WHERE tcin = ?', ('TEST001',))
    updated_price = cursor.fetchone()[0]
    print(f"   - 更新后价格: ${updated_price}")
    print("✅ 数据更新成功")
    
    # 关闭连接
    conn.close()
    
    # 显示文件信息
    if os.path.exists(DATABASE_NAME):
        file_size = os.path.getsize(DATABASE_NAME)
        print(f"\n💾 数据库文件: {DATABASE_NAME}")
        print(f"   - 文件大小: {file_size / 1024:.2f} KB")
        print(f"   - 完整路径: {os.path.abspath(DATABASE_NAME)}")
    
    print("\n" + "="*60)
    print("✅ 所有测试通过！数据库功能正常")
    print("="*60)
    print("\n📋 下一步操作:")
    print("1. 打开 DBeaver")
    print("2. 点击「新建连接」→ 选择 SQLite")
    print(f"3. 数据库文件路径: {os.path.abspath(DATABASE_NAME)}")
    print("4. 点击「完成」即可在 DBeaver 中查看数据")
    print("\n💡 提示: 运行 target_crawler.py 后会填充真实数据")

if __name__ == "__main__":
    try:
        test_database()
    except Exception as e:
        print(f"\n❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
