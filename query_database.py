#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库查询脚本 - 展示当前数据库内容
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import sqlite3
import os

DATABASE_NAME = "target_products.db"

def query_database():
    """查询并展示数据库内容"""
    
    if not os.path.exists(DATABASE_NAME):
        print(f"❌ 数据库文件不存在: {DATABASE_NAME}")
        print("请先运行 target_crawler.py 或 test_database.py")
        return
    
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    print("="*70)
    print("📊 Target 产品数据库 - 当前内容")
    print("="*70)
    
    # 1. 总记录数
    cursor.execute('SELECT COUNT(*) FROM products')
    total = cursor.fetchone()[0]
    print(f"\n📦 总商品数: {total}")
    
    # 2. 按品牌统计
    print("\n📋 按品牌统计:")
    print("-"*70)
    cursor.execute('''
        SELECT brand, COUNT(*) as count, 
               ROUND(AVG(price), 2) as avg_price
        FROM products 
        GROUP BY brand 
        ORDER BY count DESC
    ''')
    brands = cursor.fetchall()
    print(f"{'品牌':<25} {'数量':<10} {'平均价格':<10}")
    print("-"*70)
    for brand, count, avg_price in brands:
        print(f"{brand:<25} {count:<10} ${avg_price:<9.2f}")
    
    # 3. 新品统计
    cursor.execute('SELECT COUNT(*) FROM products WHERE is_new_arrival = 1')
    new_count = cursor.fetchone()[0]
    print(f"\n🆕 今日新品: {new_count}")
    
    # 4. 显示示例数据
    print("\n📝 示例商品 (前5条):")
    print("-"*70)
    cursor.execute('''
        SELECT tcin, title, brand, price, color, is_new_arrival
        FROM products 
        LIMIT 5
    ''')
    products = cursor.fetchall()
    
    for i, (tcin, title, brand, price, color, is_new) in enumerate(products, 1):
        print(f"\n[{i}] TCIN: {tcin}")
        print(f"    标题: {title[:50]}...")
        print(f"    品牌: {brand}")
        print(f"    价格: ${price:.2f}")
        print(f"    颜色: {color or 'N/A'}")
        print(f"    新品: {'是' if is_new else '否'}")
    
    # 5. 表结构信息
    print("\n" + "="*70)
    print("📐 数据库表结构:")
    print("-"*70)
    cursor.execute("PRAGMA table_info(products)")
    columns = cursor.fetchall()
    print(f"{'字段名':<30} {'类型':<15} {'主键':<6}")
    print("-"*70)
    for col in columns:
        cid, name, dtype, notnull, default_val, pk = col
        print(f"{name:<30} {dtype:<15} {'✓' if pk else '':<6}")
    
    conn.close()
    
    print("\n" + "="*70)
    print("✅ 查询完成")
    print("="*70)
    print(f"\n💡 提示:")
    print(f"   - 数据库文件: {os.path.abspath(DATABASE_NAME)}")
    print(f"   - 文件大小: {os.path.getsize(DATABASE_NAME) / 1024:.2f} KB")
    print(f"   - 使用 DBeaver 可以查看更多详细信息和进行高级查询")

if __name__ == "__main__":
    try:
        query_database()
    except Exception as e:
        print(f"\n❌ 查询失败: {str(e)}")
        import traceback
        traceback.print_exc()
