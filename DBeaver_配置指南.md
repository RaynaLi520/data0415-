# Target 爬虫 - DBeaver 数据库配置指南

## 📋 概述

你的爬虫代码 `target_crawler.py` **已经完成数据库改造**，现在支持：
- ✅ SQLite 数据库自动创建和管理
- ✅ 增量数据写入（新增/更新）
- ✅ 新品识别与报告生成
- ✅ 智能反爬策略

---

## 🚀 第一步：运行爬虫生成数据库

### 方法 1：直接运行（推荐）

```bash
python target_crawler.py
```

运行后会：
1. 自动创建 `target_products.db` 数据库文件
2. 创建 `products` 表及索引
3. 抓取数据并保存到数据库
4. 生成新品报告文件 `new_arrivals_YYYY-MM-DD.txt`

### 方法 2：测试数据库功能

```bash
python test_database.py
```

这会创建一个包含测试数据的数据库，用于验证功能。

---

## 🔧 第二步：DBeaver 配置步骤

### 1. 安装 DBeaver

如果还未安装，访问：https://dbeaver.io/download/

### 2. 新建 SQLite 连接

1. 打开 DBeaver
2. 点击左上角 **"新建连接"** 图标（或按 `Ctrl+N`）
3. 在搜索框输入 `sqlite`
4. 选择 **SQLite**，点击 **"下一步"**

### 3. 配置数据库文件

在连接设置中：

```
数据库: [浏览...] → 选择 target_products.db 文件
路径示例: d:\Lingma\target_products.db
```

**注意：**
- 如果数据库文件还不存在，先运行一次爬虫
- 或者点击"创建新数据库"，DBeaver 会自动创建

### 4. 完成连接

点击 **"完成"**，左侧导航栏会出现新的数据库连接。

---

## 📊 第三步：查看和使用数据

### 1. 浏览所有商品

```sql
SELECT * FROM products;
```

### 2. 查看今日新品

```sql
SELECT tcin, title, brand, price, color, is_new 
FROM products 
WHERE is_new_arrival = 1;
```

### 3. 按品牌筛选

```sql
SELECT * FROM products 
WHERE brand = 'Wild Fable';
```

### 4. 查看清仓商品

```sql
SELECT tcin, title, price, original_price, is_clearance 
FROM products 
WHERE is_clearance = 'Yes';
```

### 5. 统计各品牌商品数量

```sql
SELECT brand, COUNT(*) as count 
FROM products 
GROUP BY brand 
ORDER BY count DESC;
```

### 6. 查看价格区间

```sql
SELECT 
    brand,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price) as avg_price
FROM products 
GROUP BY brand;
```

### 7. 查找高评分商品

```sql
SELECT tcin, title, rating, rating_count 
FROM products 
WHERE rating >= 4.5 AND rating_count > 100
ORDER BY rating DESC;
```

---

## 🎯 第四步：每日运行流程

### 自动化运行脚本

创建批处理文件 `run_daily.bat`：

```batch
@echo off
echo ========================================
echo Target 爬虫 - 每日自动运行
echo ========================================

cd /d d:\Lingma

REM 运行爬虫
python target_crawler.py

echo.
echo ========================================
echo 运行完成！
echo 数据库文件: target_products.db
echo 日志文件: target_crawler.log
echo ========================================
pause
```

### Windows 任务计划程序

1. 打开 **任务计划程序**
2. 点击 **"创建基本任务"**
3. 名称：`Target 爬虫每日运行`
4. 触发器：每天上午 9:00
5. 操作：启动程序 → 选择 `run_daily.bat`

---

## 📁 文件说明

| 文件名 | 说明 |
|--------|------|
| `target_crawler.py` | 主爬虫程序（已支持数据库） |
| `target_products.db` | SQLite 数据库文件（运行后生成） |
| `test_database.py` | 数据库功能测试脚本 |
| `target_crawler.log` | 运行日志文件 |
| `new_arrivals_*.txt` | 每日新品报告 |

---

## 🔍 数据库表结构

### products 表

| 字段 | 类型 | 说明 |
|------|------|------|
| `tcin` | TEXT (主键) | 商品唯一标识 |
| `title` | TEXT | 商品标题 |
| `brand` | TEXT | 品牌名称 |
| `price` | REAL | 当前价格 |
| `retail_price` | REAL | 零售价 |
| `original_price` | REAL | 原价 |
| `has_promotion` | TEXT | 是否有促销 (Yes/No) |
| `savings_amount` | REAL | 节省金额 |
| `discount_percentage` | REAL | 折扣比例 |
| `max_discount` | REAL | 最大折扣 |
| `is_clearance` | TEXT | 是否清仓 (Yes/No) |
| `material_summary` | TEXT | 材料汇总 |
| `purchase_count` | INTEGER | 购买次数 |
| `delivery_date` | TEXT | 预计送达日期 |
| `is_new` | TEXT | 新品标签 (Yes/No) |
| `rating` | REAL | 评分 |
| `rating_count` | INTEGER | 评分数量 |
| `secondary_ratings` | TEXT | 分项评分 |
| `color_summary` | TEXT | 颜色汇总 |
| `color` | TEXT | 具体颜色 |
| `size_summary` | TEXT | 尺码汇总 |
| `concise_selling_points` | TEXT | 简洁卖点 |
| `product_type` | TEXT | 商品类型 |
| `image_url` | TEXT | 图片链接 |
| `buy_url` | TEXT | 购买链接 |
| `origin_brand` | TEXT | 原始品牌 |
| `origin_category` | TEXT | 原始分类 |
| `is_new_arrival` | INTEGER | 今日新品标记 (1=是, 0=否) |
| `first_seen_at` | TIMESTAMP | 首次发现时间 |
| `last_updated_at` | TIMESTAMP | 最后更新时间 |

---

## 💡 使用技巧

### 1. 导出数据为 Excel

在 DBeaver 中：
1. 执行查询
2. 右键结果集 → **导出数据**
3. 选择格式：Excel / CSV
4. 选择保存路径

### 2. 创建视图

```sql
-- 创建新品视图
CREATE VIEW new_products_view AS
SELECT tcin, title, brand, price, color, first_seen_at
FROM products
WHERE is_new_arrival = 1;

-- 创建清仓视图
CREATE VIEW clearance_view AS
SELECT tcin, title, brand, price, original_price, 
       (original_price - price) as savings
FROM products
WHERE is_clearance = 'Yes';
```

### 3. 备份数据库

定期复制 `target_products.db` 文件到备份目录。

---

## ❓ 常见问题

### Q1: 数据库文件在哪里？
A: 默认在当前目录：`d:\Lingma\target_products.db`

### Q2: 如何清空数据库重新开始？
A: 删除 `target_products.db` 文件，重新运行爬虫即可。

### Q3: 数据会重复吗？
A: 不会。代码使用 `tcin` 作为主键，相同 TCIN 只会更新，不会重复插入。

### Q4: 如何查看昨天的新品？
A: 
```sql
SELECT * FROM products 
WHERE DATE(first_seen_at) = DATE('now', '-1 day');
```

### Q5: DBeaver 连接失败？
A: 
1. 确保数据库文件存在（先运行一次爬虫）
2. 检查文件路径是否正确
3. 确保没有其他程序占用数据库文件

---

## 📞 技术支持

如遇问题，请检查：
1. 日志文件：`target_crawler.log`
2. 控制台输出信息
3. 数据库文件权限

---

**祝使用愉快！** 🎉
