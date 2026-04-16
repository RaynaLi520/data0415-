from flask import Flask, render_template, request, jsonify, send_from_directory
import sqlite3
import os
from datetime import datetime
import pandas as pd

app = Flask(__name__)
app.config['SECRET_KEY'] = 'faam-womens-clothing-2026'

# Database path
DB_PATH = os.path.join(os.path.dirname(__file__), 'faam_products.db')
IMAGE_FOLDER = os.path.join(os.path.dirname(__file__), 'images')

def get_db_connection():
    """Create database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize database tables"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create products table
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

    # Create daily_new_arrivals table
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

    # Create indexes for better query performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_brand ON products(brand)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_new ON products(is_new)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date_added ON products(date_added)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_new_arrivals(date_detected)')

    conn.commit()
    conn.close()

@app.route('/')
def index():
    """Home page - Show FAAM project overview"""
    conn = get_db_connection()

    # Get total products count
    total_products = conn.execute('SELECT COUNT(*) FROM products').fetchone()[0]

    # Get products by brand
    brand_stats = conn.execute('''
        SELECT brand, COUNT(*) as count
        FROM products
        WHERE brand IN ('A New Day', 'Wild Fable')
        GROUP BY brand
    ''').fetchall()

    # Get new arrivals count
    new_count = conn.execute('SELECT COUNT(*) FROM products WHERE is_new = "Yes"').fetchone()[0]

    # Get today's new arrivals
    today = datetime.now().strftime('%Y-%m-%d')
    today_new = conn.execute('''
        SELECT dna.*, p.image_url, p.product_url
        FROM daily_new_arrivals dna
        LEFT JOIN products p ON dna.tcin = p.tcin
        WHERE dna.date_detected = ?
        LIMIT 10
    ''', (today,)).fetchall()

    conn.close()

    return render_template('index.html',
                         total_products=total_products,
                         brand_stats=brand_stats,
                         new_count=new_count,
                         today_new=today_new)

@app.route('/products')
def products():
    """Products listing page with filters"""
    brand = request.args.get('brand', '')
    is_new = request.args.get('is_new', '')
    search = request.args.get('search', '')
    tcin = request.args.get('tcin', '')
    is_clearance = request.args.get('is_clearance', '')
    has_discount = request.args.get('has_discount', '')
    item_type = request.args.get('item_type', '')
    page = request.args.get('page', 1, type=int)
    per_page = 24

    conn = get_db_connection()

    # Build query with filters
    query = 'SELECT * FROM products WHERE 1=1'
    count_query = 'SELECT COUNT(*) FROM products WHERE 1=1'
    params = []

    if brand:
        query += ' AND brand = ?'
        count_query += ' AND brand = ?'
        params.append(brand)

    if is_new == 'yes':
        query += ' AND is_new = "Yes"'
        count_query += ' AND is_new = "Yes"'

    if tcin:
        query += ' AND tcin = ?'
        count_query += ' AND tcin = ?'
        params.append(tcin)

    if is_clearance == 'yes':
        query += ' AND is_clearance = "Yes"'
        count_query += ' AND is_clearance = "Yes"'

    if has_discount == 'yes':
        query += ' AND (discount_percentage IS NOT NULL AND discount_percentage != "" AND discount_percentage != "0%")'
        count_query += ' AND (discount_percentage IS NOT NULL AND discount_percentage != "" AND discount_percentage != "0%")'

    if item_type:
        query += ' AND item_type = ?'
        count_query += ' AND item_type = ?'
        params.append(item_type)

    if search:
        query += ' AND (title LIKE ? OR brand LIKE ?)'
        count_query += ' AND (title LIKE ? OR brand LIKE ?)'
        search_term = f'%{search}%'
        params.extend([search_term, search_term])

    # Get total count
    total = conn.execute(count_query, params).fetchone()[0]
    total_pages = (total + per_page - 1) // per_page

    # Add pagination and ordering
    query += ' ORDER BY date_added DESC LIMIT ? OFFSET ?'
    params.extend([per_page, (page - 1) * per_page])

    products_list = conn.execute(query, params).fetchall()

    # Get all item types for filter
    item_types = conn.execute('''
        SELECT item_type, COUNT(*) as cnt
        FROM products
        WHERE item_type IS NOT NULL AND item_type != ''
        GROUP BY item_type
        ORDER BY cnt DESC
    ''').fetchall()

    conn.close()

    return render_template('products.html',
                         products=products_list,
                         page=page,
                         total_pages=total_pages,
                         brand=brand,
                         is_new=is_new,
                         search=search,
                         tcin=tcin,
                         is_clearance=is_clearance,
                         has_discount=has_discount,
                         item_type=item_type,
                         item_types=item_types,
                         total=total)

@app.route('/new-arrivals')
def new_arrivals():
    """Daily new arrivals page"""
    date = request.args.get('date', '')

    conn = get_db_connection()

    # Get new arrivals for selected date (or all if empty/all)
    if date and date != 'all':
        new_products = conn.execute('''
            SELECT dna.*, p.*
            FROM daily_new_arrivals dna
            LEFT JOIN products p ON dna.tcin = p.tcin
            WHERE dna.date_detected = ?
            ORDER BY dna.id DESC
        ''', (date,)).fetchall()
    else:
        new_products = conn.execute('''
            SELECT dna.*, p.*
            FROM daily_new_arrivals dna
            LEFT JOIN products p ON dna.tcin = p.tcin
            ORDER BY dna.date_detected DESC, dna.id DESC
        ''').fetchall()

    # Get available dates
    dates = conn.execute('''
        SELECT DISTINCT date_detected
        FROM daily_new_arrivals
        ORDER BY date_detected DESC
        LIMIT 30
    ''').fetchall()

    conn.close()

    return render_template('new_arrivals.html',
                         new_products=new_products,
                         selected_date=date,
                         dates=dates)

@app.route('/api/products')
def api_products():
    """API endpoint for products"""
    brand = request.args.get('brand', '')
    is_new = request.args.get('is_new', '')

    conn = get_db_connection()
    query = 'SELECT * FROM products WHERE 1=1'
    params = []

    if brand:
        query += ' AND brand = ?'
        params.append(brand)

    if is_new == 'yes':
        query += ' AND is_new = "Yes"'

    query += ' ORDER BY date_added DESC LIMIT 100'

    products = conn.execute(query, params).fetchall()
    conn.close()

    return jsonify([dict(p) for p in products])

@app.route('/api/dashboard')
def api_dashboard():
    """API endpoint for dashboard chart data (new arrivals only)"""
    date = request.args.get('date', '')

    conn = get_db_connection()

    # 每日新品数量趋势
    daily_counts = conn.execute('''
        SELECT date_detected, COUNT(*) as count
        FROM daily_new_arrivals
        GROUP BY date_detected
        ORDER BY date_detected ASC
    ''').fetchall()

    # 构建过滤条件
    date_filter = ''
    params = []
    if date and date != 'all':
        date_filter = 'WHERE dna.date_detected = ?'
        params.append(date)

    # 新品类别分布（前10）- 基于daily_new_arrivals，可过滤日期
    category_sql = f'''
        SELECT p.item_type, COUNT(*) as count
        FROM daily_new_arrivals dna
        LEFT JOIN products p ON dna.tcin = p.tcin
        {date_filter}
        AND p.item_type IS NOT NULL AND p.item_type != ''
        GROUP BY p.item_type
        ORDER BY count DESC
        LIMIT 10
    '''
    category_stats = conn.execute(category_sql, params).fetchall()

    # 新品品牌分布 - 基于daily_new_arrivals，可过滤日期
    brand_sql = f'''
        SELECT dna.brand, COUNT(*) as count
        FROM daily_new_arrivals dna
        {date_filter}
        GROUP BY dna.brand
    '''
    brand_stats = conn.execute(brand_sql, params).fetchall()

    # 获取选中日期的商品列表TCIN（用于过滤展示）
    product_tcins = []
    if date and date != 'all':
        rows = conn.execute('SELECT tcin FROM daily_new_arrivals WHERE date_detected = ?', (date,)).fetchall()
        product_tcins = [r['tcin'] for r in rows]

    conn.close()

    return jsonify({
        'daily_counts': [dict(row) for row in daily_counts],
        'category_stats': [dict(row) for row in category_stats],
        'brand_stats': [dict(row) for row in brand_stats],
        'product_tcins': product_tcins
    })

@app.route('/images/<path:filename>')
def serve_image(filename):
    """Serve product images"""
    return send_from_directory(IMAGE_FOLDER, filename)

@app.route('/import-data', methods=['POST'])
def import_data():
    """Import data from Excel/CSV files"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    try:
        # Read Excel file
        df = pd.read_excel(file.stream)

        conn = get_db_connection()
        cursor = conn.cursor()

        imported_count = 0
        for _, row in df.iterrows():
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO products
                    (tcin, title, brand, price, retail_price, original_price,
                     has_promotion, savings_amount, discount_percentage, max_discount,
                     is_clearance, material, sales_count, delivery_date, is_new,
                     rating, review_count, secondary_ratings, color_summary, color,
                     size_summary, bullet_points, image_url, product_url, item_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('TCIN', '')),
                    row.get('名称', ''),
                    row.get('品牌', ''),
                    float(row.get('价格', 0)) if pd.notna(row.get('价格')) else None,
                    float(row.get('零售价', 0)) if pd.notna(row.get('零售价')) else None,
                    float(row.get('原价', 0)) if pd.notna(row.get('原价')) else None,
                    row.get('促销活动', ''),
                    row.get('节省金额', ''),
                    row.get('折扣比例', ''),
                    float(row.get('最大折扣', 0)) if pd.notna(row.get('最大折扣')) else None,
                    row.get('清仓状态', ''),
                    row.get('材质(面料)', ''),
                    int(row.get('购买人数', 0)) if pd.notna(row.get('购买人数')) else None,
                    row.get('预计送达', ''),
                    row.get('商品标签', ''),
                    float(row.get('评分', 0)) if pd.notna(row.get('评分')) else None,
                    int(row.get('评论数量', 0)) if pd.notna(row.get('评论数量')) else None,
                    row.get('次要评分', ''),
                    row.get('颜色汇总', ''),
                    row.get('颜色', ''),
                    row.get('尺码汇总', ''),
                    row.get('商品要点', ''),
                    row.get('图片链接', ''),
                    row.get('购买链接', ''),
                    row.get('商品分类', '')
                ))
                imported_count += 1
            except Exception as e:
                print(f"Error importing row: {e}")
                continue

        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Successfully imported {imported_count} products'
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='0.0.0.0', port=5000)
