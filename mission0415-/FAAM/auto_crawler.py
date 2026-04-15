"""
FAAM 自动化爬虫系统
每天北京时间18:30自动执行爬取任务
"""
import sys
import os
import time
import random
import urllib.parse
import re
import html
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import logging
import traceback

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==================== 日志配置 ====================
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f'crawler_{datetime.now().strftime("%Y%m%d")}.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== 配置参数 ====================
VISITOR_ID = ''.join(random.choices('0123456789ABCDEF', k=32))
API_KEY = "9f36aeafbe60771e321a7cc95a78140772ab3e96"
REVIEW_API_KEY = "c6b68aaef0eac4df4931aae70500b7056531cb37"
TARGET_STORE_ID = "1121"
TARGET_ZIP_CODE = "95628"
REQUESTS_TIMEOUT = 15
MAX_WORKERS_LISTING = 28
MAX_WORKERS_DETAIL = 28
MAX_API_OFFSET_LIMIT = 1199

# FAAM目标品牌
FAAM_BRANDS = ['A New Day', 'Wild Fable']

# 输出目录
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Session配置
session = requests.Session()
retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[400, 403, 429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.target.com/",
})

BASE_API_URL = f"https://redsky.target.com/redsky_aggregations/v1/web/plp_search_v1?category=5xtd3&count=24&default_purchasability_filter=true&include_sponsored=true&offset=0&page=%2Fc%2F5xtd3&platform=desktop&pricing_store_id=1121&spellcheck=true&store_ids=1121&visitor_id={VISITOR_ID}&zip=95628&key={API_KEY}&channel=WEB"

class FAAMCrawler:
    """FAAM专用爬虫"""

    def __init__(self):
        self.parsed_url = urllib.parse.urlparse(BASE_API_URL)
        self.base_query_params = urllib.parse.parse_qs(self.parsed_url.query)
        self.all_products = []
        self.all_reviews = []

    def fetch_json(self, url):
        try:
            resp = session.get(url, timeout=15, verify=False)
            if resp.status_code == 400:
                return {"_error": 400}
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"请求失败: {url}, 错误: {e}")
            return {}

    def extract_cat_id(self, path):
        match = re.search(r'N-([a-zA-Z0-9]+)', path)
        return match.group(1) if match else None

    def _build_url(self, params):
        return urllib.parse.urlunparse((
            self.parsed_url.scheme, self.parsed_url.netloc, self.parsed_url.path,
            self.parsed_url.params, urllib.parse.urlencode(params, doseq=True), self.parsed_url.fragment
        ))

    def get_brands_for_category(self, cat_id="5xtd3"):
        """获取指定分类下的所有品牌"""
        logger.info(f"正在获取分类 {cat_id} 下的品牌列表...")
        params = self.base_query_params.copy()
        params.update({'page': [f"/c/{cat_id}"], 'category': [cat_id], 'offset': ['0']})
        url = self._build_url(params)
        data = self.fetch_json(url)

        brand_list = []
        resp = data.get("data", {}).get("search", {}).get("search_response", {}) or \
               data.get("data", {}).get("search_response", {})

        for facet in resp.get("facet_list", []):
            if facet.get("facet_id") == "d_brand_all":
                for option in facet.get("options", []):
                    name = option.get("display_name", "").strip()
                    path = option.get("url", "").strip()
                    if name in FAAM_BRANDS:
                        brand_list.append({"brand_name": name, "brand_path": path, "cat_id": cat_id})
                        logger.info(f"找到目标品牌: {name}")

        return brand_list

    def crawl_brand_products(self, brand_info):
        """爬取指定品牌的所有商品"""
        brand_name = brand_info['brand_name']
        brand_cat_id = self.extract_cat_id(brand_info['brand_path'])
        if not brand_cat_id:
            logger.warning(f"无法提取品牌ID: {brand_name}")
            return []

        logger.info(f"开始爬取品牌: {brand_name} (ID: {brand_cat_id})")
        products = []
        offset = 0

        while offset <= MAX_API_OFFSET_LIMIT:
            params = self.base_query_params.copy()
            params.update({
                'page': [f"/c/{brand_cat_id}"],
                'category': [brand_cat_id],
                'offset': [str(offset)]
            })
            url = self._build_url(params)
            data = self.fetch_json(url)

            if not data or data.get("_error") == 400:
                break

            prods = data.get("data", {}).get("search", {}).get("products", [])
            if not prods:
                break

            for p in prods:
                tcin = p.get("tcin") or p.get("parent", {}).get("tcin")
                if tcin:
                    products.append({
                        'tcin': tcin,
                        'brand': brand_name,
                        'plp_data': p
                    })

            if len(prods) < 24:
                break

            offset += 24
            time.sleep(random.uniform(0.5, 1.5))

        logger.info(f"品牌 {brand_name} 爬取完成,共 {len(products)} 个商品")
        return products

    def process_single_product(self, product_info):
        """处理单个商品,获取详细信息"""
        tcin = product_info['tcin']
        brand = product_info['brand']
        plp_data = product_info.get('plp_data', {})

        detail_url = (
            f"https://redsky.target.com/redsky_aggregations/v1/web/pdp_client_v1"
            f"?key={API_KEY}&tcin={tcin}&is_bot=false"
            f"&store_id={TARGET_STORE_ID}"
            f"&pricing_store_id={TARGET_STORE_ID}&has_pricing_store_id=true"
            f"&zip={TARGET_ZIP_CODE}"
            f"&visitor_id={VISITOR_ID}&skip_personalized=true"
            f"&channel=WEB&page=%2Fp%2FA-{tcin}"
        )

        try:
            resp = session.get(detail_url, timeout=20, verify=False)
            pdp_data = {}
            if resp.status_code == 200:
                pdp_data = resp.json().get("data", {}).get("product", {})

            if not pdp_data and plp_data:
                pdp_data = plp_data
            if not pdp_data:
                return None

            # 提取商品信息
            item = pdp_data.get("item", {})
            p_desc = item.get("product_description", {})
            p_enrich = item.get("enrichment", {})
            p_brand = item.get("primary_brand", {})

            title = self.clean_text(p_desc.get("title"))
            price_node = pdp_data.get('price', {})

            # 价格信息
            current_price = self.parse_price(price_node.get('formatted_current_price'))
            original_price = self.parse_price(price_node.get('formatted_comparison_price'))

            # 评分
            stats = pdp_data.get('ratings_and_reviews', {}).get('statistics', {}).get('rating', {})
            rating = float(stats.get('average', 0))
            review_count = int(stats.get('count', 0))

            # 判断是否新品
            is_new = 'No'
            ribbons = item.get('ribbons', [])
            for ribbon in ribbons:
                if isinstance(ribbon, str) and 'NEW' in ribbon.upper():
                    if not any(k in ribbon.upper() for k in ['COLOR', 'SIZE']):
                        is_new = 'Yes'
                        break

            # 颜色信息
            color_list, size_list = self.get_variation_from_json(pdp_data)
            color_summary = f"[共{len(color_list)}色] {', '.join(color_list)}" if color_list else ""
            size_summary = f"[共{len(size_list)}码] {', '.join(size_list)}" if size_list else "无尺码"

            # 图片链接
            image_url = p_enrich.get("images", {}).get("primary_image_url", "")
            buy_url = p_enrich.get("buy_url", f"https://www.target.com/p/-/A-{tcin}")

            return {
                'TCIN': tcin,
                '名称': title,
                '品牌': brand,
                '价格': current_price,
                '原价': original_price if original_price and original_price != current_price else None,
                '促销活动': 'Yes' if price_node.get('save_dollar') else 'No',
                '评分': rating if rating > 0 else None,
                '评论数量': review_count,
                '颜色汇总': color_summary,
                '颜色': ', '.join(color_list) if color_list else '',
                '尺码汇总': size_summary,
                '图片链接': image_url,
                '购买链接': buy_url,
                '商品标签': is_new,
                '清仓状态': 'Yes' if 'clearance' in str(price_node.get('formatted_current_price_type', '')).lower() else 'No',
                '预计送达': '待查询',
                '购买人数': 0 if review_count == 0 else None,
                '材质(面料)': '',
                '商品要点': '',
                '次要评分': '',
                '零售价': current_price,
                '节省金额': '',
                '折扣比例': '',
                '最大折扣': None,
                '商品分类': item.get("product_classification", {}).get("item_type", {}).get("name", "")
            }

        except Exception as e:
            logger.error(f"处理商品 {tcin} 时出错: {e}")
            return None

    def clean_text(self, text):
        if text is None:
            return None
        text = html.unescape(str(text))
        text = re.sub(r'<[^>]+>', '', text)
        return text.strip()

    def parse_price(self, price_str):
        if not price_str:
            return None
        try:
            clean_str = str(price_str).replace('$', '').replace(',', '').strip()
            if '-' in clean_str:
                parts = clean_str.split('-')
                return float(parts[0].strip())
            return float(clean_str)
        except:
            return None

    def get_variation_from_json(self, product_data):
        """从JSON中提取颜色和尺码"""
        c_set = set()
        s_set = set()
        sources = [product_data, product_data.get("parent", {})]

        for data in sources:
            if not data:
                continue
            var_summary = data.get("variation_summary", {})
            themes = var_summary.get("themes", [])
            for theme in themes:
                t_name = theme.get("name", "").lower()
                swatches = theme.get("swatches", [])
                values = [self.clean_text(s.get("value")) for s in swatches if s.get("value")]
                if "color" in t_name or "pattern" in t_name:
                    c_set.update(values)
                elif "size" in t_name:
                    s_set.update(values)

        return list(c_set), list(s_set)

    def run(self):
        """执行完整的爬取流程"""
        logger.info("="*60)
        logger.info("FAAM 自动化爬虫启动")
        logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*60)

        start_time = time.time()

        # 1. 获取品牌列表
        brands = self.get_brands_for_category()
        if not brands:
            logger.error("未找到目标品牌!")
            return False

        logger.info(f"找到 {len(brands)} 个目标品牌")

        # 2. 爬取各品牌商品列表
        all_tcin_list = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_LISTING) as executor:
            future_to_brand = {executor.submit(self.crawl_brand_products, b): b for b in brands}
            for future in as_completed(future_to_brand):
                brand = future_to_brand[future]
                try:
                    products = future.result()
                    all_tcin_list.extend(products)
                except Exception as e:
                    logger.error(f"爬取品牌 {brand['brand_name']} 时出错: {e}")

        logger.info(f"共找到 {len(all_tcin_list)} 个商品TCIN")

        if not all_tcin_list:
            logger.warning("未找到任何商品")
            return False

        # 去重
        seen_tcin = set()
        unique_products = []
        for p in all_tcin_list:
            if p['tcin'] not in seen_tcin:
                seen_tcin.add(p['tcin'])
                unique_products.append(p)

        logger.info(f"去重后: {len(unique_products)} 个商品")

        # 3. 获取商品详情
        logger.info("开始获取商品详情...")
        final_products = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_DETAIL) as executor:
            future_to_product = {executor.submit(self.process_single_product, p): p for p in unique_products}
            for i, future in enumerate(as_completed(future_to_product), 1):
                try:
                    result = future.result()
                    if result:
                        final_products.append(result)
                    if i % 50 == 0 or i == len(unique_products):
                        logger.info(f"进度: {i}/{len(unique_products)}")
                except Exception as e:
                    logger.error(f"处理商品时出错: {e}")

        logger.info(f"成功获取 {len(final_products)} 个商品详情")

        if not final_products:
            logger.error("没有成功获取任何商品详情")
            return False

        # 4. 保存为Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"FAAM_Data_{timestamp}.xlsx"
        filepath = os.path.join(OUTPUT_DIR, filename)

        df = pd.DataFrame(final_products)

        # 调整列顺序
        preferred_cols = [
            'TCIN', '名称', '品牌', '价格', '原价', '促销活动',
            '评分', '评论数量', '颜色', '颜色汇总', '尺码汇总',
            '商品标签', '清仓状态', '图片链接', '购买链接',
            '预计送达', '购买人数', '材质(面料)', '商品要点',
            '次要评分', '零售价', '节省金额', '折扣比例',
            '最大折扣', '商品分类'
        ]

        existing_cols = [c for c in preferred_cols if c in df.columns]
        other_cols = [c for c in df.columns if c not in preferred_cols]
        df = df[existing_cols + other_cols]

        df.to_excel(filepath, index=False, engine='openpyxl')
        logger.info(f"数据已保存到: {filepath}")

        elapsed = time.time() - start_time
        logger.info(f"爬取完成! 耗时: {elapsed:.2f}秒")
        logger.info(f"共爬取 {len(final_products)} 个商品")

        return True


if __name__ == "__main__":
    try:
        crawler = FAAMCrawler()
        success = crawler.run()
        if success:
            logger.info("✓ 爬取任务成功完成")
            sys.exit(0)
        else:
            logger.error("✗ 爬取任务失败")
            sys.exit(1)
    except Exception as e:
        logger.error(f"程序异常: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
