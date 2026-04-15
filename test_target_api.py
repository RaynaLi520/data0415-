#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Target API 诊断脚本
用于测试 API 连接、验证 API Key 是否有效
"""

import requests
import random
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 旧的 API Key（可能已失效）
OLD_API_KEY = "9f36aeafbe60771e321a7cc95a78140772ab3e96"
REVIEW_API_KEY = "c6b68aaef0eac4df4931aae70500b7056531cb37"

# 生成新的 Visitor ID
def generate_visitor_id():
    return ''.join(random.choices('0123456789ABCDEF', k=32))

# 测试不同的 User-Agent
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def test_target_api():
    """测试 Target API 连接"""
    print("="*60)
    print("🔍 Target API 诊断测试")
    print("="*60)
    
    visitor_id = generate_visitor_id()
    print(f"\n📝 生成的 Visitor ID: {visitor_id}")
    
    # 测试 URL
    test_urls = [
        {
            "name": "PLP Search API (旧 Key)",
            "url": f"https://redsky.target.com/redsky_aggregations/v1/web/plp_search_v2?category=5xtd3&count=24&default_purchasability_filter=true&include_sponsored=true&include_review_summarization=true&offset=0&page=%2Fc%2F5xtd3&platform=desktop&pricing_store_id=1121&spellcheck=true&store_ids=1121%2C267%2C311%2C1098%2C1502&visitor_id={visitor_id}&scheduled_delivery_store_id=1121&zip=95628&key={OLD_API_KEY}&channel=WEB&include_dmc_dmr=true",
        },
        {
            "name": "PDP Client API (旧 Key)",
            "url": f"https://redsky.target.com/redsky_aggregations/v1/web/pdp_client_v1?key={OLD_API_KEY}&tcin=88722988&is_bot=false&store_id=1121&pricing_store_id=1121&has_pricing_store_id=true&zip=95628&has_financing_options=true&include_obsolete=true&visitor_id={visitor_id}&skip_personalized=true&skip_variation_hierarchy=true&channel=WEB",
        },
        {
            "name": "Target 首页 (测试网络连通性)",
            "url": "https://www.target.com",
        },
        {
            "name": "Target 商品页面 (测试浏览器访问)",
            "url": "https://www.target.com/p/women-s-short-sleeve-v-neck-t-shirt-universal-thread-82812156/A-88722988",
        },
    ]
    
    for idx, test in enumerate(test_urls, 1):
        print(f"\n{'='*60}")
        print(f"测试 {idx}: {test['name']}")
        print(f"{'='*60}")
        
        for ua_idx, ua in enumerate(USER_AGENTS[:2], 1):  # 只测试前2个 UA
            print(f"\n  User-Agent {ua_idx}: {ua[:50]}...")
            
            session = requests.Session()
            session.headers.update({
                "User-Agent": ua,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            })
            
            if "target.com/p/" in test['url']:
                session.headers.update({
                    "Referer": "https://www.target.com/",
                    "Upgrade-Insecure-Requests": "1",
                })
            else:
                session.headers.update({
                    "Referer": "https://www.target.com/",
                })
            
            retry = Retry(
                total=2,
                backoff_factor=1,
                status_forcelist=[403, 429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("https://", adapter)
            
            try:
                print(f"  ⏳ 发送请求...")
                resp = session.get(test['url'], timeout=15, verify=False)
                
                print(f"  📊 状态码: {resp.status_code}")
                print(f"  📏 响应大小: {len(resp.content)} bytes")
                
                if resp.status_code == 200:
                    print(f"  ✅ 请求成功！")
                    try:
                        data = resp.json()
                        print(f"  📦 返回 JSON 数据")
                        if 'data' in data:
                            print(f"   包含 'data' 字段")
                        if 'search' in data.get('data', {}):
                            print(f"  🔍 包含搜索结果")
                        if 'products' in data.get('data', {}).get('search', {}):
                            products = data['data']['search']['products']
                            print(f"  📦 获取到 {len(products)} 个商品")
                    except:
                        print(f"  📄 返回 HTML 页面")
                        
                elif resp.status_code == 403:
                    print(f"  ❌ 403 禁止访问 - 可能被反爬拦截")
                    print(f"  💡 建议：")
                    print(f"     1. 检查 API Key 是否失效")
                    print(f"     2. 尝试使用代理 IP")
                    print(f"     3. 降低请求频率")
                    
                elif resp.status_code == 429:
                    print(f"  ⚠️  429 请求过多 - 被限流")
                    print(f"  💡 建议：等待几分钟后重试")
                    
                else:
                    print(f"  ⚠️  其他状态码: {resp.status_code}")
                    
            except requests.exceptions.SSLError as e:
                print(f"  ❌ SSL 错误: {str(e)}")
            except requests.exceptions.ConnectionError as e:
                print(f"  ❌ 连接错误: {str(e)}")
            except requests.exceptions.Timeout as e:
                print(f"  ❌ 请求超时: {str(e)}")
            except Exception as e:
                print(f"  ❌ 未知错误: {str(e)}")
            
            # 间隔 2 秒
            time.sleep(2)
    
    print(f"\n{'='*60}")
    print("📋 诊断建议")
    print(f"{'='*60}")
    print("""
如果所有测试都返回 403：
1. 🔄 API Key 可能已失效，需要获取新的 Key
2. 🌐 你的 IP 可能被 Target 封锁，尝试：
   - 使用 VPN 更换 IP
   - 使用代理服务器
   - 等待 24 小时后重试
3. 🐢 降低请求频率：
   - 增加 MIN_REQUEST_INTERVAL 到 3-5 秒
   - 减少 MAX_WORKERS 到 5-10
4. 🎭 完善请求头：
   - 添加完整的浏览器指纹
   - 使用 Selenium/Playwright 模拟浏览器

如果 Target 首页能访问但 API 返回 403：
- 说明 API Key 已失效
- 需要抓包获取新的 API Key
    """)

if __name__ == "__main__":
    test_target_api()
