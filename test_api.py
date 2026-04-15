#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Target API 诊断脚本 - 测试 API 连接状态
"""

import requests
import random
import time
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 当前使用的 API Key
CURRENT_API_KEY = "9f36aeafbe60771e321a7cc95a78140772ab3e96"

def generate_visitor_id():
    return ''.join(random.choices('0123456789ABCDEF', k=32))

def test_api_connection():
    print("="*70)
    print("🔍 Target API 连接诊断")
    print("="*70)
    
    visitor_id = generate_visitor_id()
    print(f"\n📝 测试 Visitor ID: {visitor_id}")
    print(f"🔑 当前 API Key: {CURRENT_API_KEY[:20]}...")
    
    # 测试 1: Target 首页（检查网络连通性）
    print("\n" + "="*70)
    print("测试 1: Target 首页连通性")
    print("="*70)
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    
    try:
        resp = session.get("https://www.target.com", timeout=15, verify=False)
        print(f"  状态码: {resp.status_code}")
        if resp.status_code == 200:
            print("  ✅ Target 首页可访问")
        else:
            print(f"  ⚠️  异常状态码: {resp.status_code}")
    except Exception as e:
        print(f"  ❌ 连接失败: {str(e)}")
        print("\n💡 网络问题，请检查：")
        print("   1. 网络连接是否正常")
        print("   2. 是否需要代理/VPN")
        print("   3. 防火墙设置")
        return
    
    # 测试 2: API 连接（使用当前 Key）
    print("\n" + "="*70)
    print("测试 2: PLP Search API 连接（当前 API Key）")
    print("="*70)
    
    test_url = f"https://redsky.target.com/redsky_aggregations/v1/web/plp_search_v2?category=5xtd3&count=24&default_purchasability_filter=true&include_sponsored=true&include_review_summarization=true&offset=0&page=%2Fc%2F5xtd3&platform=desktop&pricing_store_id=1121&spellcheck=true&store_ids=1121%2C267%2C311%2C1098%2C1502&visitor_id={visitor_id}&scheduled_delivery_store_id=1121&zip=95628&key={CURRENT_API_KEY}&channel=WEB&include_dmc_dmr=true"
    
    session2 = requests.Session()
    session2.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.target.com/",
    })
    
    retry = Retry(total=2, backoff_factor=1, status_forcelist=[403, 429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session2.mount("https://", adapter)
    
    try:
        print(f"  ⏳ 发送请求...")
        resp = session2.get(test_url, timeout=15, verify=False)
        print(f"  状态码: {resp.status_code}")
        print(f"  响应大小: {len(resp.content)} bytes")
        
        if resp.status_code == 200:
            print("  ✅ API 连接成功！")
            try:
                data = resp.json()
                if 'data' in data:
                    print("  📦 返回 JSON 数据")
                if 'search' in data.get('data', {}):
                    products = data['data']['search'].get('products', [])
                    print(f"  🔍 获取到 {len(products)} 个商品")
            except:
                print("  📄 返回非 JSON 数据")
        elif resp.status_code == 403:
            print("  ❌ 403 禁止访问")
            print("\n" + "!"*70)
            print("⚠️  API Key 已失效！需要获取新的 Key")
            print("!"*70)
            print("\n💡 获取新 API Key 的步骤：")
            print("   1. 打开浏览器，访问 https://www.target.com")
            print("   2. 按 F12 打开开发者工具")
            print("   3. 切换到 Network（网络）标签")
            print("   4. 在 Target 搜索任意商品（如 'dress'）")
            print("   5. 找到名为 'plp_search_v2' 的请求")
            print("   6. 复制 Request URL 中的 key= 后面的值")
            print("   7. 替换代码中的 API_KEY 变量")
        elif resp.status_code == 429:
            print("  ⚠️  429 请求过多（被限流）")
            print("  💡 等待几分钟后重试，或更换 IP")
        else:
            print(f"  ⚠️  其他状态码: {resp.status_code}")
            
    except Exception as e:
        print(f"  ❌ 请求失败: {str(e)}")
    
    # 测试 3: PDP API
    print("\n" + "="*70)
    print("测试 3: PDP Client API 连接（商品详情）")
    print("="*70)
    
    pdp_url = f"https://redsky.target.com/redsky_aggregations/v1/web/pdp_client_v1?key={CURRENT_API_KEY}&tcin=88722988&is_bot=false&store_id=1121&pricing_store_id=1121&has_pricing_store_id=true&zip=95628&has_financing_options=true&include_obsolete=true&visitor_id={visitor_id}&skip_personalized=true&skip_variation_hierarchy=true&channel=WEB"
    
    try:
        print(f"  ⏳ 发送请求...")
        resp = session2.get(pdp_url, timeout=15, verify=False)
        print(f"  状态码: {resp.status_code}")
        
        if resp.status_code == 200:
            print("  ✅ PDP API 连接成功！")
        elif resp.status_code == 403:
            print("  ❌ 403 禁止访问（API Key 失效）")
        else:
            print(f"  ⚠️  状态码: {resp.status_code}")
            
    except Exception as e:
        print(f"  ❌ 请求失败: {str(e)}")
    
    print("\n" + "="*70)
    print("📋 诊断总结")
    print("="*70)
    print("""
如果测试 1 成功但测试 2/3 返回 403：
  → API Key 已失效，需要获取新的 Key
  
如果测试 1 也失败：
  → 网络问题，检查：
    1. 网络连接是否正常
    2. 是否需要代理/VPN（Target 可能封锁了你的 IP）
    3. 防火墙/杀毒软件设置
    
如果测试 2 返回 429：
  → 请求过多被限流，等待后重试
    """)

if __name__ == "__main__":
    test_api_connection()
