# FAAM - 女装竞品信息平台

专注追踪 Target 旗下 A New Day 和 Wild Fable 品牌的女装商品信息。

## 功能特点

- **商品查询**: 按品牌、新品状态、关键词搜索商品
- **每日新品**: 自动追踪并展示每日上架的新品
- **数据可视化**: 清晰的商品卡片展示,包含价格、评分、颜色等信息
- **数据导入**: 支持从Excel文件批量导入爬虫数据

## 项目结构

```
FAAM/
├── app.py              # Flask主应用
├── import_data.py      # 数据导入脚本
├── requirements.txt    # Python依赖
├── faam_products.db    # SQLite数据库(自动生成)
├── templates/          # HTML模板
│   ├── base.html
│   ├── index.html
│   ├── products.html
│   └── new_arrivals.html
├── static/
│   └── css/
│       └── style.css   # 样式文件
└── images/             # 商品图片目录
```

## 安装步骤

### 1. 安装Python依赖

```bash
pip install -r requirements.txt
```

### 2. 初始化数据库并导入数据

```bash
python import_data.py
```

按照提示输入包含商品数据的Excel文件所在目录。

### 3. 启动Web应用

```bash
python app.py
```

应用将在 http://localhost:5000 启动。

## 使用说明

### 首页
- 查看商品总数统计
- 查看各品牌商品数量
- 浏览今日新品
- 快速搜索商品

### 商品查询
- 按品牌筛选(A New Day / Wild Fable)
- 仅看新品
- 关键词搜索
- 分页浏览

### 每日新品
- 选择日期查看该日新品
- 查看新品详细信息
- 跳转到Target官网查看

## 数据来源

本平台的商品数据来自 `0414代码.txt` 中的爬虫脚本,该脚本可以:
- 爬取Target网站的女装商品
- 提取商品详细信息(价格、评分、颜色、尺码等)
- 识别新品和清仓商品
- 下载商品图片

## 新品检测标准

根据 `新品代码.txt` 的逻辑,新品需要满足:
1. 评论数量为0
2. 购买人数为0
3. 预计送达时间 >= 8天
4. 带有"New at"标签
5. 通过QA和PDP验证

## 技术栈

- **后端**: Flask (Python Web框架)
- **数据库**: SQLite
- **前端**: HTML5 + CSS3 (原生,无框架)
- **数据处理**: Pandas

## 注意事项

1. 首次使用前必须运行 `import_data.py` 导入数据
2. 确保Excel文件格式正确,包含所需的列名
3. 商品图片需要手动下载到 `images/` 目录,或使用图片URL
4. 建议定期更新数据以保持信息时效性

## 许可证

本项目仅供学习和研究使用。
