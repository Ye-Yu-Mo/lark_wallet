#!/usr/bin/env python3
"""
从飞书数据中提取分类规则映射表
基于已有数据学习规则，生成可编辑的CSV映射表
"""
import sys
from pathlib import Path
import csv
from collections import defaultdict

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from core.config import Config
from lib.feishu_client import FeishuClient


def extract_rules(
    account_name: str,
    output_file: str,
    config_path: str = 'config.json',
    min_count: int = 2,  # 最少出现次数
    max_rules: int = 500
):
    """
    从飞书数据提取规则映射表

    :param account_name: 账本名称
    :param output_file: 输出CSV文件路径
    :param config_path: 配置文件路径
    :param min_count: 规则最少出现次数
    :param max_rules: 最多提取多少条规则
    """
    logger.info(f"从账本 '{account_name}' 提取规则...")

    # 加载配置
    config = Config(config_path)
    accounts = config.data.get('accounts', {})

    if account_name not in accounts:
        logger.error(f"账本 '{account_name}' 不存在")
        return

    account_info = accounts[account_name]
    app_token = account_info['app_token']
    table_id = account_info['table_id']

    # 初始化飞书客户端
    mcp_config = config.data.get('mcp_server', {})
    feishu = FeishuClient(
        app_id=mcp_config['app_id'],
        app_secret=mcp_config['app_secret']
    )

    # 拉取所有记录
    logger.info("拉取数据...")
    all_records = []
    page_token = None

    while True:
        items, page_token, has_more = feishu.list_records(
            app_token=app_token,
            table_id=table_id,
            page_token=page_token,
            page_size=500
        )

        all_records.extend(items)

        if not has_more:
            break

    logger.info(f"拉取到 {len(all_records)} 条记录")

    # 统计规则
    # 使用 (备注关键词, 分类) 作为key，统计对应的 (支出目的, 细类) 的出现次数
    rule_stats = defaultdict(lambda: defaultdict(int))

    for record in all_records:
        fields = record.get('fields', {})

        # 只处理支出记录
        if fields.get('收支') != '支出':
            continue

        # 提取字段
        def parse_text(v):
            if isinstance(v, str):
                return v
            if isinstance(v, list) and v:
                return parse_text(v[0])
            if isinstance(v, dict):
                return str(v.get('text', ''))
            return str(v) if v else ''

        note_field = fields.get('备注', '')
        note = parse_text(note_field).strip()

        category = str(fields.get('分类', '')).strip()
        purpose = str(fields.get('支出目的', '')).strip()

        subcat_field = fields.get('细类')
        subcat = parse_text(subcat_field).strip()

        # 跳过不完整的记录
        if not note or not category or not purpose or not subcat:
            continue

        # 提取备注中的关键词（简化版：使用完整备注或前几个字）
        # 这里可以用更复杂的分词逻辑
        note_keywords = extract_keywords(note)

        for keyword in note_keywords:
            # 规则: (keyword, category) -> (purpose, subcat)
            rule_key = (keyword, category)
            output = (purpose, subcat)
            rule_stats[rule_key][output] += 1

    # 提取规则
    rules = []

    for (keyword, category), outputs in rule_stats.items():
        # 找出最常见的输出
        most_common = max(outputs.items(), key=lambda x: x[1])
        (purpose, subcat), count = most_common

        if count < min_count:
            continue

        # 计算置信度（该输出占所有输出的比例）
        total = sum(outputs.values())
        confidence = count / total

        rules.append({
            'keyword': keyword,
            'category': category,
            'purpose': purpose,
            'subcat': subcat,
            'confidence': confidence,
            'count': count,
            'total': total
        })

    # 按置信度和出现次数排序
    rules.sort(key=lambda x: (x['confidence'], x['count']), reverse=True)

    # 限制数量
    if len(rules) > max_rules:
        rules = rules[:max_rules]

    logger.info(f"提取到 {len(rules)} 条规则")

    # 导出到CSV
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
        fieldnames = [
            'keyword', 'category', 'purpose', 'subcat',
            'confidence', 'count', 'total', 'enabled', 'notes'
        ]

        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for rule in rules:
            writer.writerow({
                'keyword': rule['keyword'],
                'category': rule['category'],
                'purpose': rule['purpose'],
                'subcat': rule['subcat'],
                'confidence': f"{rule['confidence']:.2%}",
                'count': rule['count'],
                'total': rule['total'],
                'enabled': 'TRUE',  # 默认启用
                'notes': ''  # 供用户添加备注
            })

    logger.info(f"规则已导出到: {output_file}")

    print(f"\n✅ 已提取 {len(rules)} 条规则到: {output_file}")
    print("\n使用说明:")
    print("1. 在Excel/飞书表格中打开CSV文件")
    print("2. 检查规则的准确性:")
    print("   - keyword: 备注关键词")
    print("   - category: 分类")
    print("   - purpose: 支出目的")
    print("   - subcat: 细类")
    print("   - confidence: 置信度(该规则的准确率)")
    print("   - count: 出现次数")
    print("   - enabled: 是否启用(TRUE/FALSE)")
    print("3. 修改不准确的规则或禁用不需要的规则")
    print("4. 保存CSV文件")
    print(f"5. 使用规则填充: uv run scripts/fill_by_rules.py {account_name} {output_file}")


def extract_keywords(note: str) -> list:
    """
    从备注中提取关键词

    简化版实现:
    - 如果备注很短(<=4字)，使用完整备注
    - 否则提取前3-5个字符作为关键词
    - 也可以扩展为使用分词库
    """
    note = note.strip()

    if not note:
        return []

    keywords = []

    # 策略1: 完整备注(如果够短)
    if len(note) <= 6:
        keywords.append(note)

    # 策略2: 前N个字符
    for n in [3, 4, 5]:
        if len(note) >= n:
            keywords.append(note[:n])

    # 去重
    return list(set(keywords))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='从飞书数据提取规则映射表')
    parser.add_argument('account', choices=['jasxu', 'bendandan'], help='账本名称')
    parser.add_argument('--output', '-o', default='rules.csv', help='输出文件路径')
    parser.add_argument('--min-count', type=int, default=2, help='规则最少出现次数')
    parser.add_argument('--max-rules', type=int, default=500, help='最多提取多少条规则')

    args = parser.parse_args()

    from core.logger import setup_logger
    setup_logger(level='INFO')

    extract_rules(
        account_name=args.account,
        output_file=args.output,
        min_count=args.min_count,
        max_rules=args.max_rules
    )
