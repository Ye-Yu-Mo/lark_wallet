#!/usr/bin/env python3
"""
校验规则映射表的准确性
对比现有数据和规则预测，导出不一致的记录供审核
"""
import sys
from pathlib import Path
import csv

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from core.config import Config
from lib.feishu_client import FeishuClient


class RuleMatcher:
    """规则匹配器"""

    def __init__(self, rules_file: str):
        self.rules = []
        self.load_rules(rules_file)

    def load_rules(self, rules_file: str):
        """从CSV加载规则"""
        with open(rules_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)

            for row in reader:
                # 跳过禁用的规则
                if row.get('enabled', 'TRUE').upper() != 'TRUE':
                    continue

                self.rules.append({
                    'keyword': row['keyword'],
                    'category': row['category'],
                    'purpose': row['purpose'],
                    'subcat': row['subcat'],
                })

        logger.info(f"加载了 {len(self.rules)} 条启用的规则")

    def match(self, note: str, category: str):
        """
        匹配规则

        :return: (purpose, subcat) or (None, None)
        """
        note = note.strip()
        category = category.strip()

        # 尝试匹配规则
        for rule in self.rules:
            # 检查分类是否匹配
            if rule['category'] != category:
                continue

            # 检查备注是否包含关键词
            if rule['keyword'] in note:
                return rule['purpose'], rule['subcat']

        return None, None


def validate_rules(
    account_name: str,
    rules_file: str,
    output_file: str,
    config_path: str = 'config.json',
    max_records: int = 2000,
    only_mismatch: bool = True
):
    """
    校验规则准确性

    :param account_name: 账本名称
    :param rules_file: 规则CSV文件路径
    :param output_file: 输出CSV文件路径
    :param config_path: 配置文件路径
    :param max_records: 最多检查多少条记录
    :param only_mismatch: 是否只导出不一致的记录
    """
    logger.info(f"校验规则表: {rules_file}")

    # 加载规则
    matcher = RuleMatcher(rules_file)

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

    # 拉取记录
    logger.info("拉取数据...")
    all_records = []
    page_token = None

    while len(all_records) < max_records:
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

    # 校验记录
    results = []
    match_count = 0
    mismatch_count = 0
    no_rule_count = 0

    for record in all_records:
        fields = record.get('fields', {})
        record_id = record.get('record_id')

        # 只处理支出
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
        current_purpose = str(fields.get('支出目的', '')).strip()

        subcat_field = fields.get('细类')
        current_subcat = parse_text(subcat_field).strip()

        # 处理日期
        date_field = fields.get('日期')
        if isinstance(date_field, (int, float)):
            from datetime import datetime
            date_str = datetime.fromtimestamp(date_field / 1000).strftime('%Y-%m-%d')
        else:
            date_str = ''

        # 处理金额
        amount = fields.get('金额', 0)

        if not note or not category:
            continue

        # 匹配规则
        predicted_purpose, predicted_subcat = matcher.match(note, category)

        # 判断是否一致
        purpose_match = (current_purpose == predicted_purpose) if predicted_purpose else None
        subcat_match = (current_subcat == predicted_subcat) if predicted_subcat else None

        # 统计
        if predicted_purpose is None and predicted_subcat is None:
            no_rule_count += 1
            status = 'NO_RULE'
        elif purpose_match and subcat_match:
            match_count += 1
            status = 'MATCH'
        else:
            mismatch_count += 1
            status = 'MISMATCH'

        # 记录结果
        result = {
            'record_id': record_id,
            'date': date_str,
            'amount': amount,
            'category': category,
            'note': note,
            'current_purpose': current_purpose,
            'current_subcat': current_subcat,
            'predicted_purpose': predicted_purpose or '',
            'predicted_subcat': predicted_subcat or '',
            'status': status,
            'action': ''  # 用户可以填写: UPDATE/IGNORE/DELETE_RULE
        }

        # 根据参数决定是否导出
        if only_mismatch:
            if status == 'MISMATCH':
                results.append(result)
        else:
            results.append(result)

    logger.info(f"校验完成: 匹配 {match_count}, 不匹配 {mismatch_count}, 无规则 {no_rule_count}")

    # 导出到CSV
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
        fieldnames = [
            'record_id', 'date', 'amount', 'category', 'note',
            'current_purpose', 'current_subcat',
            'predicted_purpose', 'predicted_subcat',
            'status', 'action'
        ]

        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for result in results:
            writer.writerow(result)

    logger.info(f"校验结果已导出到: {output_file}")

    print(f"\n✅ 校验完成")
    print(f"总记录数: {match_count + mismatch_count + no_rule_count}")
    print(f"匹配: {match_count}")
    print(f"不匹配: {mismatch_count}")
    print(f"无规则: {no_rule_count}")
    print(f"\n导出了 {len(results)} 条记录到: {output_file}")

    if only_mismatch:
        print("\n使用说明:")
        print("1. 在Excel中打开CSV文件")
        print("2. 检查不一致的记录:")
        print("   - current_*: 当前值")
        print("   - predicted_*: 规则预测值")
        print("3. 在 'action' 列填写操作:")
        print("   - UPDATE: 用规则值更新")
        print("   - IGNORE: 忽略此记录")
        print("   - DELETE_RULE: 删除对应规则")
        print("4. 保存CSV文件")
        print(f"5. 批量更新: uv run scripts/apply_validation.py {account_name} {output_file}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='校验规则映射表的准确性')
    parser.add_argument('account', choices=['jasxu', 'bendandan'], help='账本名称')
    parser.add_argument('rules', help='规则CSV文件路径')
    parser.add_argument('--output', '-o', default='validation.csv', help='输出文件路径')
    parser.add_argument('--max-records', type=int, default=2000, help='最多检查多少条记录')
    parser.add_argument('--all', action='store_true', help='导出所有记录（包括匹配的）')

    args = parser.parse_args()

    from core.logger import setup_logger
    setup_logger(level='INFO')

    validate_rules(
        account_name=args.account,
        rules_file=args.rules,
        output_file=args.output,
        max_records=args.max_records,
        only_mismatch=not args.all
    )
