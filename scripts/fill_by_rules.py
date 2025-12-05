#!/usr/bin/env python3
"""
基于规则映射表填充字段
读取CSV规则表，自动填充飞书记录
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
                    'confidence': row.get('confidence', ''),
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


def fill_by_rules(
    account_name: str,
    rules_file: str,
    config_path: str = 'config.json',
    dry_run: bool = True,
    max_fill: int = 500,
    overwrite: bool = False
):
    """
    基于规则映射表填充字段

    :param account_name: 账本名称
    :param rules_file: 规则CSV文件路径
    :param config_path: 配置文件路径
    :param dry_run: 是否仅预览
    :param max_fill: 最多填充多少条
    :param overwrite: 是否覆盖已有值
    """
    logger.info(f"使用规则表填充: {rules_file}")

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

    while len(all_records) < max_fill * 2:
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

    # 筛选需要填充的记录
    to_fill = []

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

        if not note or not category:
            continue

        # 判断是否需要填充
        need_fill = False

        if overwrite:
            # 覆盖模式：所有记录都尝试填充
            need_fill = True
        else:
            # 默认模式：只填充空字段
            if not current_purpose or not current_subcat:
                need_fill = True

        if not need_fill:
            continue

        to_fill.append({
            'record_id': record_id,
            'note': note,
            'category': category,
            'current_purpose': current_purpose,
            'current_subcat': current_subcat
        })

        if len(to_fill) >= max_fill:
            break

    logger.info(f"找到 {len(to_fill)} 条记录需要填充")

    # 匹配规则并填充
    updates = []
    preview_data = []
    no_match_count = 0

    for item in to_fill:
        # 匹配规则
        purpose, subcat = matcher.match(item['note'], item['category'])

        if not purpose and not subcat:
            no_match_count += 1
            continue

        new_fields = {}

        # 根据模式决定是否更新
        if overwrite:
            if purpose:
                new_fields['支出目的'] = purpose
            if subcat:
                new_fields['细类'] = subcat
        else:
            if purpose and not item['current_purpose']:
                new_fields['支出目的'] = purpose
            if subcat and not item['current_subcat']:
                new_fields['细类'] = subcat

        if not new_fields:
            continue

        updates.append({
            'record_id': item['record_id'],
            'fields': new_fields
        })

        preview_data.append({
            'note': item['note'],
            'category': item['category'],
            'current_purpose': item['current_purpose'],
            'current_subcat': item['current_subcat'],
            'new_purpose': new_fields.get('支出目的'),
            'new_subcat': new_fields.get('细类')
        })

    logger.info(f"共需要更新 {len(updates)} 条记录 (无匹配规则: {no_match_count})")

    # 预览
    print("\n=== 预览填充结果 (前30条) ===\n")
    for i, item in enumerate(preview_data[:30], 1):
        print(f"{i}. 备注: {item['note']} | 分类: {item['category']}")

        if item['new_purpose']:
            current = item['current_purpose'] or '(空)'
            print(f"   支出目的: {current} → {item['new_purpose']}")

        if item['new_subcat']:
            current = item['current_subcat'] or '(空)'
            print(f"   细类: {current} → {item['new_subcat']}")

        print()

    if len(preview_data) > 30:
        print(f"... 还有 {len(preview_data) - 30} 条记录\n")

    if no_match_count > 0:
        print(f"⚠️  有 {no_match_count} 条记录没有匹配到规则\n")

    # 实际更新
    if dry_run:
        logger.info("这是预览模式 (dry_run=True)，不会实际更新数据")
        logger.info("如需实际更新，请使用 --no-dry-run 参数")
    else:
        if not updates:
            logger.info("没有需要更新的记录")
            return

        confirm = input(f"\n确认更新 {len(updates)} 条记录? (y/N): ")
        if confirm.lower() != 'y':
            logger.info("取消更新")
            return

        logger.info("开始批量更新...")

        # 批量更新（每次100条）
        batch_size = 100
        success_count = 0
        failed_count = 0

        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]

            try:
                result = feishu.batch_update_records(
                    app_token=app_token,
                    table_id=table_id,
                    records=batch
                )

                success_count += result.get('success', 0)
                failed_count += result.get('failed', 0)

                logger.info(f"批次 {i//batch_size + 1}: 成功 {result.get('success', 0)}, 失败 {result.get('failed', 0)}")

            except Exception as e:
                logger.error(f"批次 {i//batch_size + 1} 更新失败: {e}")
                failed_count += len(batch)

        logger.info(f"更新完成: 成功 {success_count}, 失败 {failed_count}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='基于规则映射表填充字段')
    parser.add_argument('account', choices=['jasxu', 'bendandan'], help='账本名称')
    parser.add_argument('rules', help='规则CSV文件路径')
    parser.add_argument('--no-dry-run', action='store_true', help='实际更新数据（默认为预览模式）')
    parser.add_argument('--max-fill', type=int, default=500, help='最多填充多少条记录')
    parser.add_argument('--overwrite', action='store_true', help='覆盖已有值（默认只填充空字段）')

    args = parser.parse_args()

    from core.logger import setup_logger
    setup_logger(level='INFO')

    fill_by_rules(
        account_name=args.account,
        rules_file=args.rules,
        dry_run=not args.no_dry_run,
        max_fill=args.max_fill,
        overwrite=args.overwrite
    )
