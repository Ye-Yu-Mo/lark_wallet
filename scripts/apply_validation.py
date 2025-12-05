#!/usr/bin/env python3
"""
应用校验结果
根据validation.csv中的action列批量更新记录
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


def apply_validation(
    account_name: str,
    validation_file: str,
    config_path: str = 'config.json',
    dry_run: bool = True
):
    """
    应用校验结果

    :param account_name: 账本名称
    :param validation_file: 校验结果CSV文件路径
    :param config_path: 配置文件路径
    :param dry_run: 是否仅预览
    """
    logger.info(f"应用校验结果: {validation_file}")

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

    # 读取校验结果
    updates = []
    skipped = 0

    with open(validation_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            record_id = row['record_id']
            action = row.get('action', '').strip().upper()

            # 只处理action为UPDATE的记录
            if action != 'UPDATE':
                skipped += 1
                continue

            predicted_purpose = row['predicted_purpose'].strip()
            predicted_subcat = row['predicted_subcat'].strip()

            fields = {}

            if predicted_purpose:
                fields['支出目的'] = predicted_purpose

            if predicted_subcat:
                fields['细类'] = predicted_subcat

            if fields:
                updates.append({
                    'record_id': record_id,
                    'fields': fields,
                    'note': row.get('note', ''),
                    'current_purpose': row.get('current_purpose', ''),
                    'current_subcat': row.get('current_subcat', '')
                })

    logger.info(f"读取到 {len(updates)} 条需要更新的记录，跳过 {skipped} 条")

    if not updates:
        logger.warning("没有需要更新的记录 (action列是否设置为UPDATE?)")
        return

    # 预览
    print(f"\n=== 预览更新 (前20条) ===\n")
    for i, update in enumerate(updates[:20], 1):
        print(f"{i}. 备注: {update['note']}")

        if '支出目的' in update['fields']:
            print(f"   支出目的: {update['current_purpose']} → {update['fields']['支出目的']}")

        if '细类' in update['fields']:
            print(f"   细类: {update['current_subcat']} → {update['fields']['细类']}")

        print()

    if len(updates) > 20:
        print(f"... 还有 {len(updates) - 20} 条记录\n")

    # 实际更新
    if dry_run:
        logger.info("这是预览模式 (dry_run=True)，不会实际更新数据")
        logger.info("如需实际更新，请使用 --no-dry-run 参数")
    else:
        confirm = input(f"\n确认更新 {len(updates)} 条记录? (y/N): ")
        if confirm.lower() != 'y':
            logger.info("取消更新")
            return

        logger.info("开始批量更新...")

        # 批量更新（每次100条）
        batch_size = 100
        success_count = 0
        failed_count = 0

        # 只提取record_id和fields
        update_records = [{'record_id': u['record_id'], 'fields': u['fields']} for u in updates]

        for i in range(0, len(update_records), batch_size):
            batch = update_records[i:i + batch_size]

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

        if failed_count > 0:
            logger.warning(f"有 {failed_count} 条记录更新失败，请检查日志")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='应用校验结果')
    parser.add_argument('account', choices=['jasxu', 'bendandan'], help='账本名称')
    parser.add_argument('validation_file', help='校验结果CSV文件路径')
    parser.add_argument('--no-dry-run', action='store_true', help='实际更新数据（默认为预览模式）')

    args = parser.parse_args()

    from core.logger import setup_logger
    setup_logger(level='INFO')

    apply_validation(
        account_name=args.account,
        validation_file=args.validation_file,
        dry_run=not args.no_dry_run
    )
