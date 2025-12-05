#!/usr/bin/env python3
"""
基于飞书表的规则校验工作流程

1. 将校验结果推送到飞书审核表
2. 在飞书中手动审核和修正
3. 从审核表读取修正后的数据并同步回原表
"""
import sys
from pathlib import Path
import time

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from core.config import Config
from lib.feishu_client import FeishuClient


class FeishuReviewWorkflow:
    """飞书审核工作流程"""

    def __init__(self, config: Config, account_name: str):
        self.config = config
        self.account_name = account_name
        self.account_info = config.data['accounts'][account_name]

        mcp_config = config.data.get('mcp_server', {})
        self.feishu = FeishuClient(
            app_id=mcp_config['app_id'],
            app_secret=mcp_config['app_secret']
        )

        self.app_token = self.account_info['app_token']
        self.table_id = self.account_info['table_id']
        self.review_table_id = self.account_info.get('review_table_id', '')

    def ensure_review_table(self):
        """确保审核表存在，如果不存在则创建"""
        if self.review_table_id:
            logger.info(f"使用已配置的审核表: {self.review_table_id}")
            return self.review_table_id

        # 创建审核表
        logger.info("创建审核表...")

        table_name = f"{self.account_info.get('name', self.account_name)}_分类审核"

        # 定义审核表字段
        fields = [
            {
                "field_name": "记录ID",
                "type": 1,  # 多行文本
            },
            {
                "field_name": "日期",
                "type": 5,  # 日期
            },
            {
                "field_name": "金额",
                "type": 2,  # 数字
            },
            {
                "field_name": "分类",
                "type": 1,  # 多行文本
            },
            {
                "field_name": "备注",
                "type": 1,  # 多行文本
            },
            {
                "field_name": "当前支出目的",
                "type": 1,  # 多行文本
            },
            {
                "field_name": "当前细类",
                "type": 1,  # 多行文本
            },
            {
                "field_name": "建议支出目的",
                "type": 1,  # 多行文本
            },
            {
                "field_name": "建议细类",
                "type": 1,  # 多行文本
            },
            {
                "field_name": "最终支出目的",
                "type": 1,  # 多行文本
            },
            {
                "field_name": "最终细类",
                "type": 1,  # 多行文本
            },
            {
                "field_name": "状态",
                "type": 3,  # 单选
                "property": {
                    "options": [
                        {"name": "待审核"},
                        {"name": "已确认"},
                        {"name": "已同步"},
                        {"name": "忽略"}
                    ]
                }
            }
        ]

        try:
            # 调用飞书API创建表
            result = self.feishu.create_table(
                app_token=self.app_token,
                table_name=table_name,
                default_view_name="全部记录",
                fields=fields
            )

            review_table_id = result.get('table_id')
            logger.info(f"审核表创建成功: {review_table_id}")

            # 更新配置
            self.review_table_id = review_table_id
            self.account_info['review_table_id'] = review_table_id

            # 保存配置
            self.config.save()

            return review_table_id

        except Exception as e:
            logger.error(f"创建审核表失败: {e}")
            raise

    def push_to_review(self, rules_file: str, max_records: int = 2000):
        """将校验结果推送到飞书审核表"""
        from scripts.validate_rules import RuleMatcher

        logger.info("开始校验规则...")

        # 加载规则
        matcher = RuleMatcher(rules_file)

        # 确保审核表存在
        review_table_id = self.ensure_review_table()

        # 拉取记录
        logger.info("拉取数据...")
        all_records = []
        page_token = None

        while len(all_records) < max_records:
            items, page_token, has_more = self.feishu.list_records(
                app_token=self.app_token,
                table_id=self.table_id,
                page_token=page_token,
                page_size=500
            )

            all_records.extend(items)

            if not has_more:
                break

        logger.info(f"拉取到 {len(all_records)} 条记录")

        # 校验并准备推送
        to_push = []
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
            date_ts = date_field if isinstance(date_field, (int, float)) else None

            # 处理金额 - 确保是数字类型
            amount_field = fields.get('金额', 0)
            try:
                amount = float(amount_field) if amount_field else 0.0
            except (ValueError, TypeError):
                amount = 0.0

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
                status = '待审核'  # 无规则，需要人工确认

                # 推送到审核表
                to_push.append({
                    '记录ID': record_id,
                    '日期': date_ts,
                    '金额': amount,
                    '分类': category,
                    '备注': note,
                    '当前支出目的': current_purpose,
                    '当前细类': current_subcat,
                    '建议支出目的': '',
                    '建议细类': '',
                    '最终支出目的': current_purpose,
                    '最终细类': current_subcat,
                    '状态': status
                })

            elif purpose_match and subcat_match:
                match_count += 1
                # 匹配，不推送

            else:
                mismatch_count += 1
                status = '待审核'

                # 不一致，推送到审核表
                to_push.append({
                    '记录ID': record_id,
                    '日期': date_ts,
                    '金额': amount,
                    '分类': category,
                    '备注': note,
                    '当前支出目的': current_purpose,
                    '当前细类': current_subcat,
                    '建议支出目的': predicted_purpose or '',
                    '建议细类': predicted_subcat or '',
                    '最终支出目的': predicted_purpose or current_purpose,
                    '最终细类': predicted_subcat or current_subcat,
                    '状态': status
                })

        logger.info(f"校验完成: 匹配 {match_count}, 不匹配 {mismatch_count}, 无规则 {no_rule_count}")
        logger.info(f"需要推送到审核表: {len(to_push)} 条记录")

        if not to_push:
            logger.info("所有记录都已匹配，无需审核")
            return

        # 批量推送到审核表
        logger.info("推送记录到审核表...")

        batch_size = 100
        success_count = 0
        failed_count = 0

        for i in range(0, len(to_push), batch_size):
            batch = to_push[i:i + batch_size]

            try:
                # 批量创建记录
                records = [{'fields': item} for item in batch]

                # 记录第一条数据用于调试
                if i == 0:
                    logger.debug(f"第一条记录数据: {records[0]}")

                result = self.feishu.batch_create_records(
                    app_token=self.app_token,
                    table_id=review_table_id,
                    records=records
                )

                # 检查实际结果
                if result.get('failed', 0) > 0:
                    logger.warning(f"批次 {i//batch_size + 1}: 有 {result['failed']} 条失败")
                    logger.warning(f"错误详情: {result.get('errors', [])}")
                    failed_count += result['failed']
                    success_count += result.get('success', 0)
                else:
                    success_count += len(batch)
                    logger.info(f"批次 {i//batch_size + 1}: 成功推送 {len(batch)} 条")

                time.sleep(0.2)  # 避免API限流

            except Exception as e:
                logger.error(f"批次 {i//batch_size + 1} 推送失败: {e}")
                failed_count += len(batch)

        logger.info(f"推送完成: 成功 {success_count}, 失败 {failed_count}")

        # 验证是否真的写入
        logger.info("验证写入结果...")
        verify_records, _, _ = self.feishu.list_records(
            app_token=self.app_token,
            table_id=review_table_id,
            page_size=10
        )
        actual_count = len(verify_records) if verify_records else 0
        logger.info(f"实际写入: {actual_count} 条记录")

        print(f"\n✅ 已推送 {success_count} 条记录到飞书审核表 (实际写入: {actual_count})")

        if actual_count == 0 and success_count > 0:
            print("\n⚠️  警告: API返回成功但表中没有数据，可能是字段格式问题")
            print("请检查:")
            print("1. 字段名是否与飞书表格中的字段名完全一致")
            print("2. 字段值类型是否正确（日期是时间戳，数字是number，单选是字符串）")

        print(f"\n请在飞书中审核:")
        print(f"1. 检查'建议支出目的'和'建议细类'")
        print(f"2. 在'最终支出目的'和'最终细类'中填写最终值")
        print(f"3. 将'状态'改为'已确认'")
        print(f"4. 运行同步命令更新回原表")

    def sync_from_review(self, dry_run: bool = True):
        """从审核表同步回原表"""
        if not self.review_table_id:
            logger.error("未配置审核表ID")
            return

        logger.info("从审核表读取数据...")

        # 拉取审核表中"已确认"的记录
        all_records = []
        page_token = None

        while True:
            items, page_token, has_more = self.feishu.list_records(
                app_token=self.app_token,
                table_id=self.review_table_id,
                page_token=page_token,
                page_size=500
            )

            all_records.extend(items)

            if not has_more:
                break

        logger.info(f"读取到 {len(all_records)} 条审核记录")

        # 筛选"已确认"状态的记录
        updates = []

        for record in all_records:
            fields = record.get('fields', {})
            review_record_id = record.get('record_id')

            # 提取字段
            def parse_text(v):
                if isinstance(v, str):
                    return v
                if isinstance(v, list) and v:
                    return parse_text(v[0])
                if isinstance(v, dict):
                    return str(v.get('text', ''))
                return str(v) if v else ''

            status = parse_text(fields.get('状态', '')).strip()

            # 只处理"已确认"状态
            if status != '已确认':
                continue

            original_record_id = parse_text(fields.get('记录ID', '')).strip()
            final_purpose = parse_text(fields.get('最终支出目的', '')).strip()
            final_subcat = parse_text(fields.get('最终细类', '')).strip()

            if not original_record_id:
                continue

            update_fields = {}
            if final_purpose:
                update_fields['支出目的'] = final_purpose
            if final_subcat:
                update_fields['细类'] = final_subcat

            if update_fields:
                updates.append({
                    'record_id': original_record_id,
                    'fields': update_fields,
                    'review_record_id': review_record_id
                })

        logger.info(f"找到 {len(updates)} 条需要同步的记录")

        if not updates:
            logger.info("没有需要同步的记录")
            return

        # 预览
        print(f"\n=== 预览同步 (前20条) ===\n")
        for i, update in enumerate(updates[:20], 1):
            print(f"{i}. Record ID: {update['record_id']}")
            for field, value in update['fields'].items():
                print(f"   → {field}: {value}")
            print()

        if len(updates) > 20:
            print(f"... 还有 {len(updates) - 20} 条记录\n")

        # 实际更新
        if dry_run:
            logger.info("这是预览模式 (dry_run=True)，不会实际更新数据")
            return

        logger.info("开始同步...")

        # 批量更新原表
        batch_size = 100
        success_count = 0
        review_updates = []

        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]

            try:
                # 更新原表
                records = [{'record_id': u['record_id'], 'fields': u['fields']} for u in batch]
                result = self.feishu.batch_update_records(
                    app_token=self.app_token,
                    table_id=self.table_id,
                    records=records
                )

                success_count += result.get('success', 0)
                logger.info(f"批次 {i//batch_size + 1}: 成功同步 {result.get('success', 0)} 条")

                # 记录需要更新状态的审核记录
                for u in batch:
                    review_updates.append({
                        'record_id': u['review_record_id'],
                        'fields': {'状态': '已同步'}
                    })

                time.sleep(0.2)

            except Exception as e:
                logger.error(f"批次 {i//batch_size + 1} 同步失败: {e}")

        # 更新审核表状态
        logger.info("更新审核表状态...")
        for i in range(0, len(review_updates), batch_size):
            batch = review_updates[i:i + batch_size]

            try:
                self.feishu.batch_update_records(
                    app_token=self.app_token,
                    table_id=self.review_table_id,
                    records=batch
                )
                time.sleep(0.2)
            except Exception as e:
                logger.error(f"更新审核表状态失败: {e}")

        logger.info(f"同步完成: {success_count}/{len(updates)}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='飞书表审核工作流程')
    parser.add_argument('account', choices=['jasxu', 'bendandan'], help='账本名称')
    parser.add_argument('action', choices=['push', 'sync'], help='操作: push=推送到审核表, sync=从审核表同步')
    parser.add_argument('--rules', help='规则文件路径 (push时必需)')
    parser.add_argument('--no-dry-run', action='store_true', help='实际执行 (sync时有效)')

    args = parser.parse_args()

    from core.logger import setup_logger
    setup_logger(level='INFO')

    config = Config('config.json')
    workflow = FeishuReviewWorkflow(config, args.account)

    if args.action == 'push':
        if not args.rules:
            parser.error("push操作需要提供 --rules 参数")
        workflow.push_to_review(args.rules)

    elif args.action == 'sync':
        workflow.sync_from_review(dry_run=not args.no_dry_run)
