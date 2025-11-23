"""飞书手动修改审计同步任务"""
import time
from typing import Dict, List, Optional
from loguru import logger

from core.config import Config
from core.database import AssetDB
from core.feishu_client import AssetFeishuClient


class FeishuChangeReviewTask:
    """同步 feishu_change_log 与飞书审核表"""

    DEFAULT_FIELDS = {
        'change_id': '变更ID',
        'record_id': '飞书记录ID',
        'table_name': '表名',
        'symbol': '资产代码',
        'change_type': '变更类型',
        'changed_fields': '变更字段',
        'detected_at': '检测时间',
        'status': '处理状态',
        'note': '备注',
        'resolved_at': '处理完成时间'
    }

    def __init__(self, config: Config):
        self.config = config
        asset_sync = config.get_asset_sync_config()
        feishu_cfg = config.get_feishu_config()
        review_cfg = asset_sync.get('feishu_change_review', {})

        self.enabled = review_cfg.get('enabled', False)
        if not self.enabled:
            self.review_table_id = None
            return

        table_id = review_cfg.get('table_id') or feishu_cfg['tables'].get('change_review')
        if not table_id:
            raise ValueError("feishu_change_review 缺少 table_id 配置")

        self.review_table_id = table_id
        self.batch_size = review_cfg.get('batch_size', 100)
        self.page_size = review_cfg.get('page_size', 200)
        self.pending_status = review_cfg.get('pending_status', '待确认')
        self.confirmed_status = review_cfg.get('confirmed_status', '已确认')
        self.resolved_status = review_cfg.get('resolved_status', '已同步')
        self.field_map = {**self.DEFAULT_FIELDS, **(review_cfg.get('field_mapping') or {})}

        db_cfg = config.get_database_config()
        self.db = AssetDB(db_cfg['path'])

        self.feishu = AssetFeishuClient(
            app_id=feishu_cfg['app_id'],
            app_secret=feishu_cfg['app_secret'],
            app_token=feishu_cfg['app_token'],
            table_ids=feishu_cfg['tables']
        )

    def sync(self) -> Dict:
        if not self.enabled:
            logger.info("飞书手动修改审计未启用, 跳过任务")
            return {'success': True, 'skipped': True}

        existing_records = self._load_review_records()
        pending_changes = self.db.get_pending_feishu_changes(self.batch_size)

        created = 0
        updated = 0
        resolved = 0

        for change in pending_changes:
            change_id = str(change['id'])
            payload = self._build_review_fields(change)
            review_entry = existing_records.get(change_id)

            if review_entry:
                # 用户已经修改状态, 不要覆盖
                status_field = self.field_map['status']
                current_status = self._extract_value(review_entry['fields'].get(status_field))
                if current_status and current_status != self.pending_status:
                    payload.pop(status_field, None)

                if self._needs_update(review_entry['fields'], payload):
                    if self.feishu.update_custom_record(self.review_table_id, review_entry['record_id'], payload):
                        updated += 1
            else:
                if self.feishu.create_custom_record(self.review_table_id, payload):
                    created += 1

        # 处理用户确认
        for change_id, review_entry in existing_records.items():
            status_value = self._extract_value(review_entry['fields'].get(self.field_map['status']))
            if status_value != self.confirmed_status:
                continue

            try:
                numeric_id = int(change_id)
            except ValueError:
                continue

            if not self.db.resolve_feishu_change_by_id(numeric_id):
                continue

            update_fields = {
                self.field_map['status']: self.resolved_status
            }

            resolved_at_field = self.field_map.get('resolved_at')
            if resolved_at_field:
                ts = self._format_timestamp(int(time.time()))
                if ts is not None:
                    update_fields[resolved_at_field] = ts

            self.feishu.update_custom_record(self.review_table_id, review_entry['record_id'], update_fields)
            resolved += 1

        logger.info(
            f"飞书手动修改审计同步完成: 新增 {created}, 更新 {updated}, 已确认 {resolved}"
        )

        return {
            'success': True,
            'created': created,
            'updated': updated,
            'resolved': resolved,
            'pending': len(pending_changes)
        }

    def _build_review_fields(self, change: Dict) -> Dict:
        change_type = '删除' if change.get('change_type') == 'deleted' else '更新'
        changed_fields = change.get('changed_fields') or []
        changed_summary = ', '.join(changed_fields) if changed_fields else '全部字段'

        payload = {
            self.field_map['change_id']: str(change['id']),
            self.field_map['record_id']: change.get('record_id') or '',
            self.field_map['table_name']: change.get('table_name'),
            self.field_map['symbol']: self._extract_symbol(change),
            self.field_map['change_type']: change_type,
            self.field_map['changed_fields']: changed_summary,
            self.field_map['status']: self.pending_status
        }

        detected_field = self.field_map.get('detected_at')
        if detected_field:
            ts = self._format_timestamp(change.get('detected_at'))
            if ts is not None:
                payload[detected_field] = ts

        note_field = self.field_map.get('note')
        if note_field:
            payload.setdefault(note_field, '')

        return {k: v for k, v in payload.items() if v is not None}

    def _load_review_records(self) -> Dict[str, Dict]:
        if not self.review_table_id:
            return {}

        records = {}
        try:
            items = self.feishu.fetch_table_records(self.review_table_id, self.page_size)
        except Exception as exc:
            logger.error(f"获取飞书审计表失败: {exc}")
            return {}

        change_id_field = self.field_map['change_id']
        for item in items:
            fields = item.get('fields', {})
            change_id = self._extract_value(fields.get(change_id_field))
            if not change_id:
                continue
            records[str(change_id)] = {
                'record_id': item.get('record_id'),
                'fields': fields
            }

        return records

    def _needs_update(self, current_fields: Dict, desired_fields: Dict) -> bool:
        for key, value in desired_fields.items():
            current_value = self._extract_value(current_fields.get(key))
            expected_value = value
            if not isinstance(expected_value, (dict, list)):
                expected_value = str(expected_value)
            if current_value != expected_value:
                return True
        return False

    def _extract_symbol(self, change: Dict) -> str:
        for snapshot in (change.get('current_fields'), change.get('previous_fields')):
            if not snapshot:
                continue
            value = snapshot.get('资产代码') or snapshot.get('代码')
            text = self._extract_value(value)
            if text:
                return text
        return ''

    def _extract_value(self, value) -> str:
        if value is None:
            return ''
        if isinstance(value, list):
            parts: List[str] = []
            for item in value:
                if isinstance(item, dict):
                    text = item.get('text') or item.get('name') or item.get('value')
                    if text:
                        parts.append(str(text))
                else:
                    parts.append(str(item))
            return ','.join(parts)
        if isinstance(value, dict):
            return str(value.get('text') or value.get('name') or value.get('value') or '')
        return str(value)

    @staticmethod
    def _format_timestamp(ts: int) -> Optional[int]:
        if not ts:
            return None
        return int(ts) * 1000


def sync_feishu_change_review(config_path: str = 'config.json') -> Dict:
    """入口函数"""
    config = Config(config_path)
    task = FeishuChangeReviewTask(config)
    return task.sync()
