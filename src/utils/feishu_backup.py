"""飞书 → SQLite 备份管理器"""
import hashlib
import json
import time
from typing import Dict, List
from loguru import logger

from core.database import AssetDB
from core.feishu_client import AssetFeishuClient


class FeishuBackupManager:
    """负责同步飞书多维表到本地 SQLite"""

    def __init__(
        self,
        feishu_client: AssetFeishuClient,
        database: AssetDB,
        table_ids: Dict[str, str],
        page_size: int = 200
    ):
        self.feishu = feishu_client
        self.db = database
        self.table_ids = table_ids
        self.page_size = min(max(page_size, 50), 500)

    def sync_tables(self, table_keys: List[str]) -> Dict:
        """同步多个飞书表"""
        summary = {
            'tables': {},
            'success': True,
            'errors': []
        }

        for table_key in table_keys:
            try:
                summary['tables'][table_key] = self._sync_single_table(table_key)
            except Exception as exc:
                logger.error(f"备份飞书表 {table_key} 失败: {exc}")
                summary['success'] = False
                summary['errors'].append({
                    'table': table_key,
                    'error': str(exc)
                })

        return summary

    def _sync_single_table(self, table_key: str) -> Dict:
        """同步单个飞书表"""
        if table_key not in self.table_ids:
            raise ValueError(f"配置中缺少表 {table_key}")

        table_id = self.table_ids[table_key]
        start_time = time.time()

        track_changes = table_key == 'holdings'
        previous_snapshot = self.db.get_feishu_snapshot(table_key) if track_changes else {}

        logger.info(f"开始备份飞书表 {table_key} -> SQLite ...")
        raw_records = self.feishu.fetch_table_records(table_id, self.page_size)
        logger.info(f"飞书表 {table_key} 返回 {len(raw_records)} 条记录")

        normalized = []
        for item in raw_records:
            record_id = item.get('record_id')
            if not record_id:
                continue
            normalized.append({
                'record_id': record_id,
                'fields': item.get('fields', {}),
                'updated_at': item.get('last_modified_time') or item.get('created_time')
            })

        inserted = self.db.replace_feishu_table_records(table_key, normalized)
        duration = time.time() - start_time
        self.db.update_feishu_backup_meta(table_key, inserted, duration)

        if track_changes:
            changes = self._detect_changes(previous_snapshot, normalized)
            if changes:
                self.db.record_feishu_changes(table_key, changes)
                logger.warning(
                    f"飞书表 {table_key} 检测到 {len(changes)} 个记录发生手动修改, 已写入审计日志"
                )

        logger.info(
            f"飞书表 {table_key} 备份完成, {inserted} 条写入 SQLite, 耗时 {duration:.2f}s"
        )

        return {
            'records': inserted,
            'duration': duration,
            'fetched': len(raw_records)
        }

    def _detect_changes(self, previous_snapshot: Dict[str, Dict], new_records: List[Dict]) -> List[Dict]:
        """比较本地镜像与最新数据,返回差异列表"""
        if not previous_snapshot:
            return []

        remaining = dict(previous_snapshot)
        changes: List[Dict] = []

        for record in new_records:
            record_id = record.get('record_id')
            if not record_id:
                continue

            new_fields = record.get('fields', {})
            previous = remaining.pop(record_id, None)

            if previous is None:
                # 新创建的记录无需标记 (系统或用户新增)
                continue

            if previous.get('fields') == new_fields:
                continue

            changes.append({
                'record_id': record_id,
                'change_type': 'updated',
                'previous_fields': previous.get('fields'),
                'current_fields': new_fields,
                'previous_hash': self._hash_fields(previous.get('fields')),
                'current_hash': self._hash_fields(new_fields),
                'changed_fields': self._diff_fields(previous.get('fields'), new_fields)
            })

        # 剩余的记录在飞书侧被删除
        for record_id, snapshot in remaining.items():
            changes.append({
                'record_id': record_id,
                'change_type': 'deleted',
                'previous_fields': snapshot.get('fields'),
                'current_fields': None,
                'previous_hash': self._hash_fields(snapshot.get('fields')),
                'current_hash': None,
                'changed_fields': list(snapshot.get('fields', {}).keys())
            })

        return changes

    @staticmethod
    def _hash_fields(fields: Dict) -> str:
        serialized = json.dumps(fields or {}, ensure_ascii=False, sort_keys=True)
        return hashlib.md5(serialized.encode('utf-8')).hexdigest()

    @staticmethod
    def _diff_fields(previous: Dict, current: Dict) -> List[str]:
        previous = previous or {}
        current = current or {}
        keys = set(previous.keys()) | set(current.keys())
        changed = []
        for key in sorted(keys):
            if previous.get(key) != current.get(key):
                changed.append(key)
        return changed
