"""飞书 → SQLite 备份管理器"""
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

        logger.info(
            f"飞书表 {table_key} 备份完成, {inserted} 条写入 SQLite, 耗时 {duration:.2f}s"
        )

        return {
            'records': inserted,
            'duration': duration,
            'fetched': len(raw_records)
        }
