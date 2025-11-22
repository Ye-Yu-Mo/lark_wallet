"""飞书多维表 -> SQLite 备份任务"""
from typing import Dict
from loguru import logger

from core.config import Config
from core.database import AssetDB
from core.feishu_client import AssetFeishuClient
from utils.feishu_backup import FeishuBackupManager


def sync_feishu_backup(config_path: str = 'config.json') -> Dict:
    """执行飞书备份任务"""
    config = Config(config_path)
    asset_sync = config.get_asset_sync_config()
    feature_config = asset_sync.get('feishu_backup', {})

    if not feature_config.get('enabled', False):
        logger.info("飞书备份未启用, 跳过任务")
        return {'success': True, 'skipped': True}

    feishu_cfg = config.get_feishu_config()
    db_cfg = config.get_database_config()

    db = AssetDB(db_cfg['path'])
    feishu_client = AssetFeishuClient(
        app_id=feishu_cfg['app_id'],
        app_secret=feishu_cfg['app_secret'],
        app_token=feishu_cfg['app_token'],
        table_ids=feishu_cfg['tables']
    )

    manager = FeishuBackupManager(
        feishu_client=feishu_client,
        database=db,
        table_ids=feishu_cfg['tables'],
        page_size=feature_config.get('page_size', 200)
    )

    tables = feature_config.get('tables') or list(AssetDB.FEISHU_TABLES.keys())
    result = manager.sync_tables(tables)

    if result['success']:
        logger.info("飞书备份任务完成")
    else:
        logger.error(f"飞书备份任务部分失败: {result['errors']}")

    return result
