"""
数据库备份模块
自动备份 SQLite 数据库并清理过期备份
"""
import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List
from loguru import logger


class DatabaseBackup:
    """
    数据库备份管理器

    负责创建、管理和清理数据库备份
    """

    def __init__(
        self,
        db_path: str,
        backup_path: str = 'data/backups/',
        keep_days: int = 30
    ):
        """
        初始化备份管理器

        :param db_path: 数据库文件路径
        :param backup_path: 备份目录路径
        :param keep_days: 保留天数
        """
        self.db_path = Path(db_path)
        self.backup_path = Path(backup_path)
        self.keep_days = keep_days

        # 确保备份目录存在
        self.backup_path.mkdir(parents=True, exist_ok=True)

    def create_backup(self) -> Dict:
        """
        创建数据库备份

        :return: 备份结果 {'success': bool, 'path': str, 'size': int, 'error': str}
        """
        result = {
            'success': False,
            'path': '',
            'size': 0,
            'error': ''
        }

        try:
            # 检查数据库文件是否存在
            if not self.db_path.exists():
                result['error'] = f"数据库文件不存在: {self.db_path}"
                logger.error(result['error'])
                return result

            # 生成备份文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"assets_{timestamp}.db"
            backup_file = self.backup_path / backup_filename

            # 复制数据库文件
            shutil.copy2(self.db_path, backup_file)

            # 获取备份文件大小
            backup_size = backup_file.stat().st_size

            result['success'] = True
            result['path'] = str(backup_file)
            result['size'] = backup_size

            logger.info(f"数据库备份成功: {backup_file} ({backup_size} bytes)")

        except Exception as e:
            result['error'] = str(e)
            logger.error(f"数据库备份失败: {e}")

        return result

    def cleanup_old_backups(self) -> Dict:
        """
        清理过期备份

        :return: 清理结果 {'cleaned': int, 'kept': int, 'errors': List}
        """
        result = {
            'cleaned': 0,
            'kept': 0,
            'errors': []
        }

        try:
            # 计算过期时间
            cutoff_time = datetime.now() - timedelta(days=self.keep_days)
            cutoff_timestamp = cutoff_time.timestamp()

            # 遍历备份目录
            for backup_file in self.backup_path.glob('assets_*.db'):
                try:
                    # 获取文件修改时间
                    file_mtime = backup_file.stat().st_mtime

                    if file_mtime < cutoff_timestamp:
                        # 删除过期备份
                        backup_file.unlink()
                        result['cleaned'] += 1
                        logger.debug(f"删除过期备份: {backup_file}")
                    else:
                        result['kept'] += 1

                except Exception as e:
                    error_msg = f"{backup_file}: {str(e)}"
                    result['errors'].append(error_msg)
                    logger.error(f"清理备份失败: {error_msg}")

            if result['cleaned'] > 0:
                logger.info(f"清理过期备份: 删除 {result['cleaned']} 个, 保留 {result['kept']} 个")

        except Exception as e:
            result['errors'].append(str(e))
            logger.error(f"清理备份失败: {e}")

        return result

    def get_backup_list(self) -> List[Dict]:
        """
        获取备份列表

        :return: 备份文件列表
        """
        backups = []

        try:
            for backup_file in sorted(
                self.backup_path.glob('assets_*.db'),
                key=lambda f: f.stat().st_mtime,
                reverse=True
            ):
                stat = backup_file.stat()
                backups.append({
                    'path': str(backup_file),
                    'name': backup_file.name,
                    'size': stat.st_size,
                    'mtime': stat.st_mtime,
                    'created_at': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                })

        except Exception as e:
            logger.error(f"获取备份列表失败: {e}")

        return backups

    def restore_backup(self, backup_file: str) -> bool:
        """
        从备份恢复数据库

        :param backup_file: 备份文件路径
        :return: 是否成功
        """
        try:
            backup_path = Path(backup_file)

            if not backup_path.exists():
                logger.error(f"备份文件不存在: {backup_file}")
                return False

            # 备份当前数据库 (防止恢复失败)
            current_backup = self.db_path.parent / f"{self.db_path.name}.before_restore"
            if self.db_path.exists():
                shutil.copy2(self.db_path, current_backup)

            # 恢复备份
            shutil.copy2(backup_path, self.db_path)

            logger.info(f"数据库恢复成功: {backup_file}")
            logger.info(f"原数据库已备份到: {current_backup}")

            return True

        except Exception as e:
            logger.error(f"数据库恢复失败: {e}")
            return False


def create_backup(config_path: str = 'config.json') -> Dict:
    """
    创建数据库备份 (便捷函数)

    :param config_path: 配置文件路径
    :return: 备份结果
    """
    from core.config import Config

    config = Config(config_path)
    db_config = config.get_database_config()
    backup_config = db_config.get('backup', {})

    if not backup_config.get('enabled', False):
        logger.warning("数据库备份未启用")
        return {'success': False, 'error': '备份未启用'}

    backup_manager = DatabaseBackup(
        db_path=db_config['path'],
        backup_path=backup_config.get('path', 'data/backups/'),
        keep_days=backup_config.get('keep_days', 30)
    )

    # 创建备份
    result = backup_manager.create_backup()

    # 清理过期备份
    if result['success']:
        cleanup_result = backup_manager.cleanup_old_backups()
        if cleanup_result['cleaned'] > 0:
            logger.info(f"清理了 {cleanup_result['cleaned']} 个过期备份")

    return result


if __name__ == '__main__':
    # 测试备份功能
    import sys
    from pathlib import Path

    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

    from core.logger import setup_logger
    setup_logger(level='DEBUG')

    result = create_backup()
    print(f"\n备份结果: {result}")
