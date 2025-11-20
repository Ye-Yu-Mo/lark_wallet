#!/usr/bin/env python3
"""
资产同步系统主程序
使用 APScheduler 定时执行同步任务
"""
import sys
import signal
from pathlib import Path
from typing import Optional

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / 'src'))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from loguru import logger

from core.config import Config
from core.logger import setup_logger
from schedulers.crypto_sync import sync_crypto
from schedulers.fund_sync import sync_fund
from schedulers.snapshot import create_daily_snapshot
from utils.backup import create_backup
from utils.alert import AlertManager


class AssetSyncService:
    """
    资产同步服务

    管理定时任务,自动同步加密货币和基金数据
    """

    def __init__(self, config_path: str = 'config.json'):
        """
        初始化服务

        :param config_path: 配置文件路径
        """
        self.config_path = config_path
        self.config = Config(config_path)
        self.scheduler: Optional[BlockingScheduler] = None

        # 初始化日志
        logging_config = self.config.get_logging_config()
        setup_logger(
            log_path=logging_config.get('path', 'logs/'),
            level=logging_config.get('level', 'INFO'),
            max_bytes=logging_config.get('max_bytes', 10485760),
            backup_count=logging_config.get('backup_count', 5)
        )

        logger.info("=" * 60)
        logger.info("资产同步系统启动中...")
        logger.info("=" * 60)

        # 初始化告警管理器
        alert_config = self.config.get_asset_sync_config().get('alerts', {})
        self.alert_manager = AlertManager(
            webhook_url=alert_config.get('feishu_webhook', ''),
            enabled=alert_config.get('enabled', False)
        )

    def _setup_scheduler(self):
        """配置调度器"""
        self.scheduler = BlockingScheduler(timezone='Asia/Shanghai')

        # 添加事件监听
        self.scheduler.add_listener(self._on_job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self._on_job_error, EVENT_JOB_ERROR)

        # 获取调度器配置
        asset_sync = self.config.get_asset_sync_config()
        scheduler_config = asset_sync.get('scheduler', {})

        # 1. 加密货币同步任务
        crypto_sync_config = scheduler_config.get('crypto_sync', {})
        if crypto_sync_config.get('enabled', False):
            interval = crypto_sync_config.get('interval', 'hour')
            hour = crypto_sync_config.get('hour', '*')
            minute = crypto_sync_config.get('minute', 0)

            if interval == 'hour':
                # 每小时执行
                self.scheduler.add_job(
                    func=lambda: sync_crypto(self.config_path),
                    trigger='cron',
                    hour=hour,
                    minute=minute,
                    id='crypto_sync',
                    name='加密货币同步',
                    replace_existing=True
                )
                logger.info(f"已注册任务: 加密货币同步 (每小时 {minute}分)")
            else:
                logger.warning(f"加密货币同步任务配置的 interval 不支持: {interval}")

        # 2. 基金同步任务
        fund_sync_config = scheduler_config.get('fund_sync', {})
        if fund_sync_config.get('enabled', False):
            hour = fund_sync_config.get('hour', 9)
            minute = fund_sync_config.get('minute', 0)

            self.scheduler.add_job(
                func=lambda: sync_fund(self.config_path),
                trigger='cron',
                hour=hour,
                minute=minute,
                id='fund_sync',
                name='基金同步',
                replace_existing=True
            )
            logger.info(f"已注册任务: 基金同步 (每天 {hour:02d}:{minute:02d})")

        # 3. 每日快照任务
        snapshot_config = scheduler_config.get('snapshot', {})
        if snapshot_config.get('enabled', False):
            hour = snapshot_config.get('hour', 0)
            minute = snapshot_config.get('minute', 0)

            self.scheduler.add_job(
                func=lambda: create_daily_snapshot(self.config_path),
                trigger='cron',
                hour=hour,
                minute=minute,
                id='daily_snapshot',
                name='每日快照',
                replace_existing=True
            )
            logger.info(f"已注册任务: 每日快照 (每天 {hour:02d}:{minute:02d})")

        # 4. 数据库备份任务 (每天凌晨 1点)
        db_config = asset_sync.get('database', {})
        backup_config = db_config.get('backup', {})
        if backup_config.get('enabled', False):
            self.scheduler.add_job(
                func=lambda: self._backup_database(),
                trigger='cron',
                hour=1,
                minute=0,
                id='database_backup',
                name='数据库备份',
                replace_existing=True
            )
            logger.info(f"已注册任务: 数据库备份 (每天 01:00)")

        # 检查是否有任务被注册
        jobs = self.scheduler.get_jobs()
        if not jobs:
            logger.warning("警告: 没有启用任何定时任务")
            logger.warning("请检查 config.json 中的 asset_sync.scheduler 配置")
            logger.warning("enabled 字段需要设置为 true")

    def _backup_database(self):
        """执行数据库备份"""
        try:
            result = create_backup(self.config_path)

            if result.get('success'):
                logger.info(f"数据库备份成功: {result.get('path')}")
                # 发送成功通知 (可选)
                # self.alert_manager.send_database_backup_alert(
                #     status='success',
                #     backup_path=result.get('path'),
                #     size=result.get('size')
                # )
            else:
                logger.error(f"数据库备份失败: {result.get('error')}")
                # 发送失败告警
                self.alert_manager.send_database_backup_alert(
                    status='failed',
                    backup_path='',
                    error=result.get('error')
                )
        except Exception as e:
            logger.error(f"数据库备份异常: {e}")
            self.alert_manager.send_database_backup_alert(
                status='failed',
                backup_path='',
                error=str(e)
            )

    def _on_job_executed(self, event):
        """任务执行成功的回调"""
        logger.info(f"任务执行完成: {event.job_id}")

    def _on_job_error(self, event):
        """任务执行失败的回调"""
        logger.error(f"任务执行失败: {event.job_id}, 异常: {event.exception}")

    def _setup_signal_handlers(self):
        """设置信号处理器 (优雅关闭)"""
        def signal_handler(signum, frame):
            logger.info(f"\n收到信号 {signum}, 准备关闭服务...")

            # 发送停止通知
            try:
                self.alert_manager.send_system_stop()
            except Exception as e:
                logger.error(f"发送停止通知失败: {e}")

            if self.scheduler and self.scheduler.running:
                self.scheduler.shutdown(wait=True)

            logger.info("服务已关闭")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # kill

    def start(self):
        """启动服务"""
        try:
            # 配置调度器
            self._setup_scheduler()

            # 设置信号处理
            self._setup_signal_handlers()

            # 显示已注册的任务
            jobs = self.scheduler.get_jobs()
            if jobs:
                logger.info("\n已注册的定时任务:")
                for job in jobs:
                    logger.info(f"  - {job.name} ({job.id}): {job.trigger}")
            else:
                logger.warning("没有任何任务被启用,服务将空转")
                logger.info("提示: 在 config.json 中设置 scheduler.*.enabled = true 来启用任务")

            logger.info("\n资产同步服务已启动,按 Ctrl+C 退出")
            logger.info("=" * 60)

            # 发送启动通知
            try:
                self.alert_manager.send_system_start()
            except Exception as e:
                logger.error(f"发送启动通知失败: {e}")

            # 启动调度器 (阻塞运行)
            self.scheduler.start()

        except KeyboardInterrupt:
            logger.info("\n用户中断,正在关闭服务...")
            if self.scheduler and self.scheduler.running:
                self.scheduler.shutdown(wait=True)
            logger.info("服务已关闭")

        except Exception as e:
            logger.error(f"服务启动失败: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='资产同步系统')
    parser.add_argument(
        '-c', '--config',
        default='config.json',
        help='配置文件路径 (默认: config.json)'
    )
    parser.add_argument(
        '--run-once',
        action='store_true',
        help='只运行一次所有任务,不启动定时服务'
    )
    parser.add_argument(
        '--task',
        choices=['crypto', 'fund', 'snapshot'],
        help='只运行指定任务一次'
    )

    args = parser.parse_args()

    # 如果是一次性运行
    if args.run_once or args.task:
        setup_logger(level='INFO')

        if args.task:
            logger.info(f"执行单个任务: {args.task}")
            if args.task == 'crypto':
                result = sync_crypto(args.config)
            elif args.task == 'fund':
                result = sync_fund(args.config)
            elif args.task == 'snapshot':
                result = create_daily_snapshot(args.config)
            logger.info(f"任务结果: {result}")
        else:
            logger.info("执行所有任务一次...")
            logger.info("\n1. 加密货币同步:")
            crypto_result = sync_crypto(args.config)
            logger.info(f"   结果: {crypto_result}")

            logger.info("\n2. 基金同步:")
            fund_result = sync_fund(args.config)
            logger.info(f"   结果: {fund_result}")

            logger.info("\n3. 每日快照:")
            snapshot_result = create_daily_snapshot(args.config)
            logger.info(f"   结果: {snapshot_result}")

            logger.info("\n所有任务执行完成")

        return

    # 启动定时服务
    service = AssetSyncService(args.config)
    service.start()


if __name__ == '__main__':
    main()
