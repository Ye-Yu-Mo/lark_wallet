"""
体重记录提醒任务
如果昨天没有更新体重体脂率,发送邮件提醒
"""
import time
from typing import Dict
from datetime import datetime, timedelta
from loguru import logger

from core.config import Config
from core.health_client import HealthFeishuClient
from utils.alert import AlertManager


class WeightReminderTask:
    """
    体重记录提醒任务

    检查昨天是否更新了体重数据,
    如果未更新则发送邮件提醒
    """

    def __init__(self, config: Config):
        """
        初始化任务

        :param config: 配置对象
        """
        self.config = config

        # 获取配置
        health_config = config.get_health_config()
        feishu_config = config.get_feishu_config()
        alert_config = config.get_asset_sync_config().get('alerts', {})

        # 初始化健康数据客户端
        self.health_client = HealthFeishuClient(
            app_id=feishu_config['app_id'],
            app_secret=feishu_config['app_secret'],
            app_token=health_config['app_token'],
            table_ids=health_config['tables']
        )

        # 初始化告警管理器
        self.alert_manager = AlertManager(
            webhook_url=alert_config.get('feishu_webhook', ''),
            email_config=alert_config.get('email'),
            enabled=alert_config.get('enabled', False)
        )

        logger.info("WeightReminderTask 初始化完成")

    def check_weight_record(self) -> Dict:
        """
        检查昨天的体重记录

        :return: 执行结果
        """
        start_time = time.time()

        result = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'missing_record': False,
            'errors': []
        }

        try:
            logger.info("检查昨天的体重记录...")

            # 获取昨天的日期
            yesterday = datetime.now() - timedelta(days=1)
            yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)

            yesterday_start_ts = int(yesterday_start.timestamp() * 1000)
            yesterday_end_ts = int(yesterday_end.timestamp() * 1000)

            # 查询昨天的体重记录 (使用本地过滤，因为DateTime字段不支持范围查询)
            from core.feishu_client import FeishuClient
            feishu_config = self.config.get_feishu_config()
            health_config = self.config.get_health_config()

            feishu = FeishuClient(feishu_config['app_id'], feishu_config['app_secret'])

            # 拉取最近的记录 (假设每天最多1条，拉取最近30天应该足够)
            all_records = []
            page_token = None
            max_fetch = 30  # 只拉取30条，效率更高

            items, page_token, has_more = feishu.list_records(
                app_token=health_config['app_token'],
                table_id=health_config['tables']['daily_health'],
                page_token=None,
                page_size=max_fetch
            )
            all_records.extend(items)

            # 本地过滤昨天的记录
            filtered_records = []
            for record in all_records:
                fields = record.get('fields', {})
                date_value = fields.get('日期')

                if date_value and yesterday_start_ts <= date_value <= yesterday_end_ts:
                    filtered_records.append(fields)

            if not filtered_records:
                # 昨天没有记录
                logger.warning(f"昨天 ({yesterday.strftime('%Y-%m-%d')}) 没有体重记录")
                result['missing_record'] = True

                # 发送提醒邮件
                self._send_reminder(yesterday.strftime('%Y年%m月%d日'))
            else:
                logger.info(f"昨天有体重记录: {len(filtered_records)}条")

            # 记录执行时间
            duration = time.time() - start_time
            logger.info(f"体重提醒任务完成, 耗时 {duration:.2f}s")

        except Exception as e:
            logger.error(f"体重提醒任务失败: {e}")
            result['success'] = False
            result['errors'].append(str(e))

        return result

    def _send_reminder(self, date_str: str):
        """
        发送提醒邮件

        :param date_str: 日期字符串
        """
        if not self.alert_manager.enabled:
            logger.warning("告警功能未启用,跳过发送提醒")
            return

        if not self.alert_manager.email_config or not self.alert_manager.email_config.get('enabled'):
            logger.warning("邮件功能未启用,跳过发送提醒")
            return

        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart

            email_config = self.alert_manager.email_config

            # 构建邮件
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"提醒: {date_str} 体重数据未记录"
            msg['From'] = email_config['username']
            msg['To'] = ', '.join(email_config['recipients'])

            # HTML 内容
            html_content = f"""
            <html>
            <head>
                <meta charset="utf-8">
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; padding: 20px; }}
                    .reminder {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 20px 0; }}
                    h2 {{ color: #856404; }}
                    .action {{ background-color: #d1ecf1; border-left: 4px solid #17a2b8; padding: 15px; margin: 20px 0; }}
                </style>
            </head>
            <body>
                <h2>体重记录提醒</h2>
                <div class="reminder">
                    <p><strong>{date_str}</strong> 没有记录体重和体脂率数据。</p>
                </div>
                <div class="action">
                    <p><strong>请尽快补充:</strong></p>
                    <ul>
                        <li>体重 (kg)</li>
                        <li>体脂率 (%)</li>
                    </ul>
                    <p>记录后可以获得更准确的健康建议!</p>
                </div>
                <hr>
                <p style="color: #7f8c8d; font-size: 12px;">
                    提醒时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                </p>
            </body>
            </html>
            """

            # 添加 HTML 内容
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg.attach(html_part)

            # 发送邮件
            with smtplib.SMTP_SSL(
                email_config['smtp_server'],
                email_config['smtp_port']
            ) as server:
                server.login(email_config['username'], email_config['password'])
                server.send_message(msg)

            logger.info(f"体重提醒邮件已发送到: {', '.join(email_config['recipients'])}")

        except Exception as e:
            logger.error(f"发送提醒邮件失败: {e}")


def check_weight_reminder(config_path: str = 'config.json') -> Dict:
    """
    检查体重提醒 (便捷函数)

    :param config_path: 配置文件路径
    :return: 执行结果
    """
    config = Config(config_path)
    task = WeightReminderTask(config)
    return task.check_weight_record()


if __name__ == '__main__':
    # 直接运行测试
    import sys
    from pathlib import Path

    # 添加项目根目录到路径
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

    # 设置日志
    from core.logger import setup_logger
    setup_logger(level='DEBUG')

    # 检查提醒
    result = check_weight_reminder()
    print(f"\n执行结果: {result}")
