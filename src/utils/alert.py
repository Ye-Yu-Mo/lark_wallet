"""
告警通知模块
支持飞书机器人推送告警消息
"""
import requests
from typing import Dict, Optional
from datetime import datetime
from loguru import logger


class AlertManager:
    """
    告警管理器

    负责发送各类告警通知到飞书群
    """

    def __init__(self, webhook_url: str, enabled: bool = True):
        """
        初始化告警管理器

        :param webhook_url: 飞书群机器人 webhook 地址
        :param enabled: 是否启用告警
        """
        self.webhook_url = webhook_url
        self.enabled = enabled

        # 代理配置
        self.proxies = {
            'http': 'http://127.0.0.1:7890',
            'https': 'http://127.0.0.1:7890'
        }

    def send_alert(
        self,
        title: str,
        content: str,
        level: str = 'warning'
    ) -> bool:
        """
        发送告警消息

        :param title: 告警标题
        :param content: 告警内容
        :param level: 告警级别 (info/warning/error)
        :return: 是否发送成功
        """
        if not self.enabled:
            logger.debug("告警未启用,跳过发送")
            return False

        if not self.webhook_url:
            logger.warning("未配置 webhook_url,无法发送告警")
            return False

        # 构建飞书消息卡片
        level_config = {
            'info': {'color': 'blue', 'emoji': 'ℹ️'},
            'warning': {'color': 'orange', 'emoji': '⚠️'},
            'error': {'color': 'red', 'emoji': '❌'}
        }

        config = level_config.get(level, level_config['warning'])
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        message = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": f"{config['emoji']} {title}"
                    },
                    "template": config['color']
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": content
                        }
                    },
                    {
                        "tag": "hr"
                    },
                    {
                        "tag": "note",
                        "elements": [
                            {
                                "tag": "plain_text",
                                "content": f"时间: {timestamp}"
                            }
                        ]
                    }
                ]
            }
        }

        try:
            response = requests.post(
                self.webhook_url,
                json=message,
                timeout=10,
                proxies=self.proxies
            )

            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    logger.info(f"告警发送成功: {title}")
                    return True
                else:
                    logger.error(f"告警发送失败: {result}")
                    return False
            else:
                logger.error(f"告警发送失败, HTTP {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"发送告警异常: {e}")
            return False

    def send_sync_failure(
        self,
        task_name: str,
        source: str,
        error_summary: str,
        total: int,
        failed: int
    ) -> bool:
        """
        发送同步失败告警

        :param task_name: 任务名称
        :param source: 数据源
        :param error_summary: 错误摘要
        :param total: 总数
        :param failed: 失败数
        :return: 是否发送成功
        """
        success_rate = ((total - failed) / total * 100) if total > 0 else 0

        content = f"""**任务**: {task_name}
**数据源**: {source}
**总数**: {total}
**失败**: {failed}
**成功率**: {success_rate:.1f}%

**错误详情**:
{error_summary}"""

        return self.send_alert(
            title=f"{task_name}同步失败",
            content=content,
            level='error'
        )

    def send_sync_partial_success(
        self,
        task_name: str,
        source: str,
        total: int,
        success: int,
        failed: int,
        error_summary: str
    ) -> bool:
        """
        发送部分成功告警

        :param task_name: 任务名称
        :param source: 数据源
        :param total: 总数
        :param success: 成功数
        :param failed: 失败数
        :param error_summary: 错误摘要
        :return: 是否发送成功
        """
        success_rate = (success / total * 100) if total > 0 else 0

        content = f"""**任务**: {task_name}
**数据源**: {source}
**总数**: {total}
**成功**: {success}
**失败**: {failed}
**成功率**: {success_rate:.1f}%

**失败资产**:
{error_summary}"""

        return self.send_alert(
            title=f"{task_name}部分成功",
            content=content,
            level='warning'
        )

    def send_database_backup_alert(
        self,
        status: str,
        backup_path: str,
        size: int = 0,
        error: Optional[str] = None
    ) -> bool:
        """
        发送数据库备份告警

        :param status: 状态 (success/failed)
        :param backup_path: 备份路径
        :param size: 备份大小 (字节)
        :param error: 错误信息
        :return: 是否发送成功
        """
        if status == 'success':
            size_mb = size / 1024 / 1024
            content = f"""**备份路径**: {backup_path}
**备份大小**: {size_mb:.2f} MB
**状态**: 成功"""
            return self.send_alert(
                title="数据库备份成功",
                content=content,
                level='info'
            )
        else:
            content = f"""**备份路径**: {backup_path}
**错误**: {error}"""
            return self.send_alert(
                title="数据库备份失败",
                content=content,
                level='error'
            )

    def send_system_start(self) -> bool:
        """发送系统启动通知"""
        content = "资产同步系统已启动,定时任务运行中"
        return self.send_alert(
            title="系统启动",
            content=content,
            level='info'
        )

    def send_system_stop(self) -> bool:
        """发送系统停止通知"""
        content = "资产同步系统已停止"
        return self.send_alert(
            title="系统停止",
            content=content,
            level='info'
        )
