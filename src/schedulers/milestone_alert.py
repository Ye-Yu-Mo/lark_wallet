"""
净值里程碑提醒任务
监控总资产达到特定金额时发送通知
"""
import time
from typing import Dict, List
from datetime import datetime
from loguru import logger

from core.config import Config
from core.database import AssetDB
from core.feishu_client import AssetFeishuClient
from utils.alert import AlertManager


class MilestoneAlertTask:
    """
    净值里程碑提醒任务

    监控总资产净值,
    达到预设里程碑时发送庆祝通知
    """

    # 默认里程碑列表 (单位: 元)
    DEFAULT_MILESTONES = [
        10000,      # 1万
        50000,      # 5万
        100000,     # 10万
        200000,     # 20万
        500000,     # 50万
        1000000,    # 100万
        2000000,    # 200万
        5000000,    # 500万
        10000000,   # 1000万
    ]

    def __init__(self, config: Config):
        """初始化里程碑任务"""
        self.config = config

        # 获取配置
        asset_sync = config.get_asset_sync_config()
        feishu_config = config.get_feishu_config()
        db_config = config.get_database_config()
        alert_config = asset_sync.get('alerts', {})

        # 初始化数据库
        self.db = AssetDB(db_config['path'])

        # 初始化飞书客户端
        self.feishu = AssetFeishuClient(
            app_id=feishu_config['app_id'],
            app_secret=feishu_config['app_secret'],
            app_token=feishu_config['app_token'],
            table_ids=feishu_config['tables']
        )

        # 初始化告警管理器
        self.alert_manager = AlertManager(
            webhook_url=alert_config.get('feishu_webhook', ''),
            enabled=alert_config.get('enabled', False)
        )

        # 获取自定义里程碑
        self.milestones = asset_sync.get('milestone_alert', {}).get(
            'milestones',
            self.DEFAULT_MILESTONES
        )
        self.milestones.sort()  # 确保按升序排列

        # 存储已触发的里程碑 (使用数据库记录)
        self.triggered_milestones = self._load_triggered_milestones()

        logger.info("MilestoneAlertTask 初始化完成")

    def check_milestones(self) -> Dict:
        """
        检查净值里程碑

        :return: 检查结果
        """
        start_time = time.time()

        result = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'current_value': 0,
            'triggered_milestones': [],
            'next_milestone': None,
            'progress_to_next': 0,
            'errors': []
        }

        try:
            # 1. 获取所有持仓
            logger.info("开始检查净值里程碑...")
            holdings = self.feishu.get_all_holdings()

            if not holdings:
                logger.warning("没有持仓数据")
                return result

            # 2. 计算总资产净值
            total_value = 0

            for item in holdings:
                fields = item.get('fields', {})

                try:
                    # 当前市值
                    value_field = fields.get('当前市值')
                    if isinstance(value_field, dict):
                        value_array = value_field.get('value', [0])
                        current_value = float(value_array[0]) if value_array else 0
                    else:
                        current_value = float(value_field or 0)

                    total_value += current_value

                except Exception as e:
                    logger.error(f"计算资产市值失败: {e}")
                    continue

            result['current_value'] = total_value
            logger.info(f"当前总资产: {total_value:.2f}")

            # 3. 检查是否达到新的里程碑
            for milestone in self.milestones:
                if total_value >= milestone and milestone not in self.triggered_milestones:
                    # 达到新里程碑
                    result['triggered_milestones'].append(milestone)
                    self.triggered_milestones.add(milestone)
                    self._save_triggered_milestone(milestone)

                    logger.info(f"🎉 达到里程碑: {milestone:,.0f} 元!")

            # 4. 计算下一个里程碑
            for milestone in self.milestones:
                if milestone > total_value:
                    result['next_milestone'] = milestone
                    result['progress_to_next'] = (total_value / milestone) * 100
                    break

            # 5. 发送里程碑通知
            if result['triggered_milestones']:
                logger.info(f"发现 {len(result['triggered_milestones'])} 个新里程碑")
                self._send_milestone_alerts(result)
            else:
                logger.info("未达到新的里程碑")

            # 6. 记录日志
            duration = time.time() - start_time

            try:
                self.feishu.log_sync_status(
                    source='system',
                    task_type='milestone_alert',
                    status='success',
                    record_count=len(result['triggered_milestones']),
                    error_msg=None,
                    duration=duration
                )
            except Exception as e:
                logger.error(f"记录里程碑日志失败: {e}")

        except Exception as e:
            logger.error(f"检查里程碑失败: {e}")
            result['success'] = False
            result['errors'].append(str(e))

        return result

    def _load_triggered_milestones(self) -> set:
        """
        从数据库加载已触发的里程碑

        :return: 已触发的里程碑集合
        """
        try:
            with self.db._get_connection() as conn:
                cursor = conn.cursor()

                # 创建里程碑记录表 (如果不存在)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS milestone_records (
                        milestone REAL PRIMARY KEY,
                        triggered_at INTEGER NOT NULL,
                        total_value REAL NOT NULL
                    )
                ''')
                conn.commit()

                # 读取已触发的里程碑
                cursor.execute('SELECT milestone FROM milestone_records')
                rows = cursor.fetchall()

                return set(row[0] for row in rows)

        except Exception as e:
            logger.error(f"加载里程碑记录失败: {e}")
            return set()

    def _save_triggered_milestone(self, milestone: float):
        """
        保存已触发的里程碑到数据库

        :param milestone: 里程碑金额
        """
        try:
            with self.db._get_connection() as conn:
                cursor = conn.cursor()

                # 获取当前总值
                holdings = self.feishu.get_all_holdings()
                total_value = 0
                for item in holdings:
                    fields = item.get('fields', {})
                    value_field = fields.get('当前市值')
                    if isinstance(value_field, dict):
                        value_array = value_field.get('value', [0])
                        current_value = float(value_array[0]) if value_array else 0
                    else:
                        current_value = float(value_field or 0)
                    total_value += current_value

                cursor.execute('''
                    INSERT OR REPLACE INTO milestone_records (milestone, triggered_at, total_value)
                    VALUES (?, ?, ?)
                ''', (milestone, int(time.time() * 1000), total_value))

                conn.commit()

        except Exception as e:
            logger.error(f"保存里程碑记录失败: {e}")

    def _send_milestone_alerts(self, result: Dict):
        """
        发送里程碑通知到飞书

        :param result: 检查结果
        """
        if not self.alert_manager.enabled:
            logger.warning("告警功能未启用,跳过发送通知")
            return

        try:
            for milestone in result['triggered_milestones']:
                # 构建庆祝消息
                card = {
                    "msg_type": "interactive",
                    "card": {
                        "config": {
                            "wide_screen_mode": True
                        },
                        "header": {
                            "title": {
                                "tag": "plain_text",
                                "content": f"🎉 恭喜达成里程碑!"
                            },
                            "template": "yellow"
                        },
                        "elements": [
                            {
                                "tag": "div",
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**总资产突破**: ¥{milestone:,.0f}\n\n**当前总资产**: ¥{result['current_value']:,.2f}"
                                }
                            },
                            {
                                "tag": "hr"
                            },
                            {
                                "tag": "div",
                                "text": {
                                    "tag": "lark_md",
                                    "content": self._get_milestone_message(milestone)
                                }
                            }
                        ]
                    }
                }

                # 如果有下一个里程碑,显示进度
                if result['next_milestone']:
                    card["card"]["elements"].append({
                        "tag": "hr"
                    })
                    card["card"]["elements"].append({
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": f"**下一个目标**: ¥{result['next_milestone']:,.0f}\n"
                                     f"**完成进度**: {result['progress_to_next']:.1f}%"
                        }
                    })

                # 发送消息
                import requests
                response = requests.post(
                    self.alert_manager.webhook_url,
                    json=card,
                    timeout=10
                )

                if response.status_code == 200:
                    logger.info(f"里程碑通知已发送: {milestone:,.0f}")
                else:
                    logger.error(f"发送里程碑通知失败: {response.text}")

        except Exception as e:
            logger.error(f"发送里程碑通知失败: {e}")

    def _get_milestone_message(self, milestone: float) -> str:
        """
        根据里程碑金额返回对应的鼓励消息

        :param milestone: 里程碑金额
        :return: 鼓励消息
        """
        messages = {
            10000: "万元起步,投资之路正式开始! 💪",
            50000: "5万达成,小有积累! 继续保持! 🚀",
            100000: "10万大关,资产规模上新台阶! 🎊",
            200000: "20万达成,财富增长加速中! 📈",
            500000: "50万里程碑,资产配置渐入佳境! 🌟",
            1000000: "百万富翁诞生!人生新起点! 🎉🎉🎉",
            2000000: "200万资产,财富自由之路越走越宽! 🏆",
            5000000: "500万达成,资产配置大师! 👑",
            10000000: "千万身家,投资之神! 🔥🔥🔥"
        }

        return messages.get(milestone, f"恭喜达成 ¥{milestone:,.0f} 里程碑! 🎉")


def check_milestones(config_path: str = 'config.json') -> Dict:
    """
    检查净值里程碑 (便捷函数)

    :param config_path: 配置文件路径
    :return: 检查结果
    """
    config = Config(config_path)
    task = MilestoneAlertTask(config)
    return task.check_milestones()


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

    # 检查里程碑
    result = check_milestones()
    print(f"\n检查结果: {result}")
