"""
持仓周期提醒任务
提醒用户某些资产已持有特定天数
"""
import time
from typing import Dict, List
from datetime import datetime, timedelta
from loguru import logger

from core.config import Config
from core.feishu_client import AssetFeishuClient
from utils.alert import AlertManager


class HoldingPeriodReminderTask:
    """
    持仓周期提醒任务

    检查资产持有天数,
    达到特定周期时发送提醒
    """

    # 默认提醒周期 (天数)
    DEFAULT_PERIODS = [30, 90, 180, 365, 730]  # 1个月, 3个月, 半年, 1年, 2年

    def __init__(self, config: Config):
        """初始化提醒任务"""
        self.config = config

        # 获取配置
        asset_sync = config.get_asset_sync_config()
        feishu_config = config.get_feishu_config()
        alert_config = asset_sync.get('alerts', {})

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
            email_config=alert_config.get('email'),
            enabled=alert_config.get('enabled', False)
        )

        # 获取自定义周期
        self.periods = asset_sync.get('holding_period_reminder', {}).get(
            'periods',
            self.DEFAULT_PERIODS
        )
        self.periods.sort()

        # 记录已提醒的资产 (资产代码 -> 已提醒的周期列表)
        self.reminded_assets = self._load_reminded_assets()

        logger.info("HoldingPeriodReminderTask 初始化完成")

    def check_holding_periods(self) -> Dict:
        """
        检查持仓周期

        :return: 检查结果
        """
        start_time = time.time()

        result = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'total_assets': 0,
            'reminders': [],
            'errors': []
        }

        try:
            # 1. 获取所有持仓
            logger.info("开始检查持仓周期...")
            holdings = self.feishu.get_all_holdings()

            if not holdings:
                logger.warning("没有持仓数据")
                return result

            result['total_assets'] = len(holdings)

            # 2. 检查每个资产的持有天数
            today = datetime.now().date()

            for item in holdings:
                fields = item.get('fields', {})

                try:
                    # 资产代码
                    code_field = fields.get('资产代码')
                    if isinstance(code_field, list):
                        code = code_field[0].get('text', '') if code_field else ''
                    else:
                        code = str(code_field or '')

                    # 资产名称
                    name = fields.get('资产名称', code)

                    # 资产类型
                    asset_type = fields.get('资产类型')

                    if not code or not asset_type:
                        continue

                    # 获取购买日期
                    buy_date_field = fields.get('购买日期')
                    if not buy_date_field:
                        continue

                    # 解析日期 (Feishu日期字段是时间戳,单位毫秒)
                    if isinstance(buy_date_field, int):
                        buy_date = datetime.fromtimestamp(buy_date_field / 1000).date()
                    else:
                        continue

                    # 计算持有天数
                    holding_days = (today - buy_date).days

                    # 当前市值
                    value_field = fields.get('当前市值')
                    if isinstance(value_field, dict):
                        value_array = value_field.get('value', [0])
                        current_value = float(value_array[0]) if value_array else 0
                    else:
                        current_value = float(value_field or 0)

                    # 收益率
                    rate_field = fields.get('收益率')
                    if isinstance(rate_field, dict):
                        rate_array = rate_field.get('value', [0])
                        profit_rate = float(rate_array[0]) if rate_array else 0
                    else:
                        profit_rate = float(rate_field or 0)

                    # 检查是否达到提醒周期
                    for period in self.periods:
                        if holding_days >= period:
                            # 检查是否已经提醒过这个周期
                            if code not in self.reminded_assets:
                                self.reminded_assets[code] = set()

                            if period not in self.reminded_assets[code]:
                                # 需要提醒
                                result['reminders'].append({
                                    'code': code,
                                    'name': name,
                                    'type': asset_type,
                                    'holding_days': holding_days,
                                    'period': period,
                                    'buy_date': buy_date.isoformat(),
                                    'current_value': current_value,
                                    'profit_rate': profit_rate
                                })

                                # 标记已提醒
                                self.reminded_assets[code].add(period)
                                self._save_reminded_asset(code, period)

                                logger.info(f"资产 {code} 已持有 {period} 天")

                except Exception as e:
                    logger.error(f"检查资产 {code} 持仓周期失败: {e}")
                    continue

            # 3. 发送提醒
            if result['reminders']:
                logger.info(f"发现 {len(result['reminders'])} 个持仓周期提醒")
                self._send_holding_period_reminders(result['reminders'])
            else:
                logger.info("未发现需要提醒的持仓周期")

            # 4. 记录日志
            duration = time.time() - start_time

            try:
                self.feishu.log_sync_status(
                    source='system',
                    task_type='holding_period_reminder',
                    status='success',
                    record_count=len(result['reminders']),
                    error_msg=None,
                    duration=duration
                )
            except Exception as e:
                logger.error(f"记录提醒日志失败: {e}")

        except Exception as e:
            logger.error(f"检查持仓周期失败: {e}")
            result['success'] = False
            result['errors'].append(str(e))

        return result

    def _load_reminded_assets(self) -> Dict[str, set]:
        """
        从文件加载已提醒的资产记录

        :return: 已提醒资产字典
        """
        try:
            import json
            from pathlib import Path

            # 使用数据目录存储提醒记录
            data_dir = Path('data')
            data_dir.mkdir(exist_ok=True)

            reminder_file = data_dir / 'holding_period_reminders.json'

            if reminder_file.exists():
                with open(reminder_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 将列表转换为set
                    return {k: set(v) for k, v in data.items()}

            return {}

        except Exception as e:
            logger.error(f"加载提醒记录失败: {e}")
            return {}

    def _save_reminded_asset(self, code: str, period: int):
        """
        保存已提醒的资产记录

        :param code: 资产代码
        :param period: 提醒周期
        """
        try:
            import json
            from pathlib import Path

            data_dir = Path('data')
            data_dir.mkdir(exist_ok=True)

            reminder_file = data_dir / 'holding_period_reminders.json'

            # 读取现有数据
            if reminder_file.exists():
                with open(reminder_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    data = {k: set(v) for k, v in data.items()}
            else:
                data = {}

            # 更新数据
            if code not in data:
                data[code] = set()
            data[code].add(period)

            # 保存 (将set转换为list)
            with open(reminder_file, 'w', encoding='utf-8') as f:
                json.dump({k: list(v) for k, v in data.items()}, f, ensure_ascii=False, indent=2)

        except Exception as e:
            logger.error(f"保存提醒记录失败: {e}")

    def _send_holding_period_reminders(self, reminders: List[Dict]):
        """
        发送持仓周期提醒到飞书

        :param reminders: 提醒列表
        """
        if not self.alert_manager.enabled:
            logger.warning("告警功能未启用,跳过发送通知")
            return

        try:
            # 按周期分组
            grouped = {}
            for reminder in reminders:
                period = reminder['period']
                if period not in grouped:
                    grouped[period] = []
                grouped[period].append(reminder)

            # 为每个周期发送一条消息
            for period, items in grouped.items():
                # 周期描述
                if period < 365:
                    period_text = f"{period}天"
                else:
                    years = period // 365
                    period_text = f"{years}年"

                # 构建资产列表
                asset_lines = []
                for item in items:
                    emoji = "🟢" if item['profit_rate'] >= 0 else "🔴"
                    asset_lines.append(
                        f"{emoji} **{item['code']}** - {item['name']}\n"
                        f"持有: {item['holding_days']}天 | 收益率: {item['profit_rate']:+.2f}%"
                    )

                # 构建卡片消息
                card = {
                    "msg_type": "interactive",
                    "card": {
                        "config": {
                            "wide_screen_mode": True
                        },
                        "header": {
                            "title": {
                                "tag": "plain_text",
                                "content": f"⏰ 持仓周期提醒 - {period_text}"
                            },
                            "template": "blue"
                        },
                        "elements": [
                            {
                                "tag": "div",
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"以下资产已持有满 **{period_text}**:\n\n" + "\n\n".join(asset_lines)
                                }
                            },
                            {
                                "tag": "hr"
                            },
                            {
                                "tag": "div",
                                "text": {
                                    "tag": "lark_md",
                                    "content": self._get_period_advice(period)
                                }
                            }
                        ]
                    }
                }

                # 发送消息
                import requests
                response = requests.post(
                    self.alert_manager.webhook_url,
                    json=card,
                    timeout=10
                )

                if response.status_code == 200:
                    logger.info(f"持仓周期提醒已发送: {period}天, {len(items)}个资产")
                else:
                    logger.error(f"发送持仓周期提醒失败: {response.text}")

        except Exception as e:
            logger.error(f"发送持仓周期提醒失败: {e}")

    def _get_period_advice(self, period: int) -> str:
        """
        根据持仓周期返回建议

        :param period: 持仓天数
        :return: 建议文本
        """
        advice = {
            30: "💡 **1个月提醒**\n短期持仓,建议关注市场动态,及时止盈止损。",
            90: "💡 **3个月提醒**\n持仓稳定期,可以评估是否符合预期,考虑调仓策略。",
            180: "💡 **半年提醒**\n中期持仓,建议回顾投资逻辑,评估是否继续持有。",
            365: "💡 **1年提醒**\n长期持仓达成!回顾过去一年的收益,决定未来策略。",
            730: "💡 **2年提醒**\n超长期持仓!恭喜你的耐心,回顾两年的投资成果。"
        }

        return advice.get(period, f"💡 持仓已达 {period} 天,建议定期回顾投资组合。")


def check_holding_periods(config_path: str = 'config.json') -> Dict:
    """
    检查持仓周期 (便捷函数)

    :param config_path: 配置文件路径
    :return: 检查结果
    """
    config = Config(config_path)
    task = HoldingPeriodReminderTask(config)
    return task.check_holding_periods()


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

    # 检查持仓周期
    result = check_holding_periods()
    print(f"\n检查结果: {result}")
