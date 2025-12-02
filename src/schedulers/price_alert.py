"""
价格波动监控任务
监控资产价格异常波动并发送告警
"""
import time
from typing import Dict, List
from datetime import datetime, timedelta
from loguru import logger

from core.config import Config
from core.database import AssetDB
from core.feishu_client import AssetFeishuClient
from utils.alert import AlertManager


class PriceAlertTask:
    """
    价格波动监控任务

    检测资产价格异常波动,
    超过阈值时发送告警
    """

    # 默认波动阈值
    DEFAULT_THRESHOLDS = {
        '基金': 3.0,      # 3%
        '加密货币': 10.0,  # 10%
        '股票': 5.0       # 5%
    }

    def __init__(self, config: Config):
        """初始化监控任务"""
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
            email_config=alert_config.get('email'),
            enabled=alert_config.get('enabled', False)
        )

        # 获取自定义阈值
        self.thresholds = asset_sync.get('price_alert', {}).get('thresholds', self.DEFAULT_THRESHOLDS)

        logger.info("PriceAlertTask 初始化完成")

    def check_alerts(self) -> Dict:
        """
        检查价格波动告警

        :return: 检查结果
        """
        start_time = time.time()

        result = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'total_assets': 0,
            'alerts': [],
            'errors': []
        }

        try:
            # 1. 获取所有持仓
            logger.info("开始获取资产持仓数据...")
            holdings = self.feishu.get_all_holdings()

            if not holdings:
                logger.warning("没有持仓数据")
                return result

            result['total_assets'] = len(holdings)

            # 2. 检查每个资产的价格波动
            for item in holdings:
                fields = item.get('fields', {})

                try:
                    # 资产代码
                    code_field = fields.get('资产代码')
                    if isinstance(code_field, list):
                        code = code_field[0].get('text', '') if code_field else ''
                    else:
                        code = str(code_field or '')

                    # 资产类型
                    asset_type = fields.get('资产类型')
                    if not asset_type or not code:
                        continue

                    # 获取阈值
                    threshold = self.thresholds.get(asset_type, 5.0)

                    # 计算涨跌幅
                    change_rate = self._calculate_change_rate(code, asset_type)

                    if change_rate is None:
                        continue

                    # 检查是否超过阈值
                    if abs(change_rate) >= threshold:
                        # 资产名称
                        name = fields.get('资产名称', code)

                        # 当前价格
                        price_field = fields.get('当前价格')
                        if isinstance(price_field, dict):
                            price_array = price_field.get('value', [0])
                            current_price = float(price_array[0]) if price_array else 0
                        else:
                            current_price = float(price_field or 0)

                        result['alerts'].append({
                            'code': code,
                            'name': name,
                            'type': asset_type,
                            'change_rate': change_rate,
                            'current_price': current_price,
                            'threshold': threshold
                        })

                except Exception as e:
                    logger.error(f"检查资产 {code} 失败: {e}")
                    continue

            # 3. 发送告警
            if result['alerts']:
                logger.info(f"发现 {len(result['alerts'])} 个价格异常波动")
                self._send_price_alerts(result['alerts'])
            else:
                logger.info("未发现价格异常波动")

            # 4. 记录日志
            duration = time.time() - start_time

            try:
                self.feishu.log_sync_status(
                    source='system',
                    task_type='price_alert',
                    status='success',
                    record_count=len(result['alerts']),
                    error_msg=None,
                    duration=duration
                )
            except Exception as e:
                logger.error(f"记录告警日志失败: {e}")

        except Exception as e:
            logger.error(f"价格波动检查失败: {e}")
            result['success'] = False
            result['errors'].append(str(e))

        return result

    def _calculate_change_rate(self, symbol: str, asset_type: str) -> float:
        """
        计算资产日涨跌幅

        :param symbol: 资产代码
        :param asset_type: 资产类型
        :return: 涨跌幅百分比
        """
        try:
            import time

            # 计算时间范围
            end_time = int(time.time() * 1000)

            if asset_type == '加密货币':
                # 加密货币: 获取24小时前到现在的数据
                start_time = end_time - (24 * 60 * 60 * 1000)
                trading_pair = f"{symbol}/USDT"
                prices = self.db.get_price_history(trading_pair, start_time, end_time)
            else:
                # 基金/股票: 获取最近7天的数据
                start_time = end_time - (7 * 24 * 60 * 60 * 1000)
                prices = self.db.get_price_history(symbol, start_time, end_time)

            if len(prices) < 2:
                return None

            # 最新价格和最早价格
            latest_price = prices[-1]['price']  # 最新的在最后
            previous_price = prices[0]['price']  # 最早的在开头

            if previous_price == 0:
                return None

            # 计算涨跌幅
            change_rate = ((latest_price - previous_price) / previous_price) * 100

            return change_rate

        except Exception as e:
            logger.error(f"计算 {symbol} 涨跌幅失败: {e}")
            return None

    def _send_price_alerts(self, alerts: List[Dict]):
        """
        发送价格波动告警

        :param alerts: 告警列表
        """
        if not self.alert_manager.enabled:
            logger.warning("告警功能未启用,跳过发送通知")
            return

        try:
            # 按涨跌幅排序
            alerts_sorted = sorted(alerts, key=lambda x: abs(x['change_rate']), reverse=True)

            # 构建告警消息
            alert_lines = []
            for alert in alerts_sorted[:10]:  # 最多显示10个
                emoji = "📈" if alert['change_rate'] > 0 else "📉"
                alert_lines.append(
                    f"{emoji} **{alert['code']}** {alert['change_rate']:+.2f}% (阈值: {alert['threshold']}%)"
                )

            # 构建卡片
            card = {
                "msg_type": "interactive",
                "card": {
                    "config": {
                        "wide_screen_mode": True
                    },
                    "header": {
                        "title": {
                            "tag": "plain_text",
                            "content": f"⚠️ 价格异常波动告警 ({len(alerts)}个)"
                        },
                        "template": "red"
                    },
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": f"**检测时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n" + "\n".join(alert_lines)
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
                logger.info(f"价格告警已发送到飞书 ({len(alerts)}个)")
            else:
                logger.error(f"发送告警失败: {response.text}")

        except Exception as e:
            logger.error(f"发送价格告警失败: {e}")


def check_price_alerts(config_path: str = 'config.json') -> Dict:
    """
    检查价格告警 (便捷函数)

    :param config_path: 配置文件路径
    :return: 检查结果
    """
    config = Config(config_path)
    task = PriceAlertTask(config)
    return task.check_alerts()


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

    # 检查告警
    result = check_price_alerts()
    print(f"\n检查结果: {result}")
