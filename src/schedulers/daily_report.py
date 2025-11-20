"""
每日收益报告任务
统计前一天的基金收益并发送飞书通知
"""
import time
from typing import Dict, List
from datetime import datetime, date, timedelta
from loguru import logger

from core.config import Config
from core.feishu_client import AssetFeishuClient
from utils.alert import AlertManager


class DailyReportTask:
    """
    每日收益报告任务

    统计前一天的基金收益情况,
    通过飞书机器人发送通知
    """

    def __init__(self, config: Config):
        """初始化报告任务"""
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
            enabled=alert_config.get('enabled', False)
        )

        logger.info("DailyReportTask 初始化完成")

    def generate_report(self) -> Dict:
        """
        生成每日收益报告

        :return: 报告结果
        """
        start_time = time.time()

        result = {
            'success': True,
            'date': (date.today() - timedelta(days=1)).isoformat(),
            'total_funds': 0,
            'total_value': 0,
            'total_cost': 0,
            'total_profit': 0,
            'profit_rate': 0,
            'fund_details': [],
            'errors': []
        }

        try:
            # 1. 获取所有持仓
            logger.info("开始获取基金持仓数据...")
            holdings = self.feishu.get_all_holdings()

            if not holdings:
                logger.warning("没有持仓数据")
                return result

            # 2. 按资产类型分类
            fund_holdings = []
            crypto_holdings = []

            for item in holdings:
                fields = item.get('fields', {})
                asset_type = fields.get('资产类型')

                if asset_type == '基金':
                    fund_holdings.append(fields)
                elif asset_type == '加密货币':
                    crypto_holdings.append(fields)

            if not fund_holdings and not crypto_holdings:
                logger.warning("没有基金或加密货币持仓")
                return result

            result['total_funds'] = len(fund_holdings)
            result['total_cryptos'] = len(crypto_holdings)
            result['fund_details'] = []
            result['crypto_details'] = []

            logger.info(f"获取到 {len(fund_holdings)} 个基金, {len(crypto_holdings)} 个加密货币")

            # 3. 统计基金收益
            fund_total_value = 0
            fund_total_cost = 0
            fund_total_profit = 0

            for fund in fund_holdings:
                # 资产代码
                code_field = fund.get('资产代码')
                if isinstance(code_field, list):
                    code = code_field[0].get('text', '') if code_field else ''
                else:
                    code = str(code_field or '')

                # 资产名称
                name_field = fund.get('资产名称')
                if isinstance(name_field, list):
                    name = name_field[0].get('text', '') if name_field else code
                else:
                    name = str(name_field or code)

                # 当前市值 (公式字段)
                value_field = fund.get('当前市值')
                if isinstance(value_field, dict):
                    value_array = value_field.get('value', [0])
                    current_value = float(value_array[0]) if value_array else 0
                else:
                    current_value = float(value_field or 0)

                # 总成本 (公式字段)
                cost_field = fund.get('总成本')
                if isinstance(cost_field, dict):
                    cost_array = cost_field.get('value', [0])
                    total_cost = float(cost_array[0]) if cost_array else 0
                else:
                    total_cost = float(cost_field or 0)

                # 收益金额 (公式字段)
                profit_field = fund.get('收益金额')
                if isinstance(profit_field, dict):
                    profit_array = profit_field.get('value', [0])
                    profit = float(profit_array[0]) if profit_array else 0
                else:
                    profit = float(profit_field or 0)

                # 收益率 (公式字段)
                rate_field = fund.get('收益率')
                if isinstance(rate_field, dict):
                    rate_array = rate_field.get('value', [0])
                    profit_rate = float(rate_array[0]) if rate_array else 0
                else:
                    profit_rate = float(rate_field or 0)

                # 累加总计
                fund_total_value += current_value
                fund_total_cost += total_cost
                fund_total_profit += profit

                # 记录详情
                result['fund_details'].append({
                    'code': code,
                    'name': name,
                    'value': current_value,
                    'cost': total_cost,
                    'profit': profit,
                    'profit_rate': profit_rate
                })

            # 4. 统计加密货币收益
            crypto_total_value = 0
            crypto_total_cost = 0
            crypto_total_profit = 0

            for crypto in crypto_holdings:
                # 资产代码
                code_field = crypto.get('资产代码')
                if isinstance(code_field, list):
                    code = code_field[0].get('text', '') if code_field else ''
                else:
                    code = str(code_field or '')

                # 资产名称
                name_field = crypto.get('资产名称')
                if isinstance(name_field, list):
                    name = name_field[0].get('text', '') if name_field else code
                else:
                    name = str(name_field or code)

                # 当前市值 (公式字段)
                value_field = crypto.get('当前市值')
                if isinstance(value_field, dict):
                    value_array = value_field.get('value', [0])
                    current_value = float(value_array[0]) if value_array else 0
                else:
                    current_value = float(value_field or 0)

                # 总成本 (公式字段)
                cost_field = crypto.get('总成本')
                if isinstance(cost_field, dict):
                    cost_array = cost_field.get('value', [0])
                    total_cost = float(cost_array[0]) if cost_array else 0
                else:
                    total_cost = float(cost_field or 0)

                # 收益金额 (公式字段)
                profit_field = crypto.get('收益金额')
                if isinstance(profit_field, dict):
                    profit_array = profit_field.get('value', [0])
                    profit = float(profit_array[0]) if profit_array else 0
                else:
                    profit = float(profit_field or 0)

                # 收益率 (公式字段)
                rate_field = crypto.get('收益率')
                if isinstance(rate_field, dict):
                    rate_array = rate_field.get('value', [0])
                    profit_rate = float(rate_array[0]) if rate_array else 0
                else:
                    profit_rate = float(rate_field or 0)

                # 累加总计
                crypto_total_value += current_value
                crypto_total_cost += total_cost
                crypto_total_profit += profit

                # 记录详情
                result['crypto_details'].append({
                    'code': code,
                    'name': name,
                    'value': current_value,
                    'cost': total_cost,
                    'profit': profit,
                    'profit_rate': profit_rate
                })

            # 5. 汇总数据
            result['fund_total_value'] = fund_total_value
            result['fund_total_cost'] = fund_total_cost
            result['fund_total_profit'] = fund_total_profit
            result['fund_profit_rate'] = (fund_total_profit / fund_total_cost * 100) if fund_total_cost > 0 else 0

            result['crypto_total_value'] = crypto_total_value
            result['crypto_total_cost'] = crypto_total_cost
            result['crypto_total_profit'] = crypto_total_profit
            result['crypto_profit_rate'] = (crypto_total_profit / crypto_total_cost * 100) if crypto_total_cost > 0 else 0

            result['total_value'] = fund_total_value + crypto_total_value
            result['total_cost'] = fund_total_cost + crypto_total_cost
            result['total_profit'] = fund_total_profit + crypto_total_profit
            result['profit_rate'] = (result['total_profit'] / result['total_cost'] * 100) if result['total_cost'] > 0 else 0

            # 6. 发送飞书通知
            self._send_report_notification(result)

            # 7. 记录日志
            duration = time.time() - start_time

            try:
                self.feishu.log_sync_status(
                    source='system',
                    task_type='daily_report',
                    status='success',
                    record_count=result['total_funds'] + result['total_cryptos'],
                    error_msg=None,
                    duration=duration
                )
            except Exception as e:
                logger.error(f"记录报告日志失败: {e}")

            logger.info(f"每日报告生成完成: {result['total_funds']}个基金, {result['total_cryptos']}个加密货币, "
                       f"总市值 {result['total_value']:.2f}, "
                       f"总收益 {result['total_profit']:.2f} ({result['profit_rate']:.2f}%)")

        except Exception as e:
            logger.error(f"生成每日报告失败: {e}")
            result['success'] = False
            result['errors'].append(str(e))

        return result

    def _send_report_notification(self, report: Dict):
        """
        发送报告通知到飞书

        :param report: 报告数据
        """
        if not self.alert_manager.enabled:
            logger.warning("告警功能未启用,跳过发送通知")
            return

        try:
            # 构建消息内容
            yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

            # 收益emoji
            profit_emoji = "📈" if report['total_profit'] >= 0 else "📉"

            # 构建卡片元素列表
            elements = []

            # 整体概览
            elements.append({
                "tag": "div",
                "fields": [
                    {
                        "is_short": True,
                        "text": {
                            "tag": "lark_md",
                            "content": f"**总资产**\n{report['total_funds'] + report.get('total_cryptos', 0)} 个"
                        }
                    },
                    {
                        "is_short": True,
                        "text": {
                            "tag": "lark_md",
                            "content": f"**总市值**\n¥{report['total_value']:.2f}"
                        }
                    }
                ]
            })
            elements.append({
                "tag": "div",
                "fields": [
                    {
                        "is_short": True,
                        "text": {
                            "tag": "lark_md",
                            "content": f"**总收益**\n¥{report['total_profit']:+.2f}"
                        }
                    },
                    {
                        "is_short": True,
                        "text": {
                            "tag": "lark_md",
                            "content": f"**收益率**\n{report['profit_rate']:+.2f}%"
                        }
                    }
                ]
            })

            # 基金部分
            if report.get('fund_details'):
                elements.append({"tag": "hr"})
                elements.append({
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**基金 ({report['total_funds']} 只)**"
                    }
                })
                elements.append({
                    "tag": "div",
                    "fields": [
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**市值**\n¥{report.get('fund_total_value', 0):.2f}"
                            }
                        },
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**收益**\n¥{report.get('fund_total_profit', 0):+.2f} ({report.get('fund_profit_rate', 0):+.2f}%)"
                            }
                        }
                    ]
                })

                # 基金明细
                fund_lines = []
                for fund in sorted(report['fund_details'], key=lambda x: x['profit'], reverse=True)[:10]:
                    emoji = "🟢" if fund['profit'] >= 0 else "🔴"
                    fund_lines.append(
                        f"{emoji} {fund['name']}: ¥{fund['profit']:+.2f} ({fund['profit_rate']:+.2f}%)"
                    )

                if fund_lines:
                    elements.append({
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "\n".join(fund_lines)
                        }
                    })

            # 加密货币部分
            if report.get('crypto_details'):
                elements.append({"tag": "hr"})
                elements.append({
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**加密货币 ({report.get('total_cryptos', 0)} 个)**"
                    }
                })
                elements.append({
                    "tag": "div",
                    "fields": [
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**市值**\n${report.get('crypto_total_value', 0):.2f}"
                            }
                        },
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**收益**\n${report.get('crypto_total_profit', 0):+.2f} ({report.get('crypto_profit_rate', 0):+.2f}%)"
                            }
                        }
                    ]
                })

                # 加密货币明细
                crypto_lines = []
                for crypto in sorted(report['crypto_details'], key=lambda x: x['profit'], reverse=True)[:10]:
                    emoji = "🟢" if crypto['profit'] >= 0 else "🔴"
                    crypto_lines.append(
                        f"{emoji} {crypto['name']}: ${crypto['profit']:+.2f} ({crypto['profit_rate']:+.2f}%)"
                    )

                if crypto_lines:
                    elements.append({
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "\n".join(crypto_lines)
                        }
                    })

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
                            "content": f"{profit_emoji} 资产日报 - {yesterday}"
                        },
                        "template": "blue" if report['total_profit'] >= 0 else "red"
                    },
                    "elements": elements
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
                logger.info("每日报告已发送到飞书")
            else:
                logger.error(f"发送报告失败: {response.text}")

        except Exception as e:
            logger.error(f"发送报告通知失败: {e}")


def send_daily_report(config_path: str = 'config.json') -> Dict:
    """
    发送每日报告 (便捷函数)

    :param config_path: 配置文件路径
    :return: 报告结果
    """
    config = Config(config_path)
    task = DailyReportTask(config)
    return task.generate_report()


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

    # 发送报告
    result = send_daily_report()
    print(f"\n报告结果: {result}")
