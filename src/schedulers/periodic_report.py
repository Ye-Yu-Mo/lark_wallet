"""
周期性报告任务
每周/每月统计资产收益和表现
"""
import time
from typing import Dict, List
from datetime import datetime, date, timedelta
from loguru import logger

from core.config import Config
from core.database import AssetDB
from core.feishu_client import AssetFeishuClient
from utils.alert import AlertManager


class PeriodicReportTask:
    """
    周期性报告任务

    支持周报和月报,
    统计期间的资产表现和收益
    """

    def __init__(self, config: Config):
        """初始化报告任务"""
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

        logger.info("PeriodicReportTask 初始化完成")

    def generate_weekly_report(self) -> Dict:
        """
        生成周报

        :return: 报告结果
        """
        return self._generate_report(period='week', days=7)

    def generate_monthly_report(self) -> Dict:
        """
        生成月报

        :return: 报告结果
        """
        return self._generate_report(period='month', days=30)

    def _generate_report(self, period: str, days: int) -> Dict:
        """
        生成周期性报告

        :param period: 周期类型 (week/month)
        :param days: 统计天数
        :return: 报告结果
        """
        start_time = time.time()

        result = {
            'success': True,
            'period': period,
            'days': days,
            'timestamp': datetime.now().isoformat(),
            'total_assets': 0,
            'total_value': 0,
            'total_cost': 0,
            'total_profit': 0,
            'profit_rate': 0,
            'period_change': 0,
            'period_change_rate': 0,
            'top_performers': [],
            'worst_performers': [],
            'fund_performance': [],
            'crypto_performance': [],
            'total_funds': 0,
            'total_cryptos': 0,
            'fund_total_value': 0,
            'fund_total_cost': 0,
            'fund_total_profit': 0,
            'fund_profit_rate': 0,
            'crypto_total_value': 0,
            'crypto_total_cost': 0,
            'crypto_total_profit': 0,
            'crypto_profit_rate': 0,
            'errors': []
        }

        try:
            # 1. 获取当前快照
            logger.info(f"开始生成{period}报告...")
            end_time = int(time.time() * 1000)
            start_time_ms = end_time - (days * 24 * 60 * 60 * 1000)

            # 2. 获取所有持仓
            holdings = self.feishu.get_all_holdings()

            if not holdings:
                logger.warning("没有持仓数据")
                return result

            # 3. 按资产类型分类
            fund_holdings = []
            crypto_holdings = []

            for item in holdings:
                fields = item.get('fields', {})
                asset_type = fields.get('资产类型')

                if asset_type == '基金':
                    fund_holdings.append(fields)
                elif asset_type == '加密货币':
                    crypto_holdings.append(fields)

            result['total_assets'] = len(fund_holdings) + len(crypto_holdings)
            result['total_funds'] = len(fund_holdings)
            result['total_cryptos'] = len(crypto_holdings)

            # 4. 统计基金表现
            fund_performance = []
            fund_total_value = 0
            fund_total_cost = 0
            fund_total_profit = 0

            for fields in fund_holdings:
                try:
                    # 资产代码
                    code_field = fields.get('资产代码')
                    if isinstance(code_field, list):
                        code = code_field[0].get('text', '') if code_field else ''
                    else:
                        code = str(code_field or '')

                    # 资产名称
                    name_field = fields.get('资产名称')
                    if isinstance(name_field, list):
                        name = name_field[0].get('text', '') if name_field else code
                    else:
                        name = str(name_field or code)

                    if not code:
                        continue

                    # 当前市值
                    value_field = fields.get('当前市值')
                    if isinstance(value_field, dict):
                        value_array = value_field.get('value', [0])
                        current_value = float(value_array[0]) if value_array else 0
                    else:
                        current_value = float(value_field or 0)

                    # 总成本
                    cost_field = fields.get('总成本')
                    if isinstance(cost_field, dict):
                        cost_array = cost_field.get('value', [0])
                        total_cost = float(cost_array[0]) if cost_array else 0
                    else:
                        total_cost = float(cost_field or 0)

                    # 收益金额
                    profit_field = fields.get('收益金额')
                    if isinstance(profit_field, dict):
                        profit_array = profit_field.get('value', [0])
                        profit = float(profit_array[0]) if profit_array else 0
                    else:
                        profit = float(profit_field or 0)

                    # 收益率
                    rate_field = fields.get('收益率')
                    if isinstance(rate_field, dict):
                        rate_array = rate_field.get('value', [0])
                        profit_rate = float(rate_array[0]) if rate_array else 0
                    else:
                        profit_rate = float(rate_field or 0)

                    # 累加总计
                    fund_total_value += current_value
                    fund_total_cost += total_cost
                    fund_total_profit += profit

                    # 获取期间涨跌幅
                    period_change_rate = self._calculate_period_change(
                        code, '基金', start_time_ms, end_time
                    )

                    # 记录资产表现
                    fund_performance.append({
                        'code': code,
                        'name': name,
                        'type': '基金',
                        'value': current_value,
                        'profit': profit,
                        'profit_rate': profit_rate,
                        'period_change_rate': period_change_rate or 0
                    })

                except Exception as e:
                    logger.error(f"处理基金 {code} 失败: {e}")
                    continue

            # 5. 统计加密货币表现
            crypto_performance = []
            crypto_total_value = 0
            crypto_total_cost = 0
            crypto_total_profit = 0

            for fields in crypto_holdings:
                try:
                    # 资产代码
                    code_field = fields.get('资产代码')
                    if isinstance(code_field, list):
                        code = code_field[0].get('text', '') if code_field else ''
                    else:
                        code = str(code_field or '')

                    # 资产名称
                    name_field = fields.get('资产名称')
                    if isinstance(name_field, list):
                        name = name_field[0].get('text', '') if name_field else code
                    else:
                        name = str(name_field or code)

                    if not code:
                        continue

                    # 当前市值
                    value_field = fields.get('当前市值')
                    if isinstance(value_field, dict):
                        value_array = value_field.get('value', [0])
                        current_value = float(value_array[0]) if value_array else 0
                    else:
                        current_value = float(value_field or 0)

                    # 总成本
                    cost_field = fields.get('总成本')
                    if isinstance(cost_field, dict):
                        cost_array = cost_field.get('value', [0])
                        total_cost = float(cost_array[0]) if cost_array else 0
                    else:
                        total_cost = float(cost_field or 0)

                    # 收益金额
                    profit_field = fields.get('收益金额')
                    if isinstance(profit_field, dict):
                        profit_array = profit_field.get('value', [0])
                        profit = float(profit_array[0]) if profit_array else 0
                    else:
                        profit = float(profit_field or 0)

                    # 收益率
                    rate_field = fields.get('收益率')
                    if isinstance(rate_field, dict):
                        rate_array = rate_field.get('value', [0])
                        profit_rate = float(rate_array[0]) if rate_array else 0
                    else:
                        profit_rate = float(rate_field or 0)

                    # 累加总计
                    crypto_total_value += current_value
                    crypto_total_cost += total_cost
                    crypto_total_profit += profit

                    # 获取期间涨跌幅
                    period_change_rate = self._calculate_period_change(
                        code, '加密货币', start_time_ms, end_time
                    )

                    # 记录资产表现
                    crypto_performance.append({
                        'code': code,
                        'name': name,
                        'type': '加密货币',
                        'value': current_value,
                        'profit': profit,
                        'profit_rate': profit_rate,
                        'period_change_rate': period_change_rate or 0
                    })

                except Exception as e:
                    logger.error(f"处理加密货币 {code} 失败: {e}")
                    continue

            # 6. 汇总数据
            result['fund_total_value'] = fund_total_value
            result['fund_total_cost'] = fund_total_cost
            result['fund_total_profit'] = fund_total_profit
            result['fund_profit_rate'] = (fund_total_profit / fund_total_cost * 100) if fund_total_cost > 0 else 0
            result['fund_performance'] = fund_performance

            result['crypto_total_value'] = crypto_total_value
            result['crypto_total_cost'] = crypto_total_cost
            result['crypto_total_profit'] = crypto_total_profit
            result['crypto_profit_rate'] = (crypto_total_profit / crypto_total_cost * 100) if crypto_total_cost > 0 else 0
            result['crypto_performance'] = crypto_performance

            result['total_value'] = fund_total_value + crypto_total_value
            result['total_cost'] = fund_total_cost + crypto_total_cost
            result['total_profit'] = fund_total_profit + crypto_total_profit
            result['profit_rate'] = (result['total_profit'] / result['total_cost'] * 100) if result['total_cost'] > 0 else 0

            # 7. 按期间涨跌幅排序 (基金和加密货币分开)
            fund_performance_sorted = sorted(
                fund_performance,
                key=lambda x: x['period_change_rate'],
                reverse=True
            )
            crypto_performance_sorted = sorted(
                crypto_performance,
                key=lambda x: x['period_change_rate'],
                reverse=True
            )

            # 8. 获取最佳/最差表现 (合并后取前5)
            all_performance = fund_performance + crypto_performance
            all_performance_sorted = sorted(
                all_performance,
                key=lambda x: x['period_change_rate'],
                reverse=True
            )

            result['top_performers'] = all_performance_sorted[:5]
            result['worst_performers'] = all_performance_sorted[-5:][::-1]
            result['fund_top_performers'] = fund_performance_sorted[:5]
            result['crypto_top_performers'] = crypto_performance_sorted[:5]

            # 9. 计算期间总涨跌
            if all_performance:
                avg_period_change = sum(a['period_change_rate'] for a in all_performance) / len(all_performance)
                result['period_change_rate'] = avg_period_change

            # 8. 发送飞书通知
            self._send_report_notification(result, period)

            # 9. 记录日志
            duration = time.time() - start_time

            try:
                self.feishu.log_sync_status(
                    source='system',
                    task_type=f'{period}_report',
                    status='success',
                    record_count=result['total_assets'],
                    error_msg=None,
                    duration=duration
                )
            except Exception as e:
                logger.error(f"记录报告日志失败: {e}")

            logger.info(f"{period}报告生成完成: {result['total_assets']}个资产, "
                       f"总市值 {result['total_value']:.2f}, "
                       f"总收益 {result['total_profit']:.2f} ({result['profit_rate']:.2f}%)")

        except Exception as e:
            logger.error(f"生成{period}报告失败: {e}")
            result['success'] = False
            result['errors'].append(str(e))

        return result

    def _calculate_period_change(self, symbol: str, asset_type: str, start_time: int, end_time: int) -> float:
        """
        计算期间涨跌幅

        :param symbol: 资产代码
        :param asset_type: 资产类型
        :param start_time: 开始时间戳
        :param end_time: 结束时间戳
        :return: 涨跌幅百分比
        """
        try:
            if asset_type == '加密货币':
                trading_pair = f"{symbol}/USDT"
                prices = self.db.get_price_history(trading_pair, start_time, end_time)
            else:
                prices = self.db.get_price_history(symbol, start_time, end_time)

            if len(prices) < 2:
                return None

            # 期初价格和期末价格
            start_price = prices[0]['price']
            end_price = prices[-1]['price']

            if start_price == 0:
                return None

            # 计算涨跌幅
            change_rate = ((end_price - start_price) / start_price) * 100

            return change_rate

        except Exception as e:
            logger.debug(f"计算 {symbol} 期间涨跌幅失败: {e}")
            return None

    def _send_report_notification(self, report: Dict, period: str):
        """
        发送报告通知到飞书

        :param report: 报告数据
        :param period: 周期类型
        """
        if not self.alert_manager.enabled:
            logger.warning("告警功能未启用,跳过发送通知")
            return

        try:
            # 标题
            period_name = "周报" if period == 'week' else "月报"
            today = date.today().strftime('%Y-%m-%d')

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
                            "content": f"**资产数量**\n{report['total_assets']} 个"
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
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**期间平均涨跌**\n{report['period_change_rate']:+.2f}%"
                }
            })

            # 基金部分
            if report.get('fund_performance'):
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

                # 基金最佳表现
                fund_top = report.get('fund_top_performers', [])
                if fund_top:
                    top_lines = []
                    for asset in fund_top:
                        emoji = "🏆" if asset == fund_top[0] else "🟢"
                        top_lines.append(
                            f"{emoji} {asset['name']}: {asset['period_change_rate']:+.2f}%"
                        )
                    elements.append({
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "**最佳表现**\n" + "\n".join(top_lines)
                        }
                    })

            # 加密货币部分
            if report.get('crypto_performance'):
                elements.append({"tag": "hr"})
                elements.append({
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**加密货币 ({report['total_cryptos']} 个)**"
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

                # 加密货币最佳表现
                crypto_top = report.get('crypto_top_performers', [])
                if crypto_top:
                    top_lines = []
                    for asset in crypto_top:
                        emoji = "🏆" if asset == crypto_top[0] else "🟢"
                        top_lines.append(
                            f"{emoji} {asset['name']}: {asset['period_change_rate']:+.2f}%"
                        )
                    elements.append({
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "**最佳表现**\n" + "\n".join(top_lines)
                        }
                    })

            # 整体最差表现
            if report.get('worst_performers'):
                elements.append({"tag": "hr"})
                worst_lines = []
                for asset in report['worst_performers']:
                    emoji = "💀" if asset == report['worst_performers'][0] else "🔴"
                    # 根据类型显示不同单位
                    if asset['type'] == '加密货币':
                        worst_lines.append(
                            f"{emoji} {asset['name']}: {asset['period_change_rate']:+.2f}%"
                        )
                    else:
                        worst_lines.append(
                            f"{emoji} {asset['name']}: {asset['period_change_rate']:+.2f}%"
                        )
                elements.append({
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": "**整体最差表现**\n" + "\n".join(worst_lines)
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
                            "content": f"{profit_emoji} 资产{period_name} - {today}"
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
                logger.info(f"{period_name}已发送到飞书")
            else:
                logger.error(f"发送{period_name}失败: {response.text}")

        except Exception as e:
            logger.error(f"发送{period_name}通知失败: {e}")


def generate_weekly_report(config_path: str = 'config.json') -> Dict:
    """
    生成周报 (便捷函数)

    :param config_path: 配置文件路径
    :return: 报告结果
    """
    config = Config(config_path)
    task = PeriodicReportTask(config)
    return task.generate_weekly_report()


def generate_monthly_report(config_path: str = 'config.json') -> Dict:
    """
    生成月报 (便捷函数)

    :param config_path: 配置文件路径
    :return: 报告结果
    """
    config = Config(config_path)
    task = PeriodicReportTask(config)
    return task.generate_monthly_report()


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

    # 生成周报
    print("\n=== 周报 ===")
    weekly_result = generate_weekly_report()
    print(f"周报结果: {weekly_result}")

    # 生成月报
    print("\n=== 月报 ===")
    monthly_result = generate_monthly_report()
    print(f"月报结果: {monthly_result}")
