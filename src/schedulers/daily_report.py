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
            email_config=alert_config.get('email'),
            enabled=alert_config.get('enabled', False)
        )

        logger.info("DailyReportTask 初始化完成")

    # ... (中间代码保持不变, 直到 _send_report_notification)

    def _generate_html_report(self, report: Dict) -> str:
        """生成HTML格式报告"""
        style = """
        <style>
            table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #f2f2f2; }
            .profit { color: red; }
            .loss { color: green; }
            .header { margin-bottom: 20px; }
            .summary { display: flex; justify-content: space-between; margin-bottom: 20px; }
        </style>
        """
        
        color_class = lambda x: "profit" if x >= 0 else "loss"
        
        html = f"""
        <html>
        <head>{style}</head>
        <body>
            <div class="header">
                <h2>资产日报 - {report['date']}</h2>
            </div>
            
            <div class="summary">
                <p><strong>总资产:</strong> ¥{report['total_value']:,.2f}</p>
                <p><strong>总收益:</strong> <span class="{color_class(report['total_profit'])}">¥{report['total_profit']:+,.2f} ({report['profit_rate']:+.2f}%)</span></p>
            </div>

            <h3>基金明细 ({report.get('total_funds', 0)})</h3>
            <table>
                <tr><th>名称</th><th>市值</th><th>收益</th><th>收益率</th></tr>
                {"".join([f"<tr><td>{item['name']}</td><td>¥{item['value']:,.2f}</td><td class='{color_class(item['profit'])}'>¥{item['profit']:+,.2f}</td><td class='{color_class(item['profit_rate'])}'>{item['profit_rate']:+.2f}%</td></tr>" for item in sorted(report.get('fund_details', []), key=lambda x: x['profit'], reverse=True)])}
            </table>

            <h3>加密货币明细 ({report.get('total_cryptos', 0)})</h3>
            <table>
                <tr><th>名称</th><th>市值</th><th>收益</th><th>收益率</th></tr>
                {"".join([f"<tr><td>{item['name']}</td><td>${item['value']:,.2f}</td><td class='{color_class(item['profit'])}'>${item['profit']:+,.2f}</td><td class='{color_class(item['profit_rate'])}'>{item['profit_rate']:+.2f}%</td></tr>" for item in sorted(report.get('crypto_details', []), key=lambda x: x['profit'], reverse=True)])}
            </table>
        </body>
        </html>
        """
        return html

    def _send_report_notification(self, report: Dict):
        """
        发送报告通知到飞书和邮件

        :param report: 报告数据
        """
        if not self.alert_manager.enabled:
            logger.warning("告警功能未启用,跳过发送通知")
            return

        try:
            # 1. 发送飞书卡片
            yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
            profit_emoji = "📈" if report['total_profit'] >= 0 else "📉"

            # ... (构建元素的逻辑保持不变) ...
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

            card_content = {
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

            # 发送飞书
            if self.alert_manager.send_feishu_card(card_content):
                logger.info("每日报告已发送到飞书")
            else:
                logger.warning("发送飞书报告失败")

            # 2. 发送邮件
            html_report = self._generate_html_report(report)
            if self.alert_manager.send_email(f"资产日报 {yesterday}", html_report):
                logger.info("每日报告已发送到邮件")
            else:
                logger.warning("发送邮件报告失败")

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
