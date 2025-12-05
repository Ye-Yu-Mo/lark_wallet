"""
月度财务报告任务
统计上个月的收支情况并发送邮件报告
"""
import time
import calendar
from datetime import datetime, date, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple
from loguru import logger

from core.config import Config
from lib.feishu_client import FeishuClient
from core.feishu_client import AssetFeishuClient
from utils.alert import AlertManager
from utils.ai_advisor import get_financial_advice


class MonthlyReportTask:
    """
    月度报告任务
    """

    def __init__(self, config: Config):
        """初始化"""
        self.config = config
        
        # 初始化飞书客户端 (用于读取账本)
        mcp_config = config.data.get('mcp_server', {})
        self.feishu = FeishuClient(
            app_id=mcp_config.get('app_id'),
            app_secret=mcp_config.get('app_secret')
        )
        
        # 初始化资产客户端 (用于读取持仓)
        self.asset_feishu = None
        if config.is_asset_sync_enabled():
            try:
                feishu_conf = config.get_feishu_config()
                self.asset_feishu = AssetFeishuClient(
                    app_id=feishu_conf['app_id'],
                    app_secret=feishu_conf['app_secret'],
                    app_token=feishu_conf['app_token'],
                    table_ids=feishu_conf['tables']
                )
            except Exception as e:
                logger.warning(f"初始化资产客户端失败: {e}")
        
        # 初始化告警管理器 (用于发邮件)
        # 注意：这里我们需要全局的 alert 配置来初始化 SMTP
        # 通常在 asset_sync.alerts 里
        asset_sync = config.get_asset_sync_config()
        alert_config = asset_sync.get('alerts', {})
        self.alert_manager = AlertManager(
            webhook_url=alert_config.get('feishu_webhook', ''),
            email_config=alert_config.get('email'),
            enabled=True # 强制启用，具体发不发取决于有没有收件人
        )

    def run(self, target_year: int = None, target_month: int = None):
        """
        执行月度报告任务
        :param target_year: 目标年份 (默认上个月的年份)
        :param target_month: 目标月份 (默认上个月)
        """
        logger.info("开始执行月度报告任务...")
        
        # 确定目标月份
        if target_year is None or target_month is None:
            today = date.today()
            # 上个月
            first = today.replace(day=1)
            last_month = first - timedelta(days=1)
            target_year = last_month.year
            target_month = last_month.month
            
        logger.info(f"目标月份: {target_year}-{target_month:02d}")
        
        # 计算时间范围 (毫秒时间戳)
        start_dt = datetime(target_year, target_month, 1)
        # 下个月第1天
        if target_month == 12:
            next_month_dt = datetime(target_year + 1, 1, 1)
        else:
            next_month_dt = datetime(target_year, target_month + 1, 1)
            
        end_dt = next_month_dt - timedelta(milliseconds=1)
        
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)
        
        logger.debug(f"时间范围: {start_dt} ~ {end_dt}")
        
        # 遍历所有账本
        accounts = self.config.data.get('accounts', {})
        for account_key, account_info in accounts.items():
            report_emails = account_info.get('report_emails')
            if not report_emails:
                logger.info(f"账本 '{account_key}' 未配置 report_emails，跳过")
                continue
                
            logger.info(f"正在生成账本 '{account_key}' 的月报...")
            self.generate_account_report(
                account_key, 
                account_info, 
                report_emails, 
                start_ts, 
                end_ts,
                (target_year, target_month)
            )

    def generate_account_report(self, account_name, account_info, recipients, start_ts, end_ts, period):
        """生成并发送单个账本的报告"""
        app_token = account_info.get('app_token')
        table_id = account_info.get('table_id')
        display_name = account_info.get('name', account_name)
        currency = account_info.get('currency', '¥')  # 获取货币符号，默认人民币

        # 从period元组中提取年份和月份
        target_year, target_month = period
        
        try:
            # 1. 获取字段定义 (找到 日期, 收支, 分类, 金额 等字段)
            fields_def = self.feishu.list_fields(app_token, table_id)
            field_map = {f['field_name']: f['field_id'] for f in fields_def}
            
            # 检查必要字段
            required = ['日期', '收支', '分类', '金额']
            for req in required:
                if req not in field_map:
                    logger.error(f"账本 '{account_name}' 缺少字段 '{req}'，无法生成报告")
                    return

            # 2. 拉取数据 (优化: 使用月份字段筛选)
            # 月份字段格式: "12 月"
            month_str = f"{target_month} 月"

            # 尝试使用月份字段筛选 (如果失败则全量拉取)
            try:
                logger.info(f"使用月份字段筛选: {month_str}")
                all_records_raw = self.feishu.search_records(
                    app_token=app_token,
                    table_id=table_id,
                    filter_conditions={
                        "conjunction": "and",
                        "conditions": [{
                            "field_name": "月份",
                            "operator": "is",
                            "value": [month_str]
                        }]
                    },
                    page_size=500
                )
                # search_records返回的是列表，需要转换成list_records的格式
                all_records = all_records_raw
                logger.info(f"通过月份筛选获取到 {len(all_records)} 条记录")
            except Exception as e:
                logger.warning(f"月份筛选失败 ({e})，改用全量拉取")
                # 回退到全量拉取
                all_records = []
                page_token = None
                has_more = True

                while has_more:
                    records, page_token, has_more = self.feishu.list_records(app_token, table_id, page_token=page_token, page_size=500)
                    all_records.extend(records)
                    time.sleep(0.1)
                
            # 3. 过滤和统计
            stats = {
                'income': 0.0,
                'expense': 0.0,
                'category_expense': defaultdict(float),
                'category_income': defaultdict(float),
                'purpose_expense': defaultdict(float),  # 新增：按支出目的统计
                'subcat_expense': defaultdict(float),  # 新增：按细类统计
                'count': 0,
                'expense_count': 0,  # 新增：支出笔数
                'income_count': 0,   # 新增：收入笔数
                'max_expense': 0.0,  # 新增：单笔最大支出
                'max_expense_note': '',  # 新增：最大支出备注
            }
            
            for record in all_records:
                fields = record.get('fields', {})
                
                # 日期检查
                date_val = fields.get('日期')
                if not isinstance(date_val, (int, float)):
                    continue
                    
                if not (start_ts <= date_val <= end_ts):
                    continue
                    
                # 统计
                stats['count'] += 1
                
                # 金额
                amount_val = fields.get('金额')
                try:
                    amount = float(amount_val) if amount_val is not None else 0.0
                except:
                    amount = 0.0
                    
                # 收支类型
                io_type = str(fields.get('收支', '')).strip()
                category = str(fields.get('分类', '其他')).strip()
                purpose = str(fields.get('支出目的', '')).strip()

                # 处理备注（可能是数组格式）
                note_field = fields.get('备注', '')
                if isinstance(note_field, list):
                    note = note_field[0].get('text', '') if note_field else ''
                else:
                    note = str(note_field).strip()

                # 处理细类（可能是数组格式）
                subcat_field = fields.get('细类', '')
                if isinstance(subcat_field, list):
                    subcat = subcat_field[0].get('text', '') if subcat_field else ''
                else:
                    subcat = str(subcat_field).strip()

                if io_type == '支出':
                    stats['expense'] += amount
                    stats['expense_count'] += 1
                    stats['category_expense'][category] += amount

                    # 记录支出目的
                    if purpose:
                        stats['purpose_expense'][purpose] += amount

                    # 记录细类
                    if subcat:
                        stats['subcat_expense'][subcat] += amount

                    # 记录最大支出
                    if amount > stats['max_expense']:
                        stats['max_expense'] = amount
                        stats['max_expense_note'] = note

                elif io_type == '收入':
                    stats['income'] += amount
                    stats['income_count'] += 1
                    stats['category_income'][category] += amount
            
            # 3.1 获取资产数据 (仅针对特定账号)
            # 这里简单写死 'jasxu'，也可以在config里加标记
            if account_name == 'jasxu' and self.asset_feishu:
                try:
                    holdings = self.asset_feishu.get_all_holdings()
                    total_val = 0.0
                    total_profit = 0.0
                    total_cost = 0.0

                    # 按资产类别统计
                    asset_by_type = defaultdict(lambda: {'value': 0.0, 'profit': 0.0, 'cost': 0.0})
                    asset_details = []  # 存储每个资产的详细信息

                    for h in holdings:
                        fields = h.get('fields', {})
                        # 解析数值 (飞书字段可能是 list/dict/number)

                        def parse_num(v):
                            if isinstance(v, (int, float)): return float(v)
                            if isinstance(v, list) and v: return parse_num(v[0])
                            if isinstance(v, dict): return parse_num(v.get('value') or v.get('text'))
                            return 0.0

                        def parse_text(v):
                            if isinstance(v, str): return v
                            if isinstance(v, list) and v: return parse_text(v[0])
                            if isinstance(v, dict): return str(v.get('text', ''))
                            return str(v) if v else ''

                        val = parse_num(fields.get('当前市值'))
                        profit = parse_num(fields.get('收益金额'))
                        cost = parse_num(fields.get('总成本'))
                        asset_type = parse_text(fields.get('资产类别'))
                        asset_name = parse_text(fields.get('资产名称'))
                        profit_rate = parse_num(fields.get('收益率'))

                        total_val += val
                        total_profit += profit
                        total_cost += cost

                        # 按类别统计
                        if asset_type:
                            asset_by_type[asset_type]['value'] += val
                            asset_by_type[asset_type]['profit'] += profit
                            asset_by_type[asset_type]['cost'] += cost

                        # 记录详细信息（只记录有价值的资产）
                        if val > 0:
                            asset_details.append({
                                'name': asset_name,
                                'type': asset_type,
                                'value': val,
                                'profit': profit,
                                'profit_rate': profit_rate
                            })

                    stats['asset_total_value'] = total_val
                    stats['asset_total_profit'] = total_profit
                    stats['asset_profit_rate'] = (total_profit / total_cost * 100) if total_cost > 0 else 0
                    stats['asset_by_type'] = dict(asset_by_type)
                    stats['asset_details'] = sorted(asset_details, key=lambda x: x['value'], reverse=True)[:10]  # Top 10
                    logger.info(f"已获取资产数据: 市值 {total_val}, 收益 {total_profit}, 持仓数 {len(asset_details)}")

                except Exception as e:
                    logger.error(f"获取资产数据失败: {e}")

            # 4. 获取 AI 建议 (新增)
            period_str = f"{period[0]}年{period[1]}月"
            ai_advice = get_financial_advice(self.config, period_str, stats, account_name=account_name)

            # 5. 生成报告
            html_content = self._render_html(display_name, period, stats, ai_advice, currency)
            
            # 6. 发送邮件
            subject = f"{period[0]}年{period[1]}月财务报告 - {display_name}"
            if self.alert_manager.send_email(subject, html_content, recipients=recipients):
                logger.info(f"账本 '{account_name}' 月报发送成功")
            else:
                logger.error(f"账本 '{account_name}' 月报发送失败")

        except Exception as e:
            logger.error(f"生成账本 '{account_name}' 月报失败: {e}")

    def _render_html(self, account_name, period, stats, ai_advice="", currency="¥"):
        """渲染HTML报告"""
        year, month = period
        balance = stats['income'] - stats['expense']
        
        # 排序分类
        sorted_expense = sorted(stats['category_expense'].items(), key=lambda x: x[1], reverse=True)
        sorted_income = sorted(stats['category_income'].items(), key=lambda x: x[1], reverse=True)
        
        # 简单的 CSS
        style = """
        <style>
            body { font-family: Arial, sans-serif; color: #333; max_width: 800px; margin: 0 auto; padding: 20px; }
            h1 { color: #2c3e50; text-align: center; border-bottom: 2px solid #eee; padding-bottom: 10px; }
            .summary { display: flex; justify-content: space-between; background: #f9f9f9; padding: 20px; border-radius: 8px; margin-bottom: 30px; }
            .summary-item { text-align: center; }
            .summary-val { font-size: 24px; font-weight: bold; margin-top: 5px; }
            .income { color: #27ae60; }
            .expense { color: #c0392b; }
            .balance { color: #2980b9; }
            table { width: 100%; border-collapse: collapse; margin-bottom: 20px; }
            th, td { padding: 12px 15px; text-align: left; border-bottom: 1px solid #ddd; }
            th { background-color: #f8f9fa; font-weight: 600; }
            tr:hover { background-color: #f5f5f5; }
            .bar-container { width: 100px; background: #eee; height: 10px; border-radius: 5px; display: inline-block; }
            .bar { height: 100%; border-radius: 5px; }
            .footer { margin-top: 40px; text-align: center; color: #999; font-size: 12px; }
            .ai-section { background-color: #f0f7ff; border-left: 5px solid #3498db; padding: 15px; margin-bottom: 30px; border-radius: 4px; }
            .ai-title { font-weight: bold; color: #2980b9; margin-bottom: 10px; font-size: 16px; display: flex; align-items: center; }
            .ai-content { line-height: 1.6; }
        </style>
        """
        
        # 构建 HTML
        html = f"""
        <html>
        <head>{style}</head>
        <body>
            <h1>{account_name} - {year}年{month}月 财务报告</h1>
        """

        # 插入 AI 建议
        if ai_advice:
            html += f"""
            <div class="ai-section">
                <div class="ai-title">🤖 AI 财务顾问分析</div>
                <div class="ai-content">{ai_advice}</div>
            </div>
            """

        html += f"""
            <div class="summary">
                <div class="summary-item">
                    <div>总收入</div>
                    <div class="summary-val income">+{currency}{stats['income']:,.2f}</div>
                </div>
                <div class="summary-item">
                    <div>总支出</div>
                    <div class="summary-val expense">-{currency}{stats['expense']:,.2f}</div>
                </div>
                <div class="summary-item">
                    <div>结余</div>
                    <div class="summary-val balance">{currency}{balance:,.2f}</div>
                </div>
            </div>
            
            <h3>支出构成 ({len(sorted_expense)} 类)</h3>
            <table>
                <thead>
                    <tr>
                        <th>分类</th>
                        <th>金额</th>
                        <th>占比</th>
                        <th>图示</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        for cat, amt in sorted_expense:
            percent = (amt / stats['expense'] * 100) if stats['expense'] > 0 else 0
            html += f"""
                    <tr>
                        <td>{cat}</td>
                        <td>{currency}{amt:,.2f}</td>
                        <td>{percent:.1f}%</td>
                        <td>
                            <div class="bar-container">
                                <div class="bar" style="width: {percent}%; background-color: #c0392b;"></div>
                            </div>
                        </td>
                    </tr>
            """
            
        html += """
                </tbody>
            </table>
        """

        # 支出目的分布
        sorted_purpose = sorted(stats.get('purpose_expense', {}).items(), key=lambda x: x[1], reverse=True)
        if sorted_purpose:
            html += f"""
            <h3>支出目的分布 ({len(sorted_purpose)} 类)</h3>
            <table>
                <thead>
                    <tr><th>支出目的</th><th>金额</th><th>占比</th></tr>
                </thead>
                <tbody>
            """
            for purpose, amt in sorted_purpose:
                percent = (amt / stats['expense'] * 100) if stats['expense'] > 0 else 0
                html += f"""
                    <tr>
                        <td>{purpose}</td>
                        <td>{currency}{amt:,.2f}</td>
                        <td>{percent:.1f}%</td>
                    </tr>
                """
            html += """
                </tbody>
            </table>
            """

        # 细类分布(支出)
        sorted_subcat = sorted(stats.get('subcat_expense', {}).items(), key=lambda x: x[1], reverse=True)
        if sorted_subcat:
            html += f"""
            <h3>细类分布(支出) ({len(sorted_subcat)} 类)</h3>
            <table>
                <thead>
                    <tr><th>细类</th><th>金额</th><th>占比</th></tr>
                </thead>
                <tbody>
            """
            for subcat, amt in sorted_subcat:
                percent = (amt / stats['expense'] * 100) if stats['expense'] > 0 else 0
                html += f"""
                    <tr>
                        <td>{subcat}</td>
                        <td>{currency}{amt:,.2f}</td>
                        <td>{percent:.1f}%</td>
                    </tr>
                """
            html += """
                </tbody>
            </table>
            """

        # 收入构成
        if sorted_income:
            html += """
            <h3>收入构成</h3>
            <table>
                <thead>
                    <tr><th>分类</th><th>金额</th><th>占比</th></tr>
                </thead>
                <tbody>
            """
            for cat, amt in sorted_income:
                percent = (amt / stats['income'] * 100) if stats['income'] > 0 else 0
                html += f"""
                    <tr>
                        <td>{cat}</td>
                        <td>{currency}{amt:,.2f}</td>
                        <td>{percent:.1f}%</td>
                    </tr>
                """
            html += """
                </tbody>
            </table>
            """

        # 投资组合（仅jasxu）
        if 'asset_total_value' in stats:
            asset_details = stats.get('asset_details', [])
            asset_by_type = stats.get('asset_by_type', {})

            html += f"""
            <h3>投资组合概览</h3>
            <div class="summary" style="margin-bottom: 20px;">
                <div class="summary-item">
                    <div>总市值</div>
                    <div class="summary-val balance">{currency}{stats['asset_total_value']:,.2f}</div>
                </div>
                <div class="summary-item">
                    <div>累计收益</div>
                    <div class="summary-val {'income' if stats['asset_total_profit'] >= 0 else 'expense'}">
                        {'+' if stats['asset_total_profit'] >= 0 else '-'}{currency}{abs(stats['asset_total_profit']):,.2f}
                    </div>
                </div>
                <div class="summary-item">
                    <div>收益率</div>
                    <div class="summary-val {'income' if stats['asset_profit_rate'] >= 0 else 'expense'}">
                        {stats['asset_profit_rate']:+.2f}%
                    </div>
                </div>
            </div>
            """

            # 资产类别分布
            if asset_by_type:
                html += """
                <h4>资产类别分布</h4>
                <table>
                    <thead>
                        <tr><th>类别</th><th>市值</th><th>占比</th><th>收益</th><th>收益率</th></tr>
                    </thead>
                    <tbody>
                """
                for atype, data in sorted(asset_by_type.items(), key=lambda x: x[1]['value'], reverse=True):
                    type_val = data['value']
                    type_profit = data['profit']
                    type_rate = (type_profit / data['cost'] * 100) if data['cost'] > 0 else 0
                    percent = (type_val / stats['asset_total_value'] * 100) if stats['asset_total_value'] > 0 else 0
                    html += f"""
                        <tr>
                            <td>{atype}</td>
                            <td>{currency}{type_val:,.2f}</td>
                            <td>{percent:.1f}%</td>
                            <td class="{'income' if type_profit >= 0 else 'expense'}">{currency}{type_profit:+,.2f}</td>
                            <td class="{'income' if type_rate >= 0 else 'expense'}">{type_rate:+.2f}%</td>
                        </tr>
                    """
                html += """
                    </tbody>
                </table>
                """

            # Top 10 持仓
            if asset_details:
                html += """
                <h4>Top 10 持仓</h4>
                <table>
                    <thead>
                        <tr><th>资产名称</th><th>类别</th><th>市值</th><th>收益</th><th>收益率</th></tr>
                    </thead>
                    <tbody>
                """
                for asset in asset_details:
                    html += f"""
                        <tr>
                            <td>{asset['name']}</td>
                            <td>{asset['type']}</td>
                            <td>{currency}{asset['value']:,.2f}</td>
                            <td class="{'income' if asset['profit'] >= 0 else 'expense'}">{currency}{asset['profit']:+,.2f}</td>
                            <td class="{'income' if asset['profit_rate'] >= 0 else 'expense'}">{asset['profit_rate']:+.2f}%</td>
                        </tr>
                    """
                html += """
                    </tbody>
                </table>
                """

        html += f"""
            <div class="footer">
                生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 由 Feishu Asset Sync & DeepSeek AI 生成
            </div>
        </body>
        </html>
        """
        
        return html


def send_monthly_report(config_path: str = 'config.json'):
    """便捷入口"""
    from core.logger import setup_logger
    # setup_logger() # 如果外部没调，这里可以调。但通常 main.py 会调。
    
    config = Config(config_path)
    task = MonthlyReportTask(config)
    task.run()

if __name__ == '__main__':
    # 测试运行
    from core.logger import setup_logger
    setup_logger()
    send_monthly_report()
