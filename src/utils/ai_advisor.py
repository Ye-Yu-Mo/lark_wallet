"""
AI 财务顾问模块
使用 DeepSeek API 分析财务数据并给出建议
"""
import requests
import json
from loguru import logger
from core.config import Config

def get_financial_advice(config: Config, period_str: str, stats: dict, account_name: str = None) -> str:
    """
    获取 AI 财务建议

    :param config: 配置对象
    :param period_str: 期间描述 (e.g. "2025年11月")
    :param stats: 统计数据字典
    :param account_name: 账本名称 (用于判断使用哪种风格)
    :return: HTML格式的建议 (如果失败返回空字符串)
    """
    deepseek_conf = config.get_deepseek_config()
    api_key = deepseek_conf.get('api_key')
    base_url = deepseek_conf.get('base_url', 'https://api.deepseek.com')
    model = deepseek_conf.get('model', 'deepseek-chat')
    
    if not api_key:
        logger.warning("未配置 DeepSeek API Key，跳过 AI 分析")
        return ""
        
    # 构造 Prompt
    income = stats.get('income', 0)
    expense = stats.get('expense', 0)
    balance = income - expense

    # 资产数据 (如果存在)
    asset_info = ""
    if 'asset_total_value' in stats:
        asset_val = stats.get('asset_total_value', 0)
        asset_profit = stats.get('asset_total_profit', 0)
        asset_rate = stats.get('asset_profit_rate', 0)

        # 按类别统计
        asset_by_type = stats.get('asset_by_type', {})
        asset_type_lines = []
        for atype, data in sorted(asset_by_type.items(), key=lambda x: x[1]['value'], reverse=True):
            type_val = data['value']
            type_profit = data['profit']
            type_rate = (type_profit / data['cost'] * 100) if data['cost'] > 0 else 0
            percent = (type_val / asset_val * 100) if asset_val > 0 else 0
            asset_type_lines.append(f"  - {atype}: ¥{type_val:,.2f} ({percent:.1f}%) | 收益: ¥{type_profit:+,.2f} ({type_rate:+.2f}%)")

        # Top 10 持仓
        asset_details = stats.get('asset_details', [])
        asset_detail_lines = []
        for i, asset in enumerate(asset_details[:10], 1):
            asset_detail_lines.append(
                f"  {i}. {asset['name']} ({asset['type']}): ¥{asset['value']:,.2f} | "
                f"收益: ¥{asset['profit']:+,.2f} ({asset['profit_rate']:+.2f}%)"
            )

        asset_info = f"""
**投资资产概况**:
- 总市值: ¥{asset_val:,.2f}
- 累计收益: ¥{asset_profit:+,.2f} ({asset_rate:+.2f}%)

**资产类别分布**:
{chr(10).join(asset_type_lines) if asset_type_lines else "  - 无"}

**Top 10 持仓**:
{chr(10).join(asset_detail_lines) if asset_detail_lines else "  - 无"}
"""

    # 整理前10大支出 (增加更多数据)
    sorted_expense = sorted(stats.get('category_expense', {}).items(), key=lambda x: x[1], reverse=True)[:10]
    expense_breakdown = "\n".join([f"- {cat}: ¥{amt:,.2f}" for cat, amt in sorted_expense])

    # 整理收入构成
    sorted_income = sorted(stats.get('category_income', {}).items(), key=lambda x: x[1], reverse=True)[:5]
    income_breakdown = "\n".join([f"- {cat}: ¥{amt:,.2f}" for cat, amt in sorted_income]) if sorted_income else "- 无"

    # 整理支出目的
    sorted_purpose = sorted(stats.get('purpose_expense', {}).items(), key=lambda x: x[1], reverse=True)[:5]
    purpose_breakdown = "\n".join([f"- {purpose}: ¥{amt:,.2f}" for purpose, amt in sorted_purpose]) if sorted_purpose else "- 无"

    # 整理细类分布 (按金额排序)
    sorted_subcat = sorted(stats.get('subcat_expense', {}).items(), key=lambda x: x[1], reverse=True)
    subcat_breakdown = "\n".join([f"- {subcat}: ¥{amt:,.2f}" for subcat, amt in sorted_subcat]) if sorted_subcat else "- 无"

    # 统计数据
    record_count = stats.get('count', 0)
    expense_count = stats.get('expense_count', 0)
    income_count = stats.get('income_count', 0)
    avg_daily_expense = expense / 30 if expense > 0 else 0  # 假设一个月30天
    avg_expense_per_transaction = expense / expense_count if expense_count > 0 else 0
    max_expense = stats.get('max_expense', 0)
    max_expense_note = stats.get('max_expense_note', '')

    # 储蓄率
    savings_rate = (balance / income * 100) if income > 0 else 0

    # 判断使用哪种风格 (jasxu 用 Linus 风格, 其他用温和版本)
    use_linus_style = account_name and 'jasxu' in account_name.lower()

    if use_linus_style:
        # Linus 风格 prompt
        prompt = f"""# 角色定义

你是 Linus Torvalds。
只是这一次,你不是在审代码,而是在审 **财务决策、投资逻辑、个人资产结构、风险管理与经济认知**。

你维护 Linux 30 年,现在你要用同样的逻辑、纪律和残酷诚实来维护一个人的资产结构。
你不提供情绪支持、不安慰、不粉饰太平。
你的任务是：拆穿蠢思路、清理烂逻辑、阻止财务自杀行为。

## 核心财务哲学

1. **Good Taste = 财务好品味**: 复杂的财务规划等于坏设计
2. **Never break userspace**: 会让现金流断裂的投资 = Bug
3. **实用主义**: 不预测宏观,不碰你听不懂的东西
4. **简洁执念**: 复杂度是万恶之源,任何不能在30秒解释清楚的投资都属于垃圾

## 沟通风格

- 直接、硬、不废话
- 烂财务思维必须当场枪决
- 骂的是逻辑,不是人

---

# 财务数据

**时间**: {period_str}
**记录数**: {record_count} 笔 (收入 {income_count} 笔, 支出 {expense_count} 笔)

**收支概况**:
- 总收入: ¥{income:,.2f}
- 总支出: ¥{expense:,.2f}
- 结余: ¥{balance:+,.2f}
- 储蓄率: {savings_rate:.1f}%
- 日均支出: ¥{avg_daily_expense:,.2f}
- 单笔平均支出: ¥{avg_expense_per_transaction:,.2f}
- 单笔最大支出: ¥{max_expense:,.2f} ({max_expense_note})
{asset_info}
**收入构成**:
{income_breakdown}

**主要支出构成 (Top 10)**:
{expense_breakdown}

**支出目的分布**:
{purpose_breakdown}

**细类分布(支出,按金额排序)**:
{subcat_breakdown}

---

# 任务

请用 Linus 的视角审查这个月的财务状况。记住:

1. **拆穿蠢逻辑**: 如果支出结构有问题,直接指出
2. **删除复杂度**: 如果发现不必要的支出,说它是垃圾
3. **保护现金流**: 任何会破坏未来的行为都要警告
4. **保持简洁**: 最多给 3 条建议,多了执行不了

请提供以下内容(用简单的HTML格式):

## 1. 财务状况点评
- 用一两句话说清楚这个月的核心问题(如果没问题就说没问题)
- 如果有明显的财务漏洞,直接指出

## 2. 具体建议 (最多3条)
- 基于数据给出可执行的建议
- 不要空话套话
- 如果数据看起来正常,就说正常,别制造焦虑

## 3. 风险警告 (如果有)
- 如果发现会破坏未来的财务行为,必须警告
- 如果没有严重风险,不要瞎编

**重要**:
- 用简单的 HTML 标签 (如 <p>, <ul>, <li>, <b>),不要使用 Markdown
- 用简洁、直接的语言,避免财务鸡汤
- 最后必须以一句狠话收尾
"""
    else:
        # 温和版本 prompt
        prompt = f"""你是一位专业的私人财务顾问。请根据以下月度财务数据进行简要分析并给出建议。

**时间**: {period_str}
**记录数**: {record_count} 笔 (收入 {income_count} 笔, 支出 {expense_count} 笔)

**收支概况**:
- 总收入: ¥{income:,.2f}
- 总支出: ¥{expense:,.2f}
- 结余: ¥{balance:+,.2f}
- 储蓄率: {savings_rate:.1f}%
- 日均支出: ¥{avg_daily_expense:,.2f}
- 单笔平均支出: ¥{avg_expense_per_transaction:,.2f}
- 单笔最大支出: ¥{max_expense:,.2f} ({max_expense_note})
{asset_info}
**收入构成**:
{income_breakdown}

**主要支出构成 (Top 10)**:
{expense_breakdown}

**支出目的分布**:
{purpose_breakdown}

**细类分布(支出,按金额排序)**:
{subcat_breakdown}

**要求**:
1. 用简练的语言点评本月财务状况（结合收支和投资表现）。
2. 给出 3 条具体的理财或消费建议。
3. 语气专业、客观、鼓励性。
4. 输出格式要求：使用简单的 HTML 标签 (如 <p>, <ul>, <li>, <b>)，不要使用 Markdown 代码块。
"""

    try:
        url = f"{base_url.rstrip('/')}/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        data = {
            "model": model,
            "messages": [
                {"role": "system", "content": "你是 Linus Torvalds,现在负责审查财务数据。用直接、硬核的风格指出问题,不粉饰太平。请直接输出 HTML 内容,不要包含 ```html 包裹。" if use_linus_style else "你是一个专业的财务分析助手。请直接输出 HTML 内容,不要包含 ```html 包裹。"},
                {"role": "user", "content": prompt}
            ],
            "stream": False,
            "temperature": 0.7
        }
        
        logger.info(f"正在请求 DeepSeek AI 分析 ({model})...")
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"DeepSeek API 请求失败: {response.status_code} - {response.text}")
            return ""
            
        result = response.json()
        content = result['choices'][0]['message']['content']
        
        # 清理可能的 markdown 标记 (以防万一)
        content = content.replace('```html', '').replace('```', '').strip()
        
        return content

    except Exception as e:
        logger.error(f"获取 AI 建议时发生异常: {e}")
        return ""
