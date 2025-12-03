"""
AI 财务顾问模块
使用 DeepSeek API 分析财务数据并给出建议
"""
import requests
import json
from loguru import logger
from core.config import Config

def get_financial_advice(config: Config, period_str: str, stats: dict) -> str:
    """
    获取 AI 财务建议
    
    :param config: 配置对象
    :param period_str: 期间描述 (e.g. "2025年11月")
    :param stats: 统计数据字典
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
        asset_info = f"""
**投资资产概况**:
- 总市值: ¥{asset_val:,.2f}
- 累计收益: ¥{asset_profit:+,.2f} ({asset_rate:+.2f}%)
"""

    # 整理前5大支出
    sorted_expense = sorted(stats.get('category_expense', {}).items(), key=lambda x: x[1], reverse=True)[:5]
    expense_breakdown = "\n".join([f"- {cat}: {amt:.2f}" for cat, amt in sorted_expense])
    
    prompt = f"""
你是一位专业的私人财务顾问。请根据以下月度财务数据进行简要分析并给出建议。

**时间**: {period_str}
**收支概况**:
- 总收入: ¥{income:.2f}
- 总支出: ¥{expense:.2f}
- 结余: ¥{balance:.2f}
{asset_info}
**主要支出构成 (Top 5)**:
{expense_breakdown}

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
                {"role": "system", "content": "你是一个专业的财务分析助手。请直接输出 HTML 内容，不要包含 ```html 包裹。"},
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
