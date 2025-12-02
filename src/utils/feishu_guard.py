"""飞书手动修改保护工具"""
from typing import Dict, List, Optional, Tuple

from core.database import AssetDB
from core.feishu_client import AssetFeishuClient


def prepare_holdings_payload(
    feishu: AssetFeishuClient,
    database: AssetDB,
    symbol: str,
    payload: Dict,
    record_id: Optional[str] = None
) -> Tuple[Optional[str], Dict, List[str]]:
    """根据飞书手动修改记录过滤掉需要保护的字段"""
    if not record_id:
        record_id = feishu.get_holding_record_id(symbol)
    
    sanitized_payload = dict(payload)

    if not record_id:
        return None, sanitized_payload, []

    locked_fields = database.get_locked_feishu_fields('holdings', record_id)
    if not locked_fields:
        return record_id, sanitized_payload, []

    filtered_payload = {
        key: value
        for key, value in sanitized_payload.items()
        if key not in locked_fields
    }
    blocked_fields = [key for key in sanitized_payload if key in locked_fields]

    return record_id, filtered_payload, blocked_fields
