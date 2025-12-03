#!/usr/bin/env python3
"""
回填交易对方脚本 (backfill_counterparty.py)

遍历飞书账本中已有的记录，如果 '交易对方' 字段为空，
则尝试从 '备注' 字段解析出交易对方，并回填到 '交易对方' 字段。
"""

import os
import sys
import time
from tqdm import tqdm
from lib.config import Config
from lib.feishu_client import FeishuClient
from core.logger import setup_logger
from loguru import logger

def extract_counterparty_from_note(note, category):
    """
    从备注中提取交易对方
    策略：
    1. 如果备注格式为 '分类-商户'，提取商户。
    2. 如果备注不等于分类且不包含 '-'，假设备注即商户。
    3. 如果备注等于分类，无法提取。
    """
    if not note:
        return None
        
    # 移除两端空格
    note = note.strip()
    category = category.strip() if category else ""
    
    # 策略 1: 分类-商户
    # 注意：有些分类本身可能包含 '-'，这里简单假设第一个 '-' 分隔分类和商户
    # 或者更严格一点：必须是以 category 开头
    if '-' in note:
        # 尝试分割
        parts = note.split('-', 1)
        if len(parts) == 2:
            candidate = parts[1].strip()
            if candidate:
                return candidate
    
    # 策略 2: 备注 != 分类 (且不含 -，或者上面的策略没匹配到)
    if note != category:
        return note
        
    return None

def backfill_ledger(config_path='config.json'):
    """回填账本"""
    
    # 1. 加载配置
    if not os.path.exists(config_path):
        logger.error(f"配置文件 {config_path} 不存在")
        return

    try:
        config = Config(config_path)
        accounts = config.data.get('accounts', {})
        mcp_server = config.data.get('mcp_server', {})
        
        app_id = mcp_server.get('app_id')
        app_secret = mcp_server.get('app_secret')

        if not app_id or not app_secret:
            logger.error("未配置 mcp_server.app_id 或 app_secret")
            return
            
    except Exception as e:
        logger.error(f"读取配置失败: {e}")
        return

    # 2. 初始化飞书客户端
    feishu = FeishuClient(app_id, app_secret)
    
    # 3. 遍历所有账本
    for account_key, account_info in accounts.items():
        app_token = account_info.get('app_token')
        table_id = account_info.get('table_id')
        account_name = account_info.get('name', account_key)
        
        if not app_token or not table_id:
            continue
            
        logger.info(f"正在处理账本: {account_name} ...")
        
        try:
            # 3.1 获取字段定义
            fields = feishu.list_fields(app_token, table_id)
            
            # DEBUG: 打印所有字段定义
            logger.debug(f"Fields in table {table_id}:")
            for f in fields:
                logger.debug(f"  - {f['field_name']} ({f['type']}): {f['field_id']}")

            counterparty_field_id = None
            category_field_id = None
            note_field_id = None
            
            for field in fields:
                if field['field_name'] == '交易对方':
                    counterparty_field_id = field['field_id']
                elif field['field_name'] == '分类':
                    category_field_id = field['field_id']
                elif field['field_name'] == '备注':
                    note_field_id = field['field_id']
            
            if not counterparty_field_id:
                logger.warning(f"账本 '{account_name}' 缺少 '交易对方' 列。请先在飞书中添加该列。")
                continue
            
            if not note_field_id:
                logger.warning(f"账本 '{account_name}' 缺少 '备注' 列，无法解析。")
                continue

            # 3.2 拉取记录
            logger.info(f"拉取所有记录...")
            all_records = []
            page_token = None
            has_more = True
            
            while has_more:
                records, page_token, has_more = feishu.list_records(app_token, table_id, page_token=page_token, page_size=500)
                all_records.extend(records)
                # 避免触发频率限制
                time.sleep(0.1)
            
            logger.info(f"共获取 {len(all_records)} 条记录，开始分析...")
            
            records_to_update = []
            
            # 3.3 分析并准备更新
            for index, record in enumerate(tqdm(all_records, desc=f"Analyzing {account_name}")):
                fields = record.get('fields', {})
                record_id = record.get('record_id')
                
                # 获取字段值 - 使用字段名而非ID，因为飞书API返回的是字段名
                current_counterparty = fields.get('交易对方')
                note = fields.get('备注')
                category = fields.get('分类')

                # DEBUG: 打印前5条记录的状态
                if index < 5:
                    logger.debug(f"Record {index}: Note='{note}', Category='{category}', Current CP='{current_counterparty}'")
                
                # 如果已有交易对方，跳过
                if current_counterparty:
                    # 如果是字符串且非空
                    if isinstance(current_counterparty, str) and current_counterparty.strip():
                        continue
                    # 如果是列表（文本字段可能返回列表）且非空
                    if isinstance(current_counterparty, list) and current_counterparty:
                        continue
                    # 如果是字典
                    if isinstance(current_counterparty, dict) and current_counterparty:
                        continue
                    pass 
                
                # 解析字段值
                note_str = str(note).strip() if note else ""
                category_str = str(category).strip() if category else ""
                
                if not note_str:
                    continue
                    
                # 提取交易对方
                extracted_counterparty = extract_counterparty_from_note(note_str, category_str)
                
                if index < 5 and not current_counterparty:
                     logger.debug(f"  -> Extracted: '{extracted_counterparty}'")

                if extracted_counterparty:
                    # 更新时也使用字段名作为key
                    records_to_update.append({
                        "record_id": record_id,
                        "fields": {
                            '交易对方': extracted_counterparty
                        }
                    })


            # 3.4 批量更新
            if records_to_update:
                logger.info(f"发现 {len(records_to_update)} 条记录需要回填，开始更新...")
                
                batch_size = 50 # 飞书批量更新限制通常较小，保险起见用 50
                for i in tqdm(range(0, len(records_to_update), batch_size), desc="Updating"):
                    batch = records_to_update[i:i+batch_size]
                    result = feishu.batch_update_records(app_token, table_id, batch)
                    
                    if result['failed'] > 0:
                        logger.warning(f"批次 {i//batch_size + 1} 有 {result['failed']} 条更新失败")
                    
                    # 避免频率限制
                    time.sleep(0.2)
                
                logger.success(f"账本 '{account_name}' 回填完成！")
            else:
                logger.info(f"账本 '{account_name}' 没有需要回填的记录。")

        except Exception as e:
            logger.error(f"处理账本 '{account_name}' 时出错: {e}")

if __name__ == '__main__':
    setup_logger(level='DEBUG')
    backfill_ledger()
