#!/usr/bin/env python3
"""
学习修正脚本 (learn_corrections.py)

从飞书账本中学习用户的分类修正，并更新到本地的 corrections.json 中。
这样，下次导入时就能自动应用这些修正。
"""

import os
import json
import sys
from tqdm import tqdm
from lib.config import Config
from lib.feishu_client import FeishuClient
from lib.smart_categorizer import SmartCategorizer
from core.logger import setup_logger
from loguru import logger

def learn_from_ledger(config_path='config.json'):
    """从账本中学习分类修正"""
    
    # 1. 加载配置
    if not os.path.exists(config_path):
        logger.error(f"配置文件 {config_path} 不存在")
        return

    try:
        config = Config(config_path)
        # 兼容旧版和新版config结构，这里假设是 import.py 使用的结构
        # import.py 使用 lib.config.Config, 结构通常是 config.accounts
        accounts = config.data.get('accounts', {})
        mcp_server = config.data.get('mcp_server', {}) # 假设有这个属性或者通过 get 获取
        
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
    
    total_learned = 0
    
    # 3. 遍历所有账本
    for account_key, account_info in accounts.items():
        app_token = account_info.get('app_token')
        table_id = account_info.get('table_id')
        account_name = account_info.get('name', account_key)
        
        if not app_token or not table_id:
            continue
            
        logger.info(f"正在分析账本: {account_name} ...")
        
        try:
            # 3.1 获取字段定义，找到 "交易对方" 和 "分类" 的 field_id
            fields = feishu.list_fields(app_token, table_id)
            counterparty_field_id = None
            category_field_id = None
            
            for field in fields:
                if field['field_name'] == '交易对方':
                    counterparty_field_id = field['field_id']
                elif field['field_name'] == '分类':
                    category_field_id = field['field_id']
            
            if not counterparty_field_id:
                logger.warning(f"账本 '{account_name}' 缺少 '交易对方' 列，无法学习。请先在飞书中添加该列。")
                continue
                
            if not category_field_id:
                logger.warning(f"账本 '{account_name}' 缺少 '分类' 列，跳过。")
                continue
                
            # 3.2 拉取记录 (限制最近 1000 条，避免太慢)
            logger.info(f"拉取最近记录...")
            all_records = []
            page_token = None
            
            # 只拉取前 2 页 (每页 500 条)
            for _ in range(2):
                records, page_token, has_more = feishu.list_records(app_token, table_id, page_token=page_token, page_size=500)
                all_records.extend(records)
                if not has_more:
                    break
            
            logger.info(f"分析 {len(all_records)} 条记录...")
            
            learned_count = 0
            
            # 3.3 对比并学习
            for record in tqdm(all_records, desc=f"Learning {account_name}"):
                fields = record.get('fields', {})
                
                # 获取飞书上的值 - 使用字段名
                feishu_category = fields.get('分类')
                raw_counterparty = fields.get('交易对方')
                
                # 处理飞书返回的格式 (可能是 list, dict 或 str)
                if isinstance(feishu_category, list):
                     # 单选通常返回 ['餐饮'] 或 [{'text': '餐饮'}]，这里简化处理
                     # 实际上飞书 list_records 返回的单选值通常是 string (如果在 view 里) 或者特定结构
                     # 但根据 feishu_client 实现，这里返回的是 raw json data
                     # 飞书 API 返回单选通常是 "分类": "餐饮" (如果是 text_field_as_array=False)
                     # 但如果作为 list_records, 单选字段通常是 string
                     pass

                # 简单化：尝试提取文本
                feishu_category_str = str(feishu_category).strip() if feishu_category else ""
                counterparty_str = str(raw_counterparty).strip() if raw_counterparty else ""
                
                # 过滤无效数据
                if not counterparty_str or not feishu_category_str:
                    continue
                    
                if feishu_category_str in ['其他', '待报销']: # 不向 "其他" 学习
                    continue
                
                # 本地模拟分类
                # 注意：这里我们不传入 source_type (因为不知道是支付宝还是微信), 
                # 也不传入 original_category (因为飞书上没存原始分类，只有最终分类)
                # 这意味着我们主要依赖 '交易对方' 进行学习，这符合预期
                
                # 为了对比，我们需要一个 baseline。
                # 我们用一个空的 category 和 source_type 跑一遍 categorize，看它仅仅基于 counterparty 会分出什么
                local_prediction = SmartCategorizer.categorize(
                    source_type='unknown', 
                    category='', 
                    counterparty=counterparty_str, 
                    is_income=False # 假设是支出，收入通常比较少且固定
                )
                
                # 如果本地预测的结果 != 飞书上的结果
                # 且 飞书上的结果 是有效的 (不为空)
                if local_prediction != feishu_category_str:
                    # 只有当本地没预测对，且飞书上是有效分类时，才视为用户修正
                    # 此时，我们显式地告诉 SmartCategorizer：
                    # "对于这个商户，用户想要的是 feishu_category_str"
                    SmartCategorizer.add_correction(counterparty_str, feishu_category_str)
                    learned_count += 1
            
            logger.info(f"在账本 '{account_name}' 中学到了 {learned_count} 条新规则")
            total_learned += learned_count

        except Exception as e:
            logger.error(f"处理账本 '{account_name}' 时出错: {e}")

    logger.success(f"学习完成！共新增/更新 {total_learned} 条规则。")
    logger.info(f"规则已保存至: {SmartCategorizer.CORRECTIONS_FILE}")

if __name__ == '__main__':
    setup_logger()
    learn_from_ledger()
