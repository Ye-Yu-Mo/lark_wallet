"""
账单解析器模块
支持支付宝和微信原始账单格式
"""
import csv
import pandas as pd
from datetime import datetime
from .smart_categorizer import SmartCategorizer


class BillParser:
    """账单解析器基类"""

    @staticmethod
    def parse(filename, source_type):
        """
        解析账单文件
        :param filename: 文件路径
        :param source_type: 来源类型 'alipay' 或 'wechat'
        :return: 记录列表
        """
        if source_type == 'alipay':
            records = AlipayParser.parse(filename)
        elif source_type == 'wechat':
            records = WechatParser.parse(filename)
        else:
            raise ValueError(f"不支持的账单类型: {source_type}")

        # 去重(基于时间戳+金额+分类)
        return BillParser.deduplicate(records)

    @staticmethod
    def deduplicate(records):
        """
        去重记录
        :param records: 原始记录列表
        :return: 去重后的记录列表
        """
        seen = set()
        unique_records = []

        for record in records:
            # 使用时间戳+金额+分类作为唯一键
            key = (record['日期'], record['金额'], record['分类'])
            if key not in seen:
                seen.add(key)
                unique_records.append(record)

        return unique_records


class AlipayParser:
    """支付宝账单解析器"""

    @staticmethod
    def parse(filename):
        """解析支付宝账单CSV (GBK编码,跳过前24行)"""
        records = []

        # 读取CSV,跳过前24行,使用GBK编码
        df = pd.read_csv(filename, encoding='gbk', skiprows=24)

        for _, row in df.iterrows():
            try:
                # 提取字段
                date_str = str(row.get('交易时间', '')).strip()
                category = str(row.get('交易分类', '')).strip()
                counterparty = str(row.get('交易对方', '')).strip()
                io_type = str(row.get('收/支', '')).strip()
                amount_str = str(row.get('金额', '0')).strip()
                status = str(row.get('交易状态', '')).strip()

                # 跳过无效数据
                if not date_str or date_str == 'nan' or '交易时间' in date_str:
                    continue

                # 跳过不成功的交易
                if '成功' not in status:
                    continue

                # 跳过"不计收支"
                if '不计' in io_type:
                    continue

                # 解析金额
                try:
                    amount = float(amount_str.replace(',', ''))
                except:
                    continue

                # 跳过0金额
                if amount == 0:
                    continue

                # 解析日期
                try:
                    dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                except:
                    continue

                # 判断收支类型
                is_income = '收入' in io_type

                # 使用智能分类器
                mapped_category = SmartCategorizer.categorize(
                    'alipay', category, counterparty, is_income
                )

                # 生成智能备注
                note = SmartCategorizer.generate_note(
                    category, counterparty, mapped_category
                )

                # 返回飞书表格式
                records.append({
                    "备注": note[:50],
                    "日期": int(dt.timestamp() * 1000),
                    "收支": '收入' if is_income else '支出',
                    "分类": mapped_category,
                    "金额": amount
                })

            except Exception as e:
                continue

        return records


class WechatParser:
    """微信账单解析器"""

    @staticmethod
    def parse(filename):
        """解析微信账单XLSX (跳过前16行)"""
        records = []

        try:
            # 读取Excel,跳过前16行
            df = pd.read_excel(filename, skiprows=16)

            for _, row in df.iterrows():
                try:
                    # 提取字段
                    date_str = str(row.get('交易时间', '')).strip()
                    category = str(row.get('交易类型', '')).strip()
                    counterparty = str(row.get('交易对方', '')).strip()
                    io_type = str(row.get('收/支', '')).strip()
                    amount_str = str(row.get('金额(元)', '0')).strip()
                    status = str(row.get('当前状态', '')).strip()

                    # 跳过无效数据
                    if not date_str or date_str == 'nan' or '交易时间' in date_str:
                        continue

                    # 跳过不成功的交易
                    if status not in ['支付成功', '已收钱', '对方已收钱', '对方已退还', '已全额退款', '已退款']:
                        continue

                    # 跳过中性交易
                    if '/' in io_type or io_type == 'nan' or not io_type:
                        continue

                    # 解析金额
                    try:
                        amount = float(amount_str.replace('¥', '').replace(',', ''))
                    except:
                        continue

                    # 跳过0金额
                    if amount == 0:
                        continue

                    # 解析日期
                    try:
                        dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                    except:
                        continue

                    # 判断收支类型
                    is_income = '收入' in io_type

                    # 使用智能分类器
                    mapped_category = SmartCategorizer.categorize(
                        'wechat', category, counterparty, is_income
                    )

                    # 生成智能备注
                    note = SmartCategorizer.generate_note(
                        category, counterparty, mapped_category
                    )

                    # 返回飞书表格式
                    records.append({
                        "备注": note[:50],
                        "日期": int(dt.timestamp() * 1000),
                        "收支": '收入' if is_income else '支出',
                        "分类": mapped_category,
                        "金额": amount
                    })

                except Exception as e:
                    continue

        except Exception as e:
            raise Exception(f"读取微信账单失败: {e}")

        return records
