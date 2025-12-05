"""
健康数据飞书客户端
读取和管理健康相关的飞书多维表数据
"""
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from loguru import logger

from core.feishu_client import FeishuClient


class HealthFeishuClient:
    """
    健康数据飞书客户端

    负责读取个人健康档案、每日健康记录、饮食记录、
    运动记录、冰箱库存和生活模式配置
    """

    def __init__(self, app_id: str, app_secret: str, app_token: str, table_ids: Dict[str, str]):
        """
        初始化客户端

        :param app_id: 飞书应用ID
        :param app_secret: 飞书应用密钥
        :param app_token: Base应用token
        :param table_ids: 表ID字典
        """
        self.feishu = FeishuClient(app_id, app_secret)
        self.app_token = app_token
        self.table_ids = table_ids

    def get_health_profile(self) -> Optional[Dict]:
        """
        获取个人健康档案

        :return: 健康档案数据(取第一条记录)
        """
        try:
            table_id = self.table_ids['health_profile']
            records = self.feishu.search_records(
                app_token=self.app_token,
                table_id=table_id,
                page_size=1
            )

            if records:
                return records[0].get('fields', {})

            logger.warning("未找到健康档案数据")
            return None

        except Exception as e:
            logger.error(f"获取健康档案失败: {e}")
            return None

    def get_recent_health_records(self, days: int = 5) -> List[Dict]:
        """
        获取最近N天的健康记录

        :param days: 天数
        :return: 健康记录列表
        """
        try:
            table_id = self.table_ids['daily_health']

            # 计算开始日期
            start_date = datetime.now() - timedelta(days=days)
            start_timestamp = int(start_date.timestamp() * 1000)

            # 拉取所有记录 (不使用筛选,本地过滤)
            all_records = []
            page_token = None

            while True:
                items, next_token, has_more = self.feishu.list_records(
                    app_token=self.app_token,
                    table_id=table_id,
                    page_token=page_token,
                    page_size=500
                )
                all_records.extend(items)

                if not has_more:
                    break
                page_token = next_token

            # 本地过滤最近N天的记录
            filtered = []
            for record in all_records:
                fields = record.get('fields', {})
                date_value = fields.get('日期')

                if date_value and date_value >= start_timestamp:
                    filtered.append(fields)

            # 按日期排序
            filtered.sort(key=lambda x: x.get('日期', 0))

            return filtered

        except Exception as e:
            logger.error(f"获取健康记录失败: {e}")
            return []

    def get_recent_meals(self, days: int = 5) -> List[Dict]:
        """
        获取最近N天的饮食记录

        :param days: 天数
        :return: 饮食记录列表
        """
        try:
            table_id = self.table_ids['meal_log']

            # 计算开始日期
            start_date = datetime.now() - timedelta(days=days)
            start_timestamp = int(start_date.timestamp() * 1000)

            # 拉取所有记录
            all_records = []
            page_token = None

            while True:
                items, next_token, has_more = self.feishu.list_records(
                    app_token=self.app_token,
                    table_id=table_id,
                    page_token=page_token,
                    page_size=500
                )
                all_records.extend(items)

                if not has_more:
                    break
                page_token = next_token

            # 本地过滤
            filtered = []
            for record in all_records:
                fields = record.get('fields', {})
                date_value = fields.get('日期')

                if date_value and date_value >= start_timestamp:
                    filtered.append(fields)

            # 按日期排序
            filtered.sort(key=lambda x: x.get('日期', 0))

            return filtered

        except Exception as e:
            logger.error(f"获取饮食记录失败: {e}")
            return []

    def get_recent_exercises(self, days: int = 5) -> List[Dict]:
        """
        获取最近N天的运动记录

        :param days: 天数
        :return: 运动记录列表
        """
        try:
            table_id = self.table_ids['exercise_log']

            # 计算开始日期
            start_date = datetime.now() - timedelta(days=days)
            start_timestamp = int(start_date.timestamp() * 1000)

            # 拉取所有记录
            all_records = []
            page_token = None

            while True:
                items, next_token, has_more = self.feishu.list_records(
                    app_token=self.app_token,
                    table_id=table_id,
                    page_token=page_token,
                    page_size=500
                )
                all_records.extend(items)

                if not has_more:
                    break
                page_token = next_token

            # 本地过滤
            filtered = []
            for record in all_records:
                fields = record.get('fields', {})
                date_value = fields.get('日期')

                if date_value and date_value >= start_timestamp:
                    filtered.append(fields)

            # 按日期排序
            filtered.sort(key=lambda x: x.get('日期', 0))

            return filtered

        except Exception as e:
            logger.error(f"获取运动记录失败: {e}")
            return []

    def get_fridge_inventory(self) -> List[Dict]:
        """
        获取冰箱库存

        :return: 库存列表
        """
        try:
            table_id = self.table_ids['fridge_inventory']

            # 拉取所有记录
            all_records = []
            page_token = None

            while True:
                items, next_token, has_more = self.feishu.list_records(
                    app_token=self.app_token,
                    table_id=table_id,
                    page_token=page_token,
                    page_size=500
                )
                all_records.extend(items)

                if not has_more:
                    break
                page_token = next_token

            return [r.get('fields', {}) for r in all_records]

        except Exception as e:
            logger.error(f"获取冰箱库存失败: {e}")
            return []

    def get_expiring_ingredients(self, days: int = 3) -> List[Dict]:
        """
        获取即将过期的食材(N天内)

        :param days: 天数
        :return: 即将过期的食材列表
        """
        try:
            table_id = self.table_ids['fridge_inventory']

            # 计算截止日期
            end_date = datetime.now() + timedelta(days=days)
            end_timestamp = int(end_date.timestamp() * 1000)

            # 拉取所有记录
            all_records = []
            page_token = None

            while True:
                items, next_token, has_more = self.feishu.list_records(
                    app_token=self.app_token,
                    table_id=table_id,
                    page_token=page_token,
                    page_size=500
                )
                all_records.extend(items)

                if not has_more:
                    break
                page_token = next_token

            # 本地过滤即将过期的
            filtered = []
            for record in all_records:
                fields = record.get('fields', {})
                expire_date = fields.get('过期日期')

                if expire_date and expire_date <= end_timestamp:
                    filtered.append(fields)

            # 按过期日期排序
            filtered.sort(key=lambda x: x.get('过期日期', 0))

            return filtered

        except Exception as e:
            logger.error(f"获取即将过期食材失败: {e}")
            return []

    def get_today_lifestyle(self) -> Optional[Dict]:
        """
        获取今天的生活模式配置

        :return: 今天的配置
        """
        try:
            table_id = self.table_ids['lifestyle_config']

            # 今天0点的时间戳
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_timestamp = int(today.timestamp() * 1000)

            records = self.feishu.search_records(
                app_token=self.app_token,
                table_id=table_id,
                filter_conditions={
                    "conjunction": "and",
                    "conditions": [
                        {
                            "field_name": "日期",
                            "operator": "is",
                            "value": [str(today_timestamp)]
                        }
                    ]
                },
                page_size=1
            )

            if records:
                return records[0].get('fields', {})

            logger.info("未找到今天的生活模式配置")
            return None

        except Exception as e:
            logger.error(f"获取生活模式配置失败: {e}")
            return None
