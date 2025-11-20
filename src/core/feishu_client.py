"""
飞书API客户端 - 资产同步专用
继承lib/feishu_client.py的功能,扩展资产表操作
"""
import time
from typing import Dict, List, Optional
from lib.feishu_client import FeishuClient


class AssetFeishuClient(FeishuClient):
    """资产同步专用的飞书客户端"""

    def __init__(self, app_id: str, app_secret: str, app_token: str, table_ids: Dict[str, str]):
        """
        初始化资产同步客户端

        :param app_id: 飞书应用ID
        :param app_secret: 飞书应用密钥
        :param app_token: 多维表app_token
        :param table_ids: 表ID字典 {'holdings': 'tblXXX', 'history': 'tblYYY', 'logs': 'tblZZZ'}
        """
        super().__init__(app_id, app_secret)
        self.app_token = app_token
        self.holdings_table_id = table_ids['holdings']
        self.history_table_id = table_ids['history']
        self.logs_table_id = table_ids['logs']

    # ===== 持仓表操作 =====

    def get_holding_record_id(self, symbol: str) -> Optional[str]:
        """
        通过symbol查找持仓记录的record_id

        :param symbol: 资产代码
        :return: record_id,不存在返回None
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.holdings_table_id}/records/search"

        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token()}",
            "Content-Type": "application/json"
        }

        # 搜索条件: 资产代码字段匹配
        data = {
            "field_names": ["资产代码"],
            "filter": {
                "conjunction": "and",
                "conditions": [
                    {
                        "field_name": "资产代码",
                        "operator": "is",
                        "value": [symbol]
                    }
                ]
            }
        }

        try:
            result = self._api_call_with_retry(url, headers, data, max_retries=2)

            if result.get("code") == 0:
                items = result.get("data", {}).get("items", [])
                if items:
                    return items[0]["record_id"]

            return None

        except Exception:
            return None

    def update_holding(self, symbol: str, data: Dict) -> bool:
        """
        更新持仓记录 (存在则更新,不存在则创建)

        :param symbol: 资产代码
        :param data: 字段数据字典,例如:
            {
                '持仓数量': 1000,
                '当前价格': 0.38,
                '当前市值': 380,
                '收益金额': 50,
                '收益率': 15.2,
                '最后更新时间': 1700000000000,
                '更新状态': '成功'
            }
        :return: 是否成功
        """
        # 1. 查找是否已存在记录
        record_id = self.get_holding_record_id(symbol)

        if record_id:
            # 存在则更新
            return self._update_record(self.holdings_table_id, record_id, data)
        else:
            # 不存在则创建 (需要包含symbol)
            data['资产代码'] = symbol
            return self._create_record(self.holdings_table_id, data)

    def batch_update_holdings(self, holdings: List[Dict]) -> Dict:
        """
        批量更新持仓 (暂不实现,因为需要先查询record_id)
        如果需要批量操作,建议在调用层循环调用update_holding

        :param holdings: 持仓数据列表
        :return: 结果统计
        """
        success = 0
        failed = 0
        errors = []

        for holding in holdings:
            symbol = holding.get('symbol')
            if not symbol:
                failed += 1
                errors.append("缺少symbol字段")
                continue

            data = {k: v for k, v in holding.items() if k != 'symbol'}

            try:
                if self.update_holding(symbol, data):
                    success += 1
                else:
                    failed += 1
                    errors.append(f"{symbol}: 更新失败")
            except Exception as e:
                failed += 1
                errors.append(f"{symbol}: {str(e)}")

        return {"success": success, "failed": failed, "errors": errors}

    def get_all_holdings(self) -> List[Dict]:
        """
        获取所有持仓记录

        :return: 持仓列表
        """
        try:
            records = self.list_records(self.app_token, self.holdings_table_id)
            # 提取 fields
            return [record.get('fields', {}) for record in records.get('items', [])]
        except Exception as e:
            logger.error(f"获取持仓列表失败: {e}")
            return []

    # ===== 历史表操作 =====

    def create_snapshot(self, snapshot_data: Dict) -> bool:
        """
        创建每日快照记录

        :param snapshot_data: 快照数据,例如:
            {
                '快照日期': 1700000000000,
                '资产代码': 'DOGE',
                '收盘价': 0.38,
                '持仓数量': 1000,
                '持仓市值': 380,
                '日收益率': 2.5,
                '备注': ''
            }
        :return: 是否成功
        """
        return self._create_record(self.history_table_id, snapshot_data)

    def batch_create_snapshots(self, snapshots: List[Dict]) -> Dict:
        """
        批量创建快照

        :param snapshots: 快照数据列表
        :return: 结果统计
        """
        records = [{"fields": snapshot} for snapshot in snapshots]
        return self.batch_create_records(self.app_token, self.history_table_id, records)

    # ===== 日志表操作 =====

    def log_sync_status(self, source: str, task_type: str, status: str,
                        record_count: int = 0, error_msg: Optional[str] = None,
                        duration: float = 0) -> bool:
        """
        记录同步状态到日志表

        :param source: 数据源 (binance/xueqiu)
        :param task_type: 任务类型 (价格同步/持仓同步/快照)
        :param status: 状态 (成功/失败)
        :param record_count: 同步记录数
        :param error_msg: 错误信息
        :param duration: 耗时(秒)
        :return: 是否成功
        """
        log_data = {
            '同步时间': int(time.time() * 1000),  # 毫秒时间戳
            '数据源': source,
            '任务类型': task_type,
            '状态': status,
            '同步记录数': record_count,
            '错误信息': error_msg or '',
            '耗时(秒)': round(duration, 2)
        }

        return self._create_record(self.logs_table_id, log_data)

    # ===== 通用方法 =====

    def _create_record(self, table_id: str, fields: Dict) -> bool:
        """
        创建单条记录

        :param table_id: 表ID
        :param fields: 字段数据
        :return: 是否成功
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"

        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token()}",
            "Content-Type": "application/json"
        }

        data = {"fields": fields}

        try:
            result = self._api_call_with_retry(url, headers, data, max_retries=2)
            return result.get("code") == 0
        except Exception:
            return False

    def _update_record(self, table_id: str, record_id: str, fields: Dict) -> bool:
        """
        更新记录

        :param table_id: 表ID
        :param record_id: 记录ID
        :param fields: 字段数据
        :return: 是否成功
        """
        import requests

        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/{record_id}"

        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token()}",
            "Content-Type": "application/json"
        }

        data = {"fields": fields}

        try:
            # 使用PUT方法更新记录
            response = requests.put(url, headers=headers, json=data, timeout=10)
            result = response.json()

            if result.get("code") != 0:
                print(f"更新记录失败: {result}")

            return result.get("code") == 0
        except Exception as e:
            print(f"更新记录异常: {e}")
            return False

    def get_all_holdings(self) -> List[Dict]:
        """
        获取所有持仓记录

        :return: 持仓记录列表
        """
        import requests

        # 使用查询接口而不是列表接口
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.holdings_table_id}/records/search"

        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token()}",
            "Content-Type": "application/json"
        }

        try:
            # 使用POST方法查询
            response = requests.post(url, headers=headers, json={"page_size": 100}, timeout=10)
            result = response.json()

            if result.get("code") == 0:
                items = result.get("data", {}).get("items", [])
                return items

            return []

        except Exception:
            return []
