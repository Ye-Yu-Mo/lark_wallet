"""
飞书API直接调用客户端
用于批量操作
"""
import requests
import json
import time


class FeishuClient:
    """飞书API客户端"""

    def __init__(self, app_id, app_secret):
        """
        初始化客户端
        :param app_id: 应用ID
        :param app_secret: 应用密钥
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = None
        self.token_expire_time = 0  # Token过期时间戳

    def get_tenant_access_token(self):
        """获取tenant_access_token,带过期检查和自动刷新"""
        # 检查token是否存在且未过期 (提前5分钟刷新)
        current_time = time.time()
        if self.access_token and current_time < (self.token_expire_time - 300):
            return self.access_token

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json"}
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }

        response = requests.post(url, headers=headers, json=data)
        result = response.json()

        if result.get("code") != 0:
            raise Exception(f"获取access_token失败: {result}")

        self.access_token = result["tenant_access_token"]
        # 飞书token有效期默认2小时(7200秒)
        expire_seconds = result.get("expire", 7200)
        self.token_expire_time = current_time + expire_seconds

        return self.access_token

    def _api_call_with_retry(self, url, headers, data, method='POST', max_retries=3, timeout=30):
        """
        带重试的API调用
        :param url: API URL
        :param headers: 请求头
        :param data: 请求体
        :param method: HTTP方法 (POST, PUT, DELETE等)
        :param max_retries: 最大重试次数
        :param timeout: 超时时间
        :return: API响应结果
        """
        for attempt in range(max_retries):
            try:
                if method.upper() == 'GET':
                    response = requests.get(url, headers=headers, params=data, timeout=timeout)
                elif method.upper() == 'PUT':
                    response = requests.put(url, headers=headers, json=data, timeout=timeout)
                elif method.upper() == 'DELETE':
                    response = requests.delete(url, headers=headers, json=data, timeout=timeout)
                else:
                    response = requests.post(url, headers=headers, json=data, timeout=timeout)

                result = response.json()

                # 检查是否需要重试
                code = result.get("code", 0)

                # 成功
                if code == 0:
                    return result

                # 限流错误,需要重试
                if code in [99991400, 99991664]:  # 飞书限流错误码
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt)  # 指数退避: 1s, 2s, 4s
                        time.sleep(wait_time)
                        continue

                # 其他错误,不重试
                return result

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt)
                    time.sleep(wait_time)
                    continue
                raise
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt)
                    time.sleep(wait_time)
                    continue
                raise

        # 所有重试都失败
        raise Exception(f"API调用失败,已重试{max_retries}次")

    def batch_create_records(self, app_token, table_id, records):
        """
        批量创建记录
        :param app_token: 多维表app_token
        :param table_id: 表table_id
        :param records: 记录列表,每条记录格式: {"fields": {...}}
        :return: 创建结果 {"success": int, "failed": int, "errors": list}
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"

        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token()}",
            "Content-Type": "application/json"
        }

        data = {
            "records": records
        }

        try:
            # 使用带重试的API调用
            result = self._api_call_with_retry(url, headers, data)

            if result.get("code") != 0:
                # API调用失败
                error_msg = result.get("msg", "Unknown error")
                print(f"批量创建失败: {error_msg}")
                print(f"完整响应: {result}")
                # 如果批量创建失败,尝试逐条创建
                return self._fallback_single_create(app_token, table_id, records)

            # 检查返回的records数量
            created_records = result.get("data", {}).get("records", [])
            if not created_records:
                # 没有返回创建的记录，可能是静默失败
                print(f"⚠️  警告: API返回成功但没有创建记录")
                print(f"完整响应: {result}")
                return {"success": 0, "failed": len(records), "errors": ["API返回成功但没有创建记录"]}

            return {"success": len(created_records), "failed": 0, "errors": []}

        except Exception as e:
            # 网络错误等,尝试逐条创建
            print(f"批量创建异常: {e}")
            return self._fallback_single_create(app_token, table_id, records)

    def _fallback_single_create(self, app_token, table_id, records):
        """
        逐条创建记录(批量失败时的fallback)
        """
        success = 0
        failed = 0
        errors = []

        for i, record in enumerate(records):
            try:
                url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
                headers = {
                    "Authorization": f"Bearer {self.get_tenant_access_token()}",
                    "Content-Type": "application/json"
                }

                # 单条创建也使用重试机制
                result = self._api_call_with_retry(url, headers, {"fields": record["fields"]}, max_retries=2, timeout=10)

                if result.get("code") == 0:
                    success += 1
                else:
                    failed += 1
                    errors.append(f"记录{i}: {result.get('msg', 'unknown error')}")
            except Exception as e:
                failed += 1
                errors.append(f"记录{i}: {str(e)}")

        return {"success": success, "failed": failed, "errors": errors}

    def batch_update_records(self, app_token, table_id, records):
        """
        批量更新记录
        :param app_token: 多维表app_token
        :param table_id: 表table_id
        :param records: 记录列表,每条记录格式: {"record_id": "xxx", "fields": {...}}
        :return: 更新结果 {"success": int, "failed": int, "errors": list}
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"

        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token()}",
            "Content-Type": "application/json"
        }

        data = {
            "records": records
        }

        try:
            # 使用带重试的API调用
            result = self._api_call_with_retry(url, headers, data)

            if result.get("code") != 0:
                # 如果批量更新失败,尝试逐条更新
                return self._fallback_single_update(app_token, table_id, records)

            return {"success": len(records), "failed": 0, "errors": []}

        except Exception as e:
            # 网络错误等,尝试逐条更新
            return self._fallback_single_update(app_token, table_id, records)

    def _fallback_single_update(self, app_token, table_id, records):
        """
        逐条更新记录(批量失败时的fallback)
        """
        success = 0
        failed = 0
        errors = []

        for i, record in enumerate(records):
            try:
                record_id = record.get("record_id")
                if not record_id:
                    failed += 1
                    errors.append(f"记录{i}: 缺少record_id")
                    continue

                url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
                headers = {
                    "Authorization": f"Bearer {self.get_tenant_access_token()}",
                    "Content-Type": "application/json"
                }

                # 单条更新使用PUT方法
                result = self._api_call_with_retry(url, headers, {"fields": record["fields"]}, method='PUT', max_retries=2, timeout=10)

                if result.get("code") == 0:
                    success += 1
                else:
                    failed += 1
                    errors.append(f"记录{i} (id={record_id}): {result.get('msg', 'unknown error')}")
            except Exception as e:
                failed += 1
                errors.append(f"记录{i} (id={record.get('record_id')}): {str(e)}")

        return {"success": success, "failed": failed, "errors": errors}

    def list_fields(self, app_token, table_id):
        """
        列出表字段
        :param app_token: 多维表app_token
        :param table_id: 表table_id
        :return: 字段列表
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token()}"
        }

        # 使用分页遍历所有字段
        all_fields = []
        page_token = None
        has_more = True

        while has_more:
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token

            # GET 请求不带 body, 这里的 _api_call_with_retry 需要调整一下或者直接用 requests
            # 为简单起见，且 list_fields 通常不需要重试太多次，直接用 requests
            response = requests.get(url, headers=headers, params=params, timeout=30)
            result = response.json()

            if result.get("code") != 0:
                raise Exception(f"获取字段失败: {result}")

            items = result.get("data", {}).get("items", [])
            all_fields.extend(items)

            has_more = result.get("data", {}).get("has_more", False)
            page_token = result.get("data", {}).get("page_token")

        return all_fields

    def list_records(self, app_token, table_id, page_token=None, page_size=500):
        """
        列出记录
        :param app_token: 多维表app_token
        :param table_id: 表table_id
        :param page_token: 分页token
        :param page_size: 每页大小
        :return: (items, page_token, has_more)
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token()}"
        }
        params = {
            "page_size": page_size
        }
        if page_token:
            params["page_token"] = page_token

        # GET 请求
        response = requests.get(url, headers=headers, params=params, timeout=30)
        result = response.json()

        if result.get("code") != 0:
            raise Exception(f"获取记录失败: {result}")

        data = result.get("data", {})
        return (
            data.get("items", []),
            data.get("page_token"),
            data.get("has_more", False)
        )

    def search_records(self, app_token, table_id, filter_conditions=None, sort=None, page_size=500, page_token=None):
        """
        搜索记录 (带筛选条件)
        :param app_token: 多维表app_token
        :param table_id: 表table_id
        :param filter_conditions: 筛选条件字典
        :param sort: 排序规则列表
        :param page_size: 每页大小
        :param page_token: 分页token
        :return: 记录列表
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token()}",
            "Content-Type": "application/json"
        }

        body = {
            "page_size": page_size
        }

        if filter_conditions:
            body["filter"] = filter_conditions

        if sort:
            body["sort"] = sort

        if page_token:
            body["page_token"] = page_token

        response = requests.post(url, headers=headers, json=body, timeout=30)
        result = response.json()

        if result.get("code") != 0:
            raise Exception(f"搜索记录失败: {result}")

        data = result.get("data", {})
        items = data.get("items", [])

        # 如果有更多页,递归获取
        if data.get("has_more", False) and data.get("page_token"):
            next_items = self.search_records(
                app_token,
                table_id,
                filter_conditions,
                sort,
                page_size,
                data.get("page_token")
            )
            items.extend(next_items)

        return items

    def create_table(self, app_token, table_name, default_view_name=None, fields=None):
        """
        创建数据表
        :param app_token: 多维表app_token
        :param table_name: 表名称
        :param default_view_name: 默认视图名称
        :param fields: 字段列表
        :return: 创建结果
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token()}",
            "Content-Type": "application/json"
        }

        body = {
            "table": {
                "name": table_name
            }
        }

        if default_view_name:
            body["table"]["default_view_name"] = default_view_name

        if fields:
            body["table"]["fields"] = fields

        result = self._api_call_with_retry(url, headers, body)

        if result.get("code") != 0:
            raise Exception(f"创建表失败: {result}")

        return result.get("data", {})


