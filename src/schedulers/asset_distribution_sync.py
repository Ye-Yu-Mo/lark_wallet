"""
资产分布表同步任务
将资产持仓表的数据同步到资产分布表,用于生成资产分布快照
"""
import time
from typing import Dict, List
from datetime import datetime
from loguru import logger

from core.config import Config
from core.feishu_client import AssetFeishuClient


class AssetDistributionSync:
    """
    资产分布表同步任务

    将资产持仓表的当前市值同步到资产分布表,
    按资产类型和储蓄账户进行分类汇总
    """

    # 资产类型映射
    ASSET_TYPE_MAP = {
        '加密货币': '数字货币',
        '基金': '基金',
        '股票': '股票'
    }

    # 储蓄账户映射(根据资产代码或数据源)
    ACCOUNT_MAP = {
        # 加密货币
        'binance': 'DOGE',
        # 基金
        'xueqiu': '雪球基金',
        'alipay': '支付宝基金',
    }

    def __init__(self, config: Config):
        """初始化同步任务"""
        self.config = config

        # 获取配置
        feishu_config = config.get_feishu_config()

        # 初始化飞书客户端
        self.feishu = AssetFeishuClient(
            app_id=feishu_config['app_id'],
            app_secret=feishu_config['app_secret'],
            app_token=feishu_config['app_token'],
            table_ids=feishu_config['tables']
        )

        self.app_token = feishu_config['app_token']
        self.distribution_table_id = 'tblBsvn4HCHnFuLE'  # 资产分布表

        logger.info("AssetDistributionSync 初始化完成")

    def sync_to_distribution(self) -> Dict:
        """
        同步资产持仓到资产分布表

        :return: 同步结果
        """
        start_time = time.time()

        result = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'total_assets': 0,
            'synced_records': 0,
            'errors': []
        }

        try:
            # 1. 获取所有持仓
            logger.info("开始获取资产持仓数据...")
            holdings = self.feishu.get_all_holdings()

            if not holdings:
                logger.warning("没有持仓数据")
                return result

            result['total_assets'] = len(holdings)
            logger.info(f"获取到 {len(holdings)} 条持仓记录")

            # 2. 按资产类型和账户聚合
            aggregated = {}

            for item in holdings:
                fields = item.get('fields', {})

                # 提取字段
                asset_type = fields.get('资产类型')
                data_source = fields.get('数据源', 'manual')

                # 公式字段返回字典格式: {"type": 2, "value": [97.295373]}
                current_value_field = fields.get('当前市值')
                if isinstance(current_value_field, dict):
                    value_array = current_value_field.get('value', [0])
                    current_value = float(value_array[0]) if value_array else 0
                else:
                    current_value = float(current_value_field or 0)

                # 跳过无效数据(市值小于0.01视为无效)
                if not asset_type or current_value < 0.01:
                    continue

                # 映射资产类型
                mapped_type = self.ASSET_TYPE_MAP.get(asset_type, asset_type)

                # 映射储蓄账户
                account = self.ACCOUNT_MAP.get(data_source, '其他')

                # 聚合key
                key = (mapped_type, account)

                if key not in aggregated:
                    aggregated[key] = 0

                aggregated[key] += current_value

            logger.info(f"聚合后得到 {len(aggregated)} 条记录")

            # 3. 写入资产分布表
            current_timestamp = int(datetime.now().timestamp() * 1000)

            for (asset_type, account), amount in aggregated.items():
                try:
                    record_data = {
                        '记录时间': current_timestamp,
                        '资产类型': asset_type,
                        '储蓄账户': account,
                        '金额': round(amount, 2)
                    }

                    # 创建记录
                    success = self._create_distribution_record(record_data)

                    if success:
                        result['synced_records'] += 1
                        logger.debug(f"已同步: {asset_type} - {account} = {amount:.2f}")
                    else:
                        result['errors'].append(f"创建失败: {asset_type} - {account}")

                except Exception as e:
                    logger.error(f"创建记录失败 ({asset_type} - {account}): {e}")
                    result['errors'].append(f"{asset_type} - {account}: {str(e)}")

            # 4. 记录日志
            duration = time.time() - start_time

            if result['errors']:
                result['success'] = False

            logger.info(f"资产分布同步完成: 总资产 {result['total_assets']}, "
                       f"已同步 {result['synced_records']} 条记录, "
                       f"耗时 {duration:.2f}s")

            # 记录到同步日志表
            try:
                self.feishu.log_sync_status(
                    source='system',
                    task_type='asset_distribution_sync',
                    status='success' if result['success'] else 'partial',
                    record_count=result['synced_records'],
                    error_msg='; '.join(result['errors'][:3]) if result['errors'] else None,
                    duration=duration
                )
            except Exception as e:
                logger.error(f"记录同步日志失败: {e}")

        except Exception as e:
            logger.error(f"资产分布同步失败: {e}")
            result['success'] = False
            result['errors'].append(str(e))

        return result

    def _create_distribution_record(self, fields: Dict) -> bool:
        """
        创建资产分布记录

        :param fields: 字段数据
        :return: 是否成功
        """
        import requests

        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.distribution_table_id}/records"

        headers = {
            "Authorization": f"Bearer {self.feishu.get_tenant_access_token()}",
            "Content-Type": "application/json"
        }

        data = {"fields": fields}

        try:
            response = requests.post(url, headers=headers, json=data, timeout=10)
            result = response.json()
            return result.get("code") == 0
        except Exception as e:
            logger.error(f"创建资产分布记录失败: {e}")
            return False


def sync_asset_distribution(config_path: str = 'config.json') -> Dict:
    """
    同步资产分布 (便捷函数)

    :param config_path: 配置文件路径
    :return: 同步结果
    """
    config = Config(config_path)
    task = AssetDistributionSync(config)
    return task.sync_to_distribution()


if __name__ == '__main__':
    # 直接运行测试
    import sys
    from pathlib import Path

    # 添加项目根目录到路径
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

    # 设置日志
    from core.logger import setup_logger
    setup_logger(level='DEBUG')

    # 同步资产分布
    result = sync_asset_distribution()
    print(f"\n同步结果: {result}")
