"""
每日快照任务
创建每日资产快照,记录历史数据
"""
import time
from typing import Dict, List, Optional
from datetime import datetime, date
from loguru import logger

from core.config import Config
from core.database import AssetDB
from core.feishu_client import AssetFeishuClient


class SnapshotTask:
    """
    每日快照任务

    负责创建每日资产快照,
    记录历史数据用于分析
    """

    def __init__(self, config: Config):
        """
        初始化快照任务

        :param config: 配置对象
        """
        self.config = config

        # 获取配置
        asset_sync = config.get_asset_sync_config()
        feishu_config = config.get_feishu_config()
        db_config = config.get_database_config()

        # 初始化数据库
        self.db = AssetDB(db_config['path'])

        # 初始化飞书客户端
        self.feishu = AssetFeishuClient(
            app_id=feishu_config['app_id'],
            app_secret=feishu_config['app_secret'],
            app_token=feishu_config['app_token'],
            table_ids=feishu_config['tables']
        )

        logger.info("SnapshotTask 初始化完成")

    def create_snapshot(self) -> Dict:
        """
        创建每日快照

        :return: 快照结果
        """
        start_time = time.time()

        result = {
            'date': date.today().isoformat(),
            'total_holdings': 0,
            'total_value': 0,
            'total_cost': 0,
            'total_profit': 0,
            'success': True,
            'errors': []
        }

        try:
            # 1. 从数据库获取所有持仓
            holdings = self.db.get_all_holdings()

            if not holdings:
                logger.warning("没有持仓数据,跳过快照")
                return result

            result['total_holdings'] = len(holdings)

            # 2. 计算总市值和收益
            snapshot_records = []

            for holding in holdings:
                symbol = holding['symbol']
                quantity = holding['quantity']
                avg_cost = holding['avg_cost']
                asset_type = holding['asset_type']
                platform = holding['platform']

                # 获取最新价格
                latest_price = self.db.get_latest_price(symbol)
                if not latest_price:
                    # 尝试获取交易对价格
                    trading_pair = f"{symbol}/USDT"
                    latest_price = self.db.get_latest_price(trading_pair)

                if latest_price:
                    price = latest_price['price']
                    current_value = quantity * price
                    cost_value = quantity * avg_cost
                    profit = current_value - cost_value
                    profit_percent = (profit / cost_value * 100) if cost_value > 0 else 0

                    result['total_value'] += current_value
                    result['total_cost'] += cost_value
                    result['total_profit'] += profit

                    # 记录快照
                    snapshot_records.append({
                        'symbol': symbol,
                        'asset_type': asset_type,
                        'quantity': quantity,
                        'price': price,
                        'value': current_value,
                        'cost': cost_value,
                        'profit': profit,
                        'profit_percent': profit_percent,
                        'platform': platform
                    })
                else:
                    logger.warning(f"无法获取价格: {symbol}")
                    result['errors'].append(f"无价格: {symbol}")

            # 3. 计算总收益率
            total_profit_percent = 0
            if result['total_cost'] > 0:
                total_profit_percent = (result['total_profit'] / result['total_cost']) * 100

            # 4. 保存到飞书历史表
            try:
                snapshot_data = {
                    '快照日期': int(datetime.now().timestamp() * 1000),
                    '总市值': result['total_value'],
                    '总成本': result['total_cost'],
                    '总收益': result['total_profit'],
                    '收益率': total_profit_percent,
                    '持仓数量': result['total_holdings'],
                    '备注': f"自动快照 - {len(snapshot_records)}个资产"
                }

                self.feishu.create_snapshot(snapshot_data)
                logger.info(f"快照已保存到飞书历史表")

            except Exception as e:
                logger.error(f"保存快照到飞书失败: {e}")
                result['errors'].append(f"飞书保存失败: {e}")
                result['success'] = False

            # 5. 记录日志
            duration = time.time() - start_time

            try:
                self.feishu.log_sync_status(
                    source='system',
                    task_type='daily_snapshot',
                    status='success' if result['success'] else 'failed',
                    record_count=len(snapshot_records),
                    error_msg='; '.join(result['errors'][:3]) if result['errors'] else None,
                    duration=duration
                )
            except Exception as e:
                logger.error(f"记录快照日志失败: {e}")

            logger.info(f"每日快照完成: 总市值 {result['total_value']:.2f}, "
                       f"总收益 {result['total_profit']:.2f} ({total_profit_percent:.2f}%), "
                       f"耗时 {duration:.2f}s")

        except Exception as e:
            logger.error(f"创建快照失败: {e}")
            result['success'] = False
            result['errors'].append(str(e))

        return result


def create_daily_snapshot(config_path: str = 'config.json') -> Dict:
    """
    创建每日快照 (便捷函数)

    :param config_path: 配置文件路径
    :return: 快照结果
    """
    config = Config(config_path)
    task = SnapshotTask(config)
    return task.create_snapshot()


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

    # 创建快照
    result = create_daily_snapshot()
    print(f"\n快照结果: {result}")
