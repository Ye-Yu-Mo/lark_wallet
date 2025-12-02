"""
基金同步任务
定时从雪球获取基金净值,同步到 SQLite 和飞书
"""
import time
from typing import Dict, List, Optional
from datetime import datetime, date, timedelta
from loguru import logger

from core.config import Config
from core.database import AssetDB
from core.feishu_client import AssetFeishuClient
from datasources.xueqiu_client import XueqiuClient
from utils.asset_discovery import get_fund_assets
from utils.feishu_guard import prepare_holdings_payload


class FundSyncTask:
    """
    基金同步任务

    负责从雪球获取基金净值数据,
    保存到 SQLite 并更新飞书表格
    """

    def __init__(self, config: Config):
        """
        初始化同步任务

        :param config: 配置对象
        """
        self.config = config

        # 获取配置
        asset_sync = config.get_asset_sync_config()
        xueqiu_config = asset_sync.get('xueqiu', {})
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

        # 初始化雪球客户端
        self.xueqiu = XueqiuClient(
            cookies=xueqiu_config['cookies']
        )

        # 获取基金配置 (支持新旧格式)
        self.fund_config = asset_sync.get('assets', {}).get('funds', [])

        logger.info(f"FundSyncTask 初始化完成")

    def sync(self) -> Dict:
        """
        执行同步任务

        :return: 同步结果统计
        """
        start_time = time.time()

        result = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }

        # 检查昨日是否是交易日 (基金净值通常在次日更新)
        previous_day = date.today() - timedelta(days=1)
        if not self._is_trading_day(previous_day):
            logger.info("昨天不是交易日,跳过基金同步")
            return result

        # 从飞书读取持仓数据 (用于资产发现和数据补充)
        holdings_data = []
        symbol_map = {}  # 优化: 建立 symbol -> record_id 映射
        try:
            holdings_data = self.feishu.get_all_holdings()
            logger.info(f"从飞书读取到 {len(holdings_data)} 个持仓记录")
            
            # 构建映射
            for item in holdings_data:
                record_id = item.get('record_id')
                fields = item.get('fields', {})
                
                code_field = fields.get('资产代码')
                if isinstance(code_field, list):
                    code = code_field[0].get('text', '') if code_field else ''
                else:
                    code = str(code_field or '')
                
                if code and record_id:
                    symbol_map[code] = record_id
                    
        except Exception as e:
            logger.warning(f"从飞书读取持仓失败: {e}")

        # 自动发现或获取配置的基金列表 (传入飞书持仓数据)
        fund_list = get_fund_assets(self.xueqiu, self.fund_config, holdings_data)

        if not fund_list:
            logger.warning("基金列表为空,跳过同步")
            return result

        # 飞书持仓数据已经包含在fund_list中，无需再次合并
        result['total'] = len(fund_list)
        logger.info(f"开始同步基金, 共 {len(fund_list)} 只基金")

        for fund_config in fund_list:
            try:
                # 从映射中查找 record_id
                symbol = fund_config.get('symbol')
                record_id = symbol_map.get(symbol)
                
                success = self._sync_fund(fund_config, record_id)
                if success:
                    result['success'] += 1
                else:
                    result['failed'] += 1
                    result['errors'].append({
                        'symbol': fund_config.get('symbol', 'unknown'),
                        'error': '同步失败'
                    })
            except Exception as e:
                result['failed'] += 1
                error_msg = str(e)
                result['errors'].append({
                    'symbol': fund_config.get('symbol', 'unknown'),
                    'error': error_msg
                })
                logger.error(f"同步基金失败: {fund_config.get('symbol', 'unknown')} - {error_msg}")

        # 计算耗时
        duration = time.time() - start_time

        # 记录同步日志到飞书
        status = 'success' if result['failed'] == 0 else 'partial'
        error_summary = '; '.join([f"{e['symbol']}: {e['error']}" for e in result['errors'][:3]])

        try:
            self.feishu.log_sync_status(
                source='xueqiu',
                task_type='fund_sync',
                status=status,
                record_count=result['success'],
                error_msg=error_summary if error_summary else None,
                duration=duration
            )
        except Exception as e:
            logger.error(f"记录同步日志失败: {e}")

        logger.info(f"基金同步完成: 成功 {result['success']}/{result['total']}, 耗时 {duration:.2f}s")

        return result

    def _sync_fund(self, fund_config: Dict, record_id: Optional[str] = None) -> bool:
        """
        同步单只基金

        :param fund_config: 基金配置
        :param record_id: 飞书记录ID (可选)
        :return: 是否成功
        """
        symbol = fund_config.get('symbol')  # 如 'SH510300'
        shares = fund_config.get('shares', 0)  # 持有份额
        avg_cost = fund_config.get('avg_cost', 0)  # 平均成本

        if not symbol:
            logger.warning("基金配置缺少 symbol 字段")
            return False

        logger.debug(f"同步基金: {symbol}")

        # 1. 获取当前净值
        nav = self.xueqiu.get_price(symbol)
        if nav is None:
            logger.error(f"获取净值失败: {symbol}")
            return False

        # 2. 获取基金详细信息 (自动获取资产名称)
        fund_info = self.xueqiu.get_fund_info(symbol)
        fund_name = fund_config.get('name', symbol)
        volume = None
        change_percent = 0

        if fund_info:
            # 从API自动获取资产名称
            fund_name = fund_info.get('name', fund_name)
            volume = fund_info.get('volume')
            change_percent = fund_info.get('percent', 0)

        # 3. 保存价格到 SQLite
        try:
            self.db.save_price(
                symbol=symbol,
                price=nav,
                volume=volume,
                source='xueqiu'
            )
        except Exception as e:
            logger.error(f"保存净值到数据库失败: {symbol} - {e}")

        # 4. 更新持仓到 SQLite
        if shares > 0:
            try:
                self.db.update_holding(
                    symbol=symbol,
                    asset_type='fund',
                    quantity=shares,
                    avg_cost=avg_cost,
                    platform='xueqiu'
                )
            except Exception as e:
                logger.error(f"更新持仓到数据库失败: {symbol} - {e}")

        # 5. 计算收益
        current_value = shares * nav
        cost_value = shares * avg_cost if avg_cost > 0 else 0
        profit = current_value - cost_value
        profit_percent = (profit / cost_value * 100) if cost_value > 0 else 0

        # 6. 更新飞书持仓表
        try:
            # 确保基金名称是纯字符串
            if isinstance(fund_name, list):
                fund_name = fund_name[0].get('text') if len(fund_name) > 0 else symbol

            feishu_data = {
                '资产名称': str(fund_name),  # 确保是字符串
                '资产类型': '基金',
                '持仓数量': shares,
                '当前价格': nav,
                '数据源': 'xueqiu',
                '更新状态': '成功',
                '最后更新时间': int(datetime.now().timestamp() * 1000)
            }

            # 只在成本价存在时才更新 (保留用户首次填写的成本价)
            if avg_cost > 0:
                feishu_data['单位成本'] = avg_cost

            record_id, sanitized_payload, blocked_fields = prepare_holdings_payload(
                self.feishu,
                self.db,
                symbol,
                feishu_data,
                record_id  # 传入 record_id
            )

            if blocked_fields:
                logger.warning(
                    f"{symbol} 在飞书中存在手动变更字段 {blocked_fields}, 自动同步不会覆盖, 如需恢复自动更新请确认并清理记录"
                )

            if not sanitized_payload:
                logger.warning(f"{symbol} 的飞书字段全部由用户维护, 跳过自动更新")
                return True

            self.feishu.update_holding(symbol, sanitized_payload, record_id=record_id)
            logger.debug(f"更新飞书成功: {symbol}, 净值: {nav}, 市值: {current_value}")

        except Exception as e:
            logger.error(f"更新飞书失败: {symbol} - {e}")
            return False

        return True

    def _is_trading_day(self, target_date: Optional[date] = None) -> bool:
        """
        检查指定日期是否是交易日

        默认判断昨日是否为交易日 (基金净值在次日更新)
        简单判断: 周一到周五为交易日
        注意: 这里没有处理节假日,实际使用时需要完善

        :return: 是否是交易日
        """
        target_date = target_date or date.today()
        # 周一=0, 周日=6
        return target_date.weekday() < 5


def sync_fund(config_path: str = 'config.json') -> Dict:
    """
    执行基金同步 (便捷函数)

    :param config_path: 配置文件路径
    :return: 同步结果
    """
    config = Config(config_path)
    task = FundSyncTask(config)
    return task.sync()


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

    # 执行同步
    result = sync_fund()
    print(f"\n同步结果: {result}")
