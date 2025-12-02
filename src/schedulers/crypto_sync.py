"""
加密货币同步任务
定时从 Binance 获取价格和持仓,同步到 SQLite 和飞书
"""
import time
from typing import Dict, List, Optional
from datetime import datetime
from loguru import logger

from core.config import Config
from core.database import AssetDB
from core.feishu_client import AssetFeishuClient
from datasources.binance_client import BinanceClient
from utils.asset_discovery import get_crypto_assets
from utils.alert import AlertManager
from utils.feishu_guard import prepare_holdings_payload


class CryptoSyncTask:
    """
    加密货币同步任务

    负责从 Binance 获取价格和持仓数据,
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
        binance_config = asset_sync.get('binance', {})
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

        # 初始化 Binance 客户端
        self.binance = BinanceClient(
            api_key=binance_config['api_key'],
            api_secret=binance_config['api_secret'],
            testnet=binance_config.get('testnet', False)
        )

        # 获取加密货币配置 (支持新旧格式)
        self.crypto_config = asset_sync.get('assets', {}).get('crypto', [])

        # 初始化告警管理器
        alert_config = asset_sync.get('alerts', {})
        self.alert_manager = AlertManager(
            webhook_url=alert_config.get('feishu_webhook', ''),
            email_config=alert_config.get('email'),
            enabled=alert_config.get('enabled', False)
        )
        self.alert_on_failure = alert_config.get('alert_on_failure', True)
        self.alert_on_partial = alert_config.get('alert_on_partial_success', False)
        self.min_success_rate = alert_config.get('min_success_rate', 0.8)

        logger.info(f"CryptoSyncTask 初始化完成")

    def sync(self) -> Dict:
        """
        执行同步任务

        :return: 同步结果统计
        """
        # 检查是否启用了 Binance 数据源
        binance_config = self.config.get_asset_sync_config().get('binance', {})
        if not binance_config.get('enabled', False):
            logger.info("Binance 数据源未启用, 跳过同步")
            return {
                'total': 0,
                'success': 0,
                'failed': 0,
                'errors': [],
                'message': 'Binance disabled'
            }

        start_time = time.time()

        result = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'errors': []
        }

        # 从飞书读取持仓数据 (用于资产发现和数据补充)
        holdings_data = []
        symbol_map = {}  # 优化: 建立 symbol -> record_id 映射, 避免重复搜索
        try:
            holdings_data = self.feishu.get_all_holdings()
            logger.info(f"从飞书读取到 {len(holdings_data)} 个持仓记录")
            
            # 构建映射
            for item in holdings_data:
                record_id = item.get('record_id')
                fields = item.get('fields', {})
                
                # 健壮的提取逻辑 (参考 price_alert.py)
                code_field = fields.get('资产代码')
                if isinstance(code_field, list):
                    code = code_field[0].get('text', '') if code_field else ''
                else:
                    code = str(code_field or '')
                
                if code and record_id:
                    symbol_map[code] = record_id
                    
        except Exception as e:
            logger.warning(f"从飞书读取持仓失败: {e}")

        # 自动发现或获取配置的资产列表 (传入飞书持仓数据)
        crypto_list = get_crypto_assets(self.binance, self.crypto_config, holdings_data)

        if not crypto_list:
            logger.warning("加密货币列表为空,跳过同步")
            return result

        result['total'] = len(crypto_list)
        logger.info(f"开始同步加密货币, 共 {len(crypto_list)} 个资产")

        # 优化: 批量获取一次余额, 避免在循环中重复调用高开销的 account 接口
        all_balances = {}
        try:
            all_balances = self.binance.get_all_balances()
            logger.info(f"已缓存账户余额信息, 包含 {len(all_balances)} 个资产")
        except Exception as e:
            logger.warning(f"批量获取余额失败, 将尝试单个获取: {e}")
            all_balances = None

        for asset_config in crypto_list:
            try:
                # 从映射中查找 record_id
                symbol = asset_config.get('symbol')
                record_id = symbol_map.get(symbol)
                
                success = self._sync_asset(asset_config, all_balances, record_id)
                if success:
                    result['success'] += 1
                else:
                    result['failed'] += 1
                    result['errors'].append({
                        'symbol': asset_config.get('symbol', 'unknown'),
                        'error': '同步失败'
                    })
            except Exception as e:
                result['failed'] += 1
                error_msg = str(e)
                result['errors'].append({
                    'symbol': asset_config.get('symbol', 'unknown'),
                    'error': error_msg
                })
                logger.error(f"同步资产失败: {asset_config.get('symbol', 'unknown')} - {error_msg}")

        # 计算耗时
        duration = time.time() - start_time

        # 计算成功率
        success_rate = (result['success'] / result['total']) if result['total'] > 0 else 0

        # 记录同步日志到飞书
        status = 'success' if result['failed'] == 0 else 'partial'
        error_summary = '; '.join([f"{e['symbol']}: {e['error']}" for e in result['errors'][:3]])

        try:
            self.feishu.log_sync_status(
                source='binance',
                task_type='crypto_sync',
                status=status,
                record_count=result['success'],
                error_msg=error_summary if error_summary else None,
                duration=duration
            )
        except Exception as e:
            logger.error(f"记录同步日志失败: {e}")

        # 发送告警
        self._send_alert(result, error_summary)

        logger.info(f"加密货币同步完成: 成功 {result['success']}/{result['total']}, 耗时 {duration:.2f}s")

        return result

    def _send_alert(self, result: Dict, error_summary: str):
        """发送告警通知"""
        total = result['total']
        success = result['success']
        failed = result['failed']

        if total == 0:
            return

        success_rate = success / total

        # 完全失败
        if failed == total and self.alert_on_failure:
            self.alert_manager.send_sync_failure(
                task_name='加密货币同步',
                source='Binance',
                error_summary=error_summary,
                total=total,
                failed=failed
            )

        # 部分成功
        elif failed > 0 and success_rate < self.min_success_rate and self.alert_on_partial:
            self.alert_manager.send_sync_partial_success(
                task_name='加密货币同步',
                source='Binance',
                total=total,
                success=success,
                failed=failed,
                error_summary=error_summary
            )

    def _sync_asset(self, asset_config: Dict, all_balances: Optional[Dict] = None, record_id: Optional[str] = None) -> bool:
        """
        同步单个资产

        :param asset_config: 资产配置
        :param all_balances: 余额缓存字典 (可选)
        :param record_id: 飞书记录ID (可选, 避免重复搜索)
        :return: 是否成功
        """
        symbol = asset_config.get('symbol')  # 如 'DOGE'
        trading_pair = asset_config.get('trading_pair', f"{symbol}/USDT")  # 如 'DOGE/USDT'
        quantity = asset_config.get('quantity', 0)  # 配置的持仓数量 (可选)
        avg_cost = asset_config.get('avg_cost', 0)  # 平均成本 (可选)

        if not symbol:
            logger.warning("资产配置缺少 symbol 字段")
            return False

        logger.debug(f"同步资产: {symbol}")

        # 1. 获取当前价格
        # 特殊处理稳定币 (USDT/USDC/BUSD 等)
        base_symbol = symbol[2:] if symbol.startswith('LD') else symbol
        if base_symbol in ['USDT', 'USDC', 'BUSD', 'TUSD', 'USDP']:
            price = 1.0  # 稳定币固定为 1.0
            logger.debug(f"稳定币 {symbol} 使用固定价格 1.0")
        else:
            price = self.binance.get_price(trading_pair)
            if price is None:
                logger.error(f"获取价格失败: {trading_pair}")
                return False

        # 2. 获取交易量 (可选)
        # 优化: SimpleBinanceClient 的 get_ticker_info 实现是虚假的(不返回volume), 且会重复请求价格
        # 因此这里直接跳过, 除非需要真正的 volume 数据 (需要升级 Client)
        volume = None

        # 3. 获取实际持仓余额 (如果启用了 API 权限)
        actual_balance = None
        try:
            if all_balances is not None:
                # 使用缓存 (注意: Binance返回的key通常是大写)
                actual_balance = all_balances.get(symbol.upper(), 0.0)
            else:
                # 降级为单独获取
                actual_balance = self.binance.get_balance(symbol)
        except Exception as e:
            logger.warning(f"获取持仓余额失败: {symbol} - {e}")

        # 使用实际余额或配置的数量
        final_quantity = actual_balance if actual_balance is not None else quantity

        # 4. 保存价格到 SQLite
        try:
            self.db.save_price(
                symbol=trading_pair,
                price=price,
                volume=volume,
                source='binance'
            )
        except Exception as e:
            logger.error(f"保存价格到数据库失败: {trading_pair} - {e}")

        # 5. 更新持仓到 SQLite
        if final_quantity > 0:
            try:
                self.db.update_holding(
                    symbol=symbol,
                    asset_type='crypto',
                    quantity=final_quantity,
                    avg_cost=avg_cost,
                    platform='binance'
                )
            except Exception as e:
                logger.error(f"更新持仓到数据库失败: {symbol} - {e}")

        # 6. 计算收益
        current_value = final_quantity * price
        cost_value = final_quantity * avg_cost if avg_cost > 0 else 0
        profit = current_value - cost_value
        profit_percent = (profit / cost_value * 100) if cost_value > 0 else 0

        # 7. 更新飞书持仓表
        try:
            # 确保资产名称是纯字符串
            asset_name = asset_config.get('name', symbol)
            if isinstance(asset_name, list):
                asset_name = asset_name[0].get('text') if len(asset_name) > 0 else symbol

            feishu_data = {
                '资产名称': str(asset_name),  # 确保是字符串
                '资产类型': '加密货币',
                '持仓数量': final_quantity,
                '当前价格': price,
                '数据源': 'binance',
                '更新状态': '成功',
                '最后更新时间': int(datetime.now().timestamp() * 1000)
            }

            # 只在成本价存在时才更新
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
                    f"{symbol} 在飞书中的字段 {blocked_fields} 已被手动修改, 自动同步不会覆盖"
                )

            if not sanitized_payload:
                logger.warning(f"{symbol} 的飞书记录全部字段被保护, 跳过自动更新")
                return True

            self.feishu.update_holding(symbol, sanitized_payload, record_id=record_id)
            logger.debug(f"更新飞书成功: {symbol}, 价格: {price}, 市值: {current_value}")

        except Exception as e:
            logger.error(f"更新飞书失败: {symbol} - {e}")
            return False

        return True


def sync_crypto(config_path: str = 'config.json') -> Dict:
    """
    执行加密货币同步 (便捷函数)

    :param config_path: 配置文件路径
    :return: 同步结果
    """
    config = Config(config_path)
    task = CryptoSyncTask(config)
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
    result = sync_crypto()
    print(f"\n同步结果: {result}")
