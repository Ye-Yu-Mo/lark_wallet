"""
资产发现模块
自动从交易所/数据源获取所有资产,并根据 ignore 规则过滤
"""
import fnmatch
from typing import List, Dict, Optional
from loguru import logger

from datasources.binance_client import BinanceClient
from datasources.xueqiu_client import XueqiuClient


class AssetDiscovery:
    """
    资产自动发现

    从数据源自动获取所有资产,并根据 ignore 规则过滤
    """

    @staticmethod
    def discover_crypto_assets(
        binance: BinanceClient,
        ignore_patterns: List[str] = None,
        feishu_holdings: List[Dict] = None
    ) -> List[Dict]:
        """
        从飞书持仓表和Binance自动发现加密货币资产

        优先级：
        1. 从飞书持仓表读取已有加密货币（包含成本价）
        2. 从Binance余额补充新资产

        :param binance: Binance 客户端
        :param ignore_patterns: 忽略规则列表 (支持通配符)
        :param feishu_holdings: 飞书持仓数据
        :return: 资产列表
        """
        ignore_patterns = ignore_patterns or []

        logger.info("开始自动发现加密货币资产...")

        discovered = {}  # 用字典去重，key是symbol

        try:
            # 只从Binance获取加密货币资产 (不从飞书读取)
            all_balances = binance.get_all_balances()

            if not all_balances:
                logger.warning("Binance未发现任何余额")
            else:
                logger.info(f"从Binance发现 {len(all_balances)} 个资产余额")
                ignored_count = 0

                for symbol, balance in all_balances.items():
                    # 处理 LD* 前缀 (Liquid Swap 代币)
                    # LDUSDT -> USDT, LDETH -> ETH
                    base_symbol = symbol
                    trading_symbol = symbol
                    if symbol.startswith('LD'):
                        base_symbol = symbol[2:]  # 去掉LD前缀
                        trading_symbol = base_symbol
                        logger.debug(f"检测到Liquid Swap资产: {symbol} -> {base_symbol}")

                    # 检查是否匹配 ignore 规则
                    if AssetDiscovery._should_ignore(symbol, ignore_patterns):
                        logger.debug(f"忽略资产: {symbol} (匹配规则)")
                        ignored_count += 1
                        continue

                    # 新发现的资产
                    discovered[symbol] = {
                        'symbol': symbol,  # 保留原始代码 (LDUSDT)
                        'trading_pair': f"{trading_symbol}/USDT",  # 用基础币种获取价格 (USDT/USDT)
                        'name': symbol,
                        'quantity': balance,
                        'avg_cost': 0  # 需要在飞书手动填写成本价
                    }

                logger.info(f"Binance资产处理: 有效 {len(all_balances) - ignored_count} 个, 忽略 {ignored_count} 个")

            assets = list(discovered.values())
            logger.info(f"加密货币资产发现完成: 共 {len(assets)} 个")

            return assets

        except Exception as e:
            logger.error(f"加密货币资产发现失败: {e}")
            return []

    @staticmethod
    def discover_fund_assets(
        xueqiu: XueqiuClient,
        ignore_patterns: List[str] = None,
        feishu_holdings: List[Dict] = None
    ) -> List[Dict]:
        """
        从飞书持仓表和雪球自选股发现基金资产

        优先级：
        1. 从飞书持仓表读取已有基金（最可靠）
        2. 从雪球自选股补充新基金（可选）

        :param xueqiu: 雪球客户端
        :param ignore_patterns: 忽略规则列表
        :param feishu_holdings: 飞书持仓数据
        :return: 资产列表
        """
        ignore_patterns = ignore_patterns or []

        logger.info("开始自动发现基金资产...")

        discovered = {}  # 用字典去重，key是symbol

        try:
            # 1. 优先从飞书持仓表读取
            if feishu_holdings:
                fund_holdings = [
                    h for h in feishu_holdings
                    if h.get('fields', {}).get('资产类型') == '基金'
                ]

                if fund_holdings:
                    logger.info(f"从飞书持仓表发现 {len(fund_holdings)} 个基金")
                    for holding in fund_holdings:
                        fields = holding.get('fields', {})

                        # 解析资产代码 (可能是富文本格式)
                        symbol_field = fields.get('资产代码')
                        symbol = None
                        if isinstance(symbol_field, list) and len(symbol_field) > 0:
                            # 富文本格式: [{"text":"161119","type":"text"}]
                            symbol = symbol_field[0].get('text')
                        elif isinstance(symbol_field, str):
                            # 纯文本格式
                            symbol = symbol_field

                        if not symbol:
                            continue

                        # 检查是否匹配 ignore 规则
                        if AssetDiscovery._should_ignore(symbol, ignore_patterns):
                            logger.debug(f"忽略基金: {symbol} (匹配规则)")
                            continue

                        discovered[symbol] = {
                            'symbol': symbol,
                            'name': fields.get('资产名称', symbol),
                            'shares': fields.get('持仓数量', 0) or fields.get('数量', 0),
                            'avg_cost': fields.get('成本价', 0)
                        }

            # 2. 从雪球自选股补充
            watch_list = xueqiu.get_watch_list()
            if watch_list:
                logger.info(f"从雪球自选股发现 {len(watch_list)} 个资产")
                for item in watch_list:
                    symbol = item.get('symbol')
                    if not symbol or symbol in discovered:
                        continue

                    # 检查是否匹配 ignore 规则
                    if AssetDiscovery._should_ignore(symbol, ignore_patterns):
                        logger.debug(f"忽略资产: {symbol} (匹配规则)")
                        continue

                    # 补充新发现的资产
                    discovered[symbol] = {
                        'symbol': symbol,
                        'name': item.get('name', symbol),
                        'shares': 0,  # 需要手动在飞书填写
                        'avg_cost': 0,  # 需要手动在飞书填写
                        'asset_type': item.get('type', 'unknown')
                    }

            assets = list(discovered.values())
            logger.info(f"基金资产发现完成: 共 {len(assets)} 个")

            return assets

        except Exception as e:
            logger.error(f"基金资产发现失败: {e}")
            return []

    @staticmethod
    def _should_ignore(symbol: str, patterns: List[str]) -> bool:
        """
        检查资产是否应该被忽略 (类似 gitignore)

        支持的规则:
        - 精确匹配: "USDT"
        - 通配符: "BNB*", "*USDT", "*DOWN*"
        - 前缀匹配: "BNB"  (匹配 BNB, BNBUSDT, BNBBTC)

        :param symbol: 资产代码
        :param patterns: 忽略规则列表
        :return: 是否应该忽略
        """
        if not patterns:
            return False

        for pattern in patterns:
            # 精确匹配
            if pattern == symbol:
                return True

            # 通配符匹配
            if '*' in pattern or '?' in pattern:
                if fnmatch.fnmatch(symbol, pattern):
                    return True

            # 前缀/后缀匹配
            if pattern.startswith('*') and symbol.endswith(pattern[1:]):
                return True
            if pattern.endswith('*') and symbol.startswith(pattern[:-1]):
                return True

        return False


def get_crypto_assets(
    binance: BinanceClient,
    config: Dict,
    feishu_holdings: List[Dict] = None
) -> List[Dict]:
    """
    获取加密货币资产列表

    如果启用 auto_discover,从飞书和Binance获取;
    否则返回手动配置的列表

    :param binance: Binance 客户端
    :param config: 资产配置
    :param feishu_holdings: 飞书持仓数据
    :return: 资产列表
    """
    # 兼容旧格式 (数组)
    if isinstance(config, list):
        return config

    # 新格式 (字典)
    auto_discover = config.get('auto_discover', False)

    if auto_discover:
        ignore_patterns = config.get('ignore', [])
        return AssetDiscovery.discover_crypto_assets(binance, ignore_patterns, feishu_holdings)
    else:
        # 手动配置模式
        return config.get('manual', [])


def get_fund_assets(
    xueqiu: XueqiuClient,
    config: Dict,
    feishu_holdings: List[Dict] = None
) -> List[Dict]:
    """
    获取基金资产列表

    如果启用 auto_discover,从飞书持仓表和雪球自选股获取;
    否则返回手动配置的列表

    :param xueqiu: 雪球客户端
    :param config: 资产配置
    :param feishu_holdings: 飞书持仓数据
    :return: 资产列表
    """
    # 兼容旧格式 (数组)
    if isinstance(config, list):
        return config

    # 新格式 (字典)
    auto_discover = config.get('auto_discover', False)

    if auto_discover:
        ignore_patterns = config.get('ignore', [])
        return AssetDiscovery.discover_fund_assets(xueqiu, ignore_patterns, feishu_holdings)
    else:
        # 手动配置模式
        return config.get('manual', [])
