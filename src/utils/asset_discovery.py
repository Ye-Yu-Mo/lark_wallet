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
        ignore_patterns: List[str] = None
    ) -> List[Dict]:
        """
        从 Binance 自动发现加密货币资产

        :param binance: Binance 客户端
        :param ignore_patterns: 忽略规则列表 (支持通配符)
                               示例: ['*USDT', 'BNB', 'BUSD*']
        :return: 资产列表
        """
        ignore_patterns = ignore_patterns or []

        logger.info("开始自动发现加密货币资产...")

        try:
            # 获取所有余额
            all_balances = binance.get_all_balances()

            if not all_balances:
                logger.warning("未发现任何加密货币持仓")
                return []

            logger.info(f"发现 {len(all_balances)} 个加密货币资产")

            # 过滤资产
            assets = []
            ignored_count = 0

            for symbol, balance in all_balances.items():
                # 检查是否匹配 ignore 规则
                if AssetDiscovery._should_ignore(symbol, ignore_patterns):
                    logger.debug(f"忽略资产: {symbol} (匹配规则)")
                    ignored_count += 1
                    continue

                # 构建资产配置
                assets.append({
                    'symbol': symbol,
                    'trading_pair': f"{symbol}/USDT",
                    'name': symbol,
                    'quantity': balance,
                    'avg_cost': 0  # 成本需要从飞书或手动配置获取
                })

            logger.info(f"加密货币资产发现完成: 有效 {len(assets)} 个, 忽略 {ignored_count} 个")

            return assets

        except Exception as e:
            logger.error(f"加密货币资产发现失败: {e}")
            return []

    @staticmethod
    def discover_fund_assets(
        xueqiu: XueqiuClient,
        ignore_patterns: List[str] = None,
        holdings_data: List[Dict] = None
    ) -> List[Dict]:
        """
        从雪球/飞书自动发现基金资产

        注意: 雪球 API 不直接支持获取持仓列表,
        需要从飞书持仓表读取已有基金列表

        :param xueqiu: 雪球客户端
        :param ignore_patterns: 忽略规则列表
        :param holdings_data: 从飞书读取的持仓数据
        :return: 资产列表
        """
        ignore_patterns = ignore_patterns or []

        logger.info("开始自动发现基金资产...")

        try:
            if not holdings_data:
                logger.warning("未提供持仓数据,无法发现基金资产")
                logger.info("提示: 基金资产需要从飞书持仓表读取")
                return []

            # 过滤基金类型的资产
            fund_holdings = [
                h for h in holdings_data
                if h.get('资产类型') == '基金'
            ]

            if not fund_holdings:
                logger.warning("飞书持仓表中未发现基金资产")
                return []

            logger.info(f"从飞书发现 {len(fund_holdings)} 只基金")

            # 过滤资产
            assets = []
            ignored_count = 0

            for holding in fund_holdings:
                symbol = holding.get('资产代码')
                if not symbol:
                    continue

                # 检查是否匹配 ignore 规则
                if AssetDiscovery._should_ignore(symbol, ignore_patterns):
                    logger.debug(f"忽略基金: {symbol} (匹配规则)")
                    ignored_count += 1
                    continue

                # 构建资产配置
                assets.append({
                    'symbol': symbol,
                    'name': holding.get('资产名称', symbol),
                    'shares': holding.get('数量', 0),
                    'avg_cost': holding.get('成本价', 0)
                })

            logger.info(f"基金资产发现完成: 有效 {len(assets)} 个, 忽略 {ignored_count} 个")

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
    config: Dict
) -> List[Dict]:
    """
    获取加密货币资产列表

    如果启用 auto_discover,自动从 Binance 获取;
    否则返回手动配置的列表

    :param binance: Binance 客户端
    :param config: 资产配置
    :return: 资产列表
    """
    # 兼容旧格式 (数组)
    if isinstance(config, list):
        return config

    # 新格式 (字典)
    auto_discover = config.get('auto_discover', False)

    if auto_discover:
        ignore_patterns = config.get('ignore', [])
        return AssetDiscovery.discover_crypto_assets(binance, ignore_patterns)
    else:
        # 手动配置模式
        return config.get('manual', [])


def get_fund_assets(
    xueqiu: XueqiuClient,
    config: Dict,
    holdings_data: List[Dict] = None
) -> List[Dict]:
    """
    获取基金资产列表

    如果启用 auto_discover,从飞书持仓表读取;
    否则返回手动配置的列表

    :param xueqiu: 雪球客户端
    :param config: 资产配置
    :param holdings_data: 飞书持仓数据
    :return: 资产列表
    """
    # 兼容旧格式 (数组)
    if isinstance(config, list):
        return config

    # 新格式 (字典)
    auto_discover = config.get('auto_discover', False)

    if auto_discover:
        ignore_patterns = config.get('ignore', [])
        return AssetDiscovery.discover_fund_assets(xueqiu, ignore_patterns, holdings_data)
    else:
        # 手动配置模式
        return config.get('manual', [])
