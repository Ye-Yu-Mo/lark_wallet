"""
Binance 数据源客户端
直接调用Binance REST API,不依赖ccxt
"""
from typing import Optional, List, Dict, Any
from datasources.base import DataSource
from datasources.simple_binance_client import SimpleBinanceClient


class BinanceClient(DataSource):
    """
    币安交易所数据源

    支持获取实时价格、账户余额等
    直接调用Binance REST API
    """

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        """
        初始化币安客户端

        :param api_key: Binance API Key
        :param api_secret: Binance API Secret
        :param testnet: 是否使用测试网
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet

        # 使用简化版客户端
        self.client = SimpleBinanceClient(api_key, api_secret, testnet)

    def get_name(self) -> str:
        """获取数据源名称"""
        return 'binance_testnet' if self.testnet else 'binance'

    def get_price(self, symbol: str) -> Optional[float]:
        """
        获取交易对价格

        :param symbol: 交易对符号 (如 BTC/USDT 或 BTCUSDT)
        :return: 当前价格
        """
        # 标准化交易对格式 (移除/)
        symbol = symbol.replace('/', '')

        return self.client.get_price(symbol)

    def get_balance(self, symbol: str) -> Optional[float]:
        """
        获取指定资产余额

        :param symbol: 资产代码 (如 BTC)
        :return: 余额
        """
        try:
            balances = self.client.get_all_balances()
            return balances.get(symbol, 0)
        except Exception as e:
            print(f"获取余额失败 ({symbol}): {e}")
            return None

    def get_all_balances(self) -> Dict[str, float]:
        """
        获取所有非零余额

        :return: {asset: balance} 字典
        """
        return self.client.get_all_balances()

    def get_klines(self, symbol: str, interval: str = '1d', limit: int = 100) -> List[List]:
        """
        获取K线数据

        注意: 当前简化版不支持K线,返回空列表

        :param symbol: 交易对符号
        :param interval: 时间周期
        :param limit: 数量限制
        :return: K线数据
        """
        # TODO: 如需K线功能,需要实现 /api/v3/klines 接口
        print(f"警告: 简化版Binance客户端暂不支持K线数据")
        return []

    def get_ticker_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取交易对详细信息

        :param symbol: 交易对符号
        :return: 交易对信息
        """
        # 标准化交易对格式
        symbol = symbol.replace('/', '')

        try:
            price = self.client.get_price(symbol)
            if price is None:
                return None

            return {
                'symbol': symbol,
                'price': price,
                'last': price,  # 兼容字段
                'close': price  # 兼容字段
            }
        except Exception as e:
            print(f"获取ticker信息失败 ({symbol}): {e}")
            return None

    def validate_symbol(self, symbol: str) -> bool:
        """
        验证交易对是否有效

        :param symbol: 交易对符号
        :return: 是否有效
        """
        try:
            price = self.get_price(symbol)
            return price is not None
        except Exception:
            return False

    def test_connectivity(self) -> bool:
        """
        测试连接性

        :return: 连接是否正常
        """
        return self.client.test_connectivity()

    def __str__(self) -> str:
        """字符串表示"""
        return str(self.client)
