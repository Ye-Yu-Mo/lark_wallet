"""
Binance 数据源客户端
使用 ccxt 库封装币安 API
"""
import ccxt
from typing import Optional, List, Dict, Any
from datasources.base import DataSource


class BinanceClient(DataSource):
    """
    币安交易所数据源

    支持获取实时价格、账户余额、K线数据等
    使用 ccxt 库实现，自动处理限流
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

        # 初始化 ccxt 交易所实例
        self.exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,  # 自动限流
            'options': {
                'defaultType': 'spot',  # 现货交易
                'adjustForTimeDifference': True  # 自动调整时间差
            }
        })

        # 如果是测试网
        if testnet:
            self.exchange.set_sandbox_mode(True)

    def get_name(self) -> str:
        """获取数据源名称"""
        return 'binance_testnet' if self.testnet else 'binance'

    def get_price(self, symbol: str) -> Optional[float]:
        """
        获取当前价格

        :param symbol: 交易对 (如 'DOGE/USDT', 'BTC/USDT')
        :return: 当前价格，失败返回 None
        """
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return ticker.get('last') or ticker.get('close')
        except ccxt.NetworkError as e:
            print(f"网络错误: {e}")
            return None
        except ccxt.ExchangeError as e:
            print(f"交易所错误: {e}")
            return None
        except Exception as e:
            print(f"获取价格失败 ({symbol}): {e}")
            return None

    def get_balance(self, symbol: str) -> Optional[float]:
        """
        获取账户余额

        :param symbol: 资产代码 (如 'DOGE', 'BTC', 'USDT')
        :return: 可用余额，失败返回 None
        """
        try:
            balance = self.exchange.fetch_balance()
            # 返回 free (可用) 余额
            return balance.get('free', {}).get(symbol, 0.0)
        except ccxt.AuthenticationError as e:
            print(f"认证错误: {e}")
            return None
        except ccxt.NetworkError as e:
            print(f"网络错误: {e}")
            return None
        except Exception as e:
            print(f"获取余额失败 ({symbol}): {e}")
            return None

    def get_klines(self, symbol: str, interval: str = '1h', limit: int = 100) -> List[List]:
        """
        获取K线数据

        :param symbol: 交易对 (如 'DOGE/USDT')
        :param interval: 时间周期 (1m, 5m, 15m, 1h, 4h, 1d)
        :param limit: 返回数量限制
        :return: K线数据 [[timestamp, open, high, low, close, volume], ...]
        """
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe=interval, limit=limit)
            return ohlcv
        except ccxt.NetworkError as e:
            print(f"网络错误: {e}")
            return []
        except ccxt.ExchangeError as e:
            print(f"交易所错误: {e}")
            return []
        except Exception as e:
            print(f"获取K线失败 ({symbol}): {e}")
            return []

    def get_ticker_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取ticker详细信息

        :param symbol: 交易对
        :return: ticker信息字典
        """
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return {
                'symbol': ticker.get('symbol'),
                'last': ticker.get('last'),
                'bid': ticker.get('bid'),
                'ask': ticker.get('ask'),
                'high': ticker.get('high'),
                'low': ticker.get('low'),
                'volume': ticker.get('quoteVolume'),  # USDT计价的交易量
                'base_volume': ticker.get('baseVolume'),  # 基础货币交易量
                'change': ticker.get('change'),
                'percentage': ticker.get('percentage'),
                'timestamp': ticker.get('timestamp')
            }
        except Exception as e:
            print(f"获取ticker信息失败 ({symbol}): {e}")
            return None

    def get_24h_volume(self, symbol: str) -> Optional[float]:
        """
        获取24小时交易量 (USDT计价)

        :param symbol: 交易对
        :return: 24小时交易量
        """
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return ticker.get('quoteVolume')  # USDT计价的交易量
        except Exception as e:
            print(f"获取24h交易量失败 ({symbol}): {e}")
            return None

    def validate_symbol(self, symbol: str) -> bool:
        """
        验证交易对是否有效

        :param symbol: 交易对
        :return: 是否有效
        """
        try:
            # 加载市场信息
            if not self.exchange.markets:
                self.exchange.load_markets()

            return symbol in self.exchange.markets
        except Exception as e:
            print(f"验证交易对失败 ({symbol}): {e}")
            return False

    def get_all_balances(self) -> Dict[str, float]:
        """
        获取所有资产余额 (扩展方法)

        :return: 资产余额字典 {'BTC': 0.1, 'USDT': 1000, ...}
        """
        try:
            balance = self.exchange.fetch_balance()
            # 只返回有余额的资产
            return {
                asset: amount
                for asset, amount in balance.get('free', {}).items()
                if amount > 0
            }
        except Exception as e:
            print(f"获取所有余额失败: {e}")
            return {}

    def get_order_book(self, symbol: str, limit: int = 10) -> Optional[Dict]:
        """
        获取订单簿 (扩展方法)

        :param symbol: 交易对
        :param limit: 深度限制
        :return: 订单簿数据 {'bids': [...], 'asks': [...]}
        """
        try:
            order_book = self.exchange.fetch_order_book(symbol, limit)
            return {
                'bids': order_book.get('bids', [])[:limit],
                'asks': order_book.get('asks', [])[:limit],
                'timestamp': order_book.get('timestamp')
            }
        except Exception as e:
            print(f"获取订单簿失败 ({symbol}): {e}")
            return None

    def test_connectivity(self) -> bool:
        """
        测试连接性

        :return: 连接是否正常
        """
        try:
            # 获取服务器时间
            self.exchange.fetch_time()
            return True
        except Exception as e:
            print(f"连接测试失败: {e}")
            return False

    def __str__(self) -> str:
        """字符串表示"""
        mode = "测试网" if self.testnet else "主网"
        return f"BinanceClient({mode})"
