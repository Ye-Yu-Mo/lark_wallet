"""
简化版Binance客户端 - 直接调用REST API
不依赖第三方库,只使用requests
"""
import time
import hmac
import hashlib
from typing import Dict, Optional
from urllib.parse import urlencode
import requests


class SimpleBinanceClient:
    """
    简化版Binance API客户端

    只实现必要的功能:
    1. 获取账户余额
    2. 获取价格
    """

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        """
        初始化客户端

        :param api_key: API Key
        :param api_secret: API Secret
        :param testnet: 是否使用测试网
        """
        self.api_key = api_key
        self.api_secret = api_secret

        # 使用官方主域名
        if testnet:
            self.base_url = "https://testnet.binance.vision"
        else:
            self.base_url = "https://api.binance.com"

        self.session = requests.Session()
        self.session.headers.update({
            'X-MBX-APIKEY': self.api_key
        })

    def _generate_signature(self, params: Dict) -> str:
        """
        生成HMAC SHA256签名

        :param params: 参数字典
        :return: 签名字符串
        """
        query_string = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

    def _request(self, method: str, endpoint: str, signed: bool = False, **kwargs) -> Dict:
        """
        发送HTTP请求

        :param method: HTTP方法 (GET/POST)
        :param endpoint: API端点
        :param signed: 是否需要签名
        :param kwargs: 其他参数
        :return: 响应JSON
        """
        url = f"{self.base_url}{endpoint}"

        if signed:
            # 添加时间戳
            params = kwargs.get('params', {})
            params['timestamp'] = int(time.time() * 1000)
            params['recvWindow'] = 10000  # 10秒超时

            # 生成签名
            signature = self._generate_signature(params)
            params['signature'] = signature

            kwargs['params'] = params

        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP Error {e.response.status_code}"
            try:
                error_data = e.response.json()
                error_msg = f"{error_msg}: {error_data.get('msg', str(e))}"
            except:
                error_msg = f"{error_msg}: {str(e)}"
            raise Exception(error_msg)
        except Exception as e:
            raise Exception(f"请求失败: {str(e)}")

    def get_account_info(self) -> Dict:
        """
        获取账户信息(包含余额)

        :return: 账户信息
        """
        # omitZeroBalances=true 可以隐藏零余额,提升性能
        return self._request('GET', '/api/v3/account', signed=True, params={'omitZeroBalances': 'true'})

    def get_all_balances(self) -> Dict[str, float]:
        """
        获取所有非零余额

        :return: {symbol: balance} 字典
        """
        try:
            account_info = self.get_account_info()
            balances = {}

            for asset in account_info.get('balances', []):
                free = float(asset.get('free', 0))
                locked = float(asset.get('locked', 0))
                total = free + locked

                # 只保留非零余额
                if total > 0:
                    balances[asset['asset']] = total

            return balances

        except Exception as e:
            print(f"获取余额失败: {e}")
            return {}

    def get_price(self, symbol: str) -> Optional[float]:
        """
        获取单个交易对价格

        :param symbol: 交易对符号 (如 BTCUSDT)
        :return: 价格
        """
        try:
            result = self._request('GET', '/api/v3/ticker/price', params={'symbol': symbol})
            return float(result.get('price', 0))
        except Exception as e:
            print(f"获取价格失败 ({symbol}): {e}")
            return None

    def get_all_prices(self) -> Dict[str, float]:
        """
        获取所有交易对价格

        :return: {symbol: price} 字典
        """
        try:
            result = self._request('GET', '/api/v3/ticker/price')
            prices = {}

            for item in result:
                symbol = item.get('symbol')
                price = float(item.get('price', 0))
                if symbol:
                    prices[symbol] = price

            return prices

        except Exception as e:
            print(f"获取所有价格失败: {e}")
            return {}

    def get_usdt_price(self, asset: str) -> Optional[float]:
        """
        获取资产的USDT价格

        :param asset: 资产代码 (如 BTC)
        :return: USDT价格
        """
        # 特殊处理稳定币
        if asset in ['USDT', 'BUSD', 'USDC']:
            return 1.0

        # 尝试获取 {ASSET}USDT 交易对价格
        symbol = f"{asset}USDT"
        price = self.get_price(symbol)

        # 如果失败,尝试通过BTC中转
        if price is None and asset != 'BTC':
            btc_price = self.get_price(f"{asset}BTC")
            btc_usdt_price = self.get_price("BTCUSDT")

            if btc_price and btc_usdt_price:
                price = btc_price * btc_usdt_price

        return price

    def test_connectivity(self) -> bool:
        """
        测试连接性

        :return: 连接是否正常
        """
        try:
            # 获取BTC价格作为连接测试
            price = self.get_price("BTCUSDT")
            return price is not None
        except Exception as e:
            print(f"连接测试失败: {e}")
            return False

    def __str__(self) -> str:
        """字符串表示"""
        key_preview = self.api_key[:10] + "..." if len(self.api_key) > 10 else self.api_key
        return f"SimpleBinanceClient(api_key={key_preview}, base_url={self.base_url})"
