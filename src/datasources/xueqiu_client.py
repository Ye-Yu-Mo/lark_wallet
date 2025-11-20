"""
雪球数据源客户端
使用 pysnowball 库封装雪球 API
"""
from typing import Optional, List, Dict, Any
from datasources.base import DataSource


class XueqiuClient(DataSource):
    """
    雪球数据源

    支持获取基金净值、股票行情等数据
    使用 Cookie 认证
    """

    def __init__(self, cookies: str):
        """
        初始化雪球客户端

        :param cookies: 雪球网站的 cookies 字符串
        """
        self.cookies = cookies

        # 设置 pysnowball 的 cookies
        try:
            import pysnowball as ball
            from pysnowball import token

            # 设置 token (pysnowball 0.1.8 需要设置)
            token.set_token(cookies)
            self.ball = ball
        except Exception as e:
            print(f"导入pysnowball失败: {e}")
            raise

    def get_name(self) -> str:
        """获取数据源名称"""
        return 'xueqiu'

    def get_price(self, symbol: str) -> Optional[float]:
        """
        获取当前价格（基金净值或股票价格）

        :param symbol: 资产代码 (基金代码或股票代码)
                      基金示例: '000001' (华夏成长)
                      股票示例: 'SH600519' (贵州茅台)
        :return: 当前价格/净值，失败返回 None
        """
        try:
            # 使用 pysnowball 0.1.8 API
            result = self.ball.quotec(symbol)

            if result and 'data' in result and len(result['data']) > 0:
                quote_data = result['data'][0]  # data 是数组,取第一个元素
                # 尝试获取当前价格
                return quote_data.get('current') or quote_data.get('close')

            return None

        except Exception as e:
            print(f"获取价格失败 ({symbol}): {e}")
            return None

    def get_balance(self, symbol: str) -> Optional[float]:
        """
        获取账户余额

        注意: 雪球API不直接支持持仓查询
        如需持仓信息，需要从配置文件读取

        :param symbol: 资产代码
        :return: None (不支持)
        """
        # 雪球不支持余额查询，需要从配置文件读取
        return None

    def get_klines(self, symbol: str, interval: str = '1d', limit: int = 100) -> List[List]:
        """
        获取K线数据

        :param symbol: 资产代码
        :param interval: 时间周期 (支持 1d, 1w, 1M, 1h, 30m, 1m)
        :param limit: 返回数量限制
        :return: K线数据 [[timestamp, open, high, low, close, volume], ...]
        """
        try:
            # pysnowball 的 period 参数
            # day: 日K, week: 周K, month: 月K, 60m: 60分钟, 30m: 30分钟, 1m: 1分钟
            period_map = {
                '1d': 'day',
                '1w': 'week',
                '1M': 'month',
                '1h': '60m',
                '30m': '30m',
                '1m': '1m'
            }
            period = period_map.get(interval, 'day')

            # 使用 pysnowball 0.1.8 API
            result = self.ball.kline(symbol, period=period, count=limit)

            if not result or 'data' not in result:
                return []

            data = result['data']
            klines = data.get('items', [])
            if not klines:
                return []

            # 转换为标准格式 [timestamp, open, high, low, close, volume]
            result_list = []
            for item in klines:
                result_list.append([
                    item.get('timestamp', 0),
                    item.get('open', 0),
                    item.get('high', 0),
                    item.get('low', 0),
                    item.get('close', 0),
                    item.get('volume', 0)
                ])

            return result_list

        except Exception as e:
            print(f"获取K线失败 ({symbol}): {e}")
            return []

    def get_fund_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取基金详细信息

        :param symbol: 基金代码
        :return: 基金信息字典
        """
        try:
            # 使用 pysnowball 0.1.8 API
            result = self.ball.quote_detail(symbol)

            if not result or 'data' not in result:
                return None

            data = result['data']
            if 'quote' not in data:
                return None

            quote = data['quote']

            return {
                'symbol': quote.get('symbol'),
                'name': quote.get('name'),
                'current': quote.get('current'),
                'percent': quote.get('percent'),
                'chg': quote.get('chg'),
                'high': quote.get('high'),
                'low': quote.get('low'),
                'open': quote.get('open'),
                'last_close': quote.get('last_close'),
                'volume': quote.get('volume'),
                'amount': quote.get('amount'),
                'market_capital': quote.get('market_capital'),
                'timestamp': quote.get('timestamp')
            }

        except Exception as e:
            print(f"获取基金信息失败 ({symbol}): {e}")
            return None

    def get_ticker_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取ticker详细信息 (与get_fund_info类似)

        :param symbol: 资产代码
        :return: ticker信息字典
        """
        return self.get_fund_info(symbol)

    def validate_symbol(self, symbol: str) -> bool:
        """
        验证资产代码是否有效

        :param symbol: 资产代码
        :return: 是否有效
        """
        try:
            # 尝试获取价格，如果成功则有效
            price = self.get_price(symbol)
            return price is not None
        except Exception:
            return False

    def search_stock(self, keyword: str, limit: int = 10) -> List[Dict]:
        """
        搜索股票/基金 (扩展方法)

        :param keyword: 搜索关键词
        :param limit: 返回数量限制
        :return: 搜索结果列表
        """
        try:
            # 使用 pysnowball 0.1.8 API
            data = self.ball.search(query=keyword, count=limit)

            if not data or 'data' not in data:
                return []

            results = data['data']
            if not results:
                return []

            return [
                {
                    'symbol': item.get('code'),
                    'name': item.get('name'),
                    'type': item.get('type')
                }
                for item in results
            ]

        except Exception as e:
            print(f"搜索失败 ({keyword}): {e}")
            return []

    def get_stock_detail(self, symbol: str) -> Optional[Dict]:
        """
        获取股票/基金详细信息 (扩展方法)

        :param symbol: 资产代码
        :return: 详细信息字典
        """
        try:
            # 使用 pysnowball 0.1.8 API
            result = self.ball.quote_detail(symbol)

            if not result or 'data' not in result:
                return None

            data = result['data']
            if 'quote' not in data:
                return None

            detail = data['quote']

            return {
                'symbol': detail.get('symbol'),
                'name': detail.get('name'),
                'type': detail.get('type'),
                'exchange': detail.get('exchange'),
                'current': detail.get('current'),
                'percent': detail.get('percent'),
                'market_capital': detail.get('market_capital'),
                'pe_ttm': detail.get('pe_ttm'),
                'pb': detail.get('pb'),
                'dividend_yield': detail.get('dividend_yield')
            }

        except Exception as e:
            print(f"获取详细信息失败 ({symbol}): {e}")
            return None

    def test_connectivity(self) -> bool:
        """
        测试连接性 (通过尝试获取热门股票)

        :return: 连接是否正常
        """
        try:
            # 尝试获取上证指数行情
            data = self.ball.quotec('SH000001')
            return data is not None and 'data' in data
        except Exception as e:
            print(f"连接测试失败: {e}")
            return False

    def __str__(self) -> str:
        """字符串表示"""
        cookie_preview = self.cookies[:20] + "..." if len(self.cookies) > 20 else self.cookies
        return f"XueqiuClient(cookies={cookie_preview})"
