"""
数据源抽象基类
定义统一的数据源接口规范
"""
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any


class DataSource(ABC):
    """
    数据源抽象基类

    所有数据源(Binance, 雪球等)必须继承此类并实现所有抽象方法
    确保接口统一，方便扩展新的数据源
    """

    @abstractmethod
    def get_price(self, symbol: str) -> Optional[float]:
        """
        获取当前价格

        :param symbol: 资产代码 (如 DOGE, BTC, 基金代码)
        :return: 当前价格，失败返回 None
        """
        pass

    @abstractmethod
    def get_balance(self, symbol: str) -> Optional[float]:
        """
        获取账户余额/持仓数量

        :param symbol: 资产代码
        :return: 余额/持仓数量，失败或不支持返回 None
        """
        pass

    @abstractmethod
    def get_klines(self, symbol: str, interval: str = '1h', limit: int = 100) -> List[List]:
        """
        获取K线数据

        :param symbol: 资产代码
        :param interval: 时间周期 (如 1m, 5m, 1h, 1d)
        :param limit: 返回数量限制
        :return: K线数据列表 [[timestamp, open, high, low, close, volume], ...]
                 失败返回空列表
        """
        pass

    @abstractmethod
    def get_name(self) -> str:
        """
        获取数据源名称

        :return: 数据源标识 (如 'binance', 'xueqiu')
        """
        pass

    # 可选方法 (子类可以选择性实现)

    def get_ticker_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取ticker详细信息 (可选)

        :param symbol: 资产代码
        :return: ticker信息字典，包含价格、交易量、涨跌幅等
        """
        return None

    def get_24h_volume(self, symbol: str) -> Optional[float]:
        """
        获取24小时交易量 (可选)

        :param symbol: 资产代码
        :return: 24小时交易量
        """
        return None

    def validate_symbol(self, symbol: str) -> bool:
        """
        验证资产代码是否有效 (可选)

        :param symbol: 资产代码
        :return: 是否有效
        """
        return True

    def __str__(self) -> str:
        """字符串表示"""
        return f"DataSource({self.get_name()})"

    def __repr__(self) -> str:
        """开发者表示"""
        return self.__str__()
