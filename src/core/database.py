"""
SQLite 数据库封装模块
管理资产价格、K线、持仓、订单等数据
"""
import sqlite3
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from contextlib import contextmanager


class AssetDB:
    """SQLite 数据库管理类"""

    def __init__(self, db_path='data/assets.db'):
        """
        初始化数据库连接

        :param db_path: 数据库文件路径
        """
        self.db_path = db_path

        # 确保数据目录存在
        db_dir = Path(db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        # 初始化数据库表
        self._init_tables()

    @contextmanager
    def _get_connection(self):
        """上下文管理器: 获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # 使查询结果可以通过列名访问
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_tables(self):
        """初始化数据库表结构"""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # 1. 价格历史表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    price REAL NOT NULL,
                    volume REAL,
                    source TEXT NOT NULL,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                )
            """)

            # 价格历史表索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_symbol_timestamp
                ON price_history(symbol, timestamp)
            """)

            # 2. K线表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS klines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    open_time INTEGER NOT NULL,
                    close_time INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    source TEXT NOT NULL
                )
            """)

            # K线表唯一索引 (防止重复数据)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_klines_unique
                ON klines(symbol, interval, open_time)
            """)

            # 3. 持仓表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS holdings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT UNIQUE NOT NULL,
                    asset_type TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    avg_cost REAL NOT NULL,
                    platform TEXT NOT NULL,
                    last_updated INTEGER NOT NULL
                )
            """)

            # 4. 订单历史表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT UNIQUE,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    price REAL NOT NULL,
                    quantity REAL NOT NULL,
                    timestamp INTEGER NOT NULL,
                    strategy TEXT,
                    status TEXT NOT NULL,
                    platform TEXT NOT NULL
                )
            """)

            # 订单表索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_orders_symbol_timestamp
                ON orders(symbol, timestamp)
            """)

    # ===== 价格相关方法 =====

    def save_price(self, symbol: str, price: float, volume: Optional[float], source: str) -> int:
        """
        保存价格数据

        :param symbol: 资产代码 (如 DOGE, BTC)
        :param price: 价格
        :param volume: 交易量 (可选)
        :param source: 数据源 (binance/xueqiu)
        :return: 插入的记录ID
        """
        timestamp = int(time.time())

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO price_history (symbol, timestamp, price, volume, source)
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, timestamp, price, volume, source))
            return cursor.lastrowid

    def get_latest_price(self, symbol: str) -> Optional[Dict]:
        """
        获取最新价格

        :param symbol: 资产代码
        :return: 价格记录字典,不存在返回None
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM price_history
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_price_history(self, symbol: str, start_time: int, end_time: int) -> List[Dict]:
        """
        获取历史价格

        :param symbol: 资产代码
        :param start_time: 起始时间戳 (秒)
        :param end_time: 结束时间戳 (秒)
        :return: 价格记录列表
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM price_history
                WHERE symbol = ? AND timestamp BETWEEN ? AND ?
                ORDER BY timestamp ASC
            """, (symbol, start_time, end_time))
            return [dict(row) for row in cursor.fetchall()]

    # ===== K线相关方法 =====

    def save_klines(self, symbol: str, interval: str, klines: List[List], source: str) -> int:
        """
        批量保存K线数据

        :param symbol: 资产代码
        :param interval: 周期 (1h, 4h, 1d)
        :param klines: K线数据列表 [[open_time, open, high, low, close, volume], ...]
        :param source: 数据源
        :return: 成功插入的记录数
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            count = 0

            for kline in klines:
                open_time = int(kline[0] / 1000) if kline[0] > 1e10 else kline[0]  # 毫秒转秒
                close_time = open_time + self._interval_to_seconds(interval)

                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO klines
                        (symbol, interval, open_time, close_time, open, high, low, close, volume, source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol, interval, open_time, close_time,
                        kline[1], kline[2], kline[3], kline[4], kline[5],
                        source
                    ))
                    count += 1
                except sqlite3.IntegrityError:
                    # 跳过重复数据
                    pass

            return count

    def get_klines(self, symbol: str, interval: str, limit: int = 100) -> List[Dict]:
        """
        获取K线数据

        :param symbol: 资产代码
        :param interval: 周期
        :param limit: 返回数量限制
        :return: K线记录列表
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM klines
                WHERE symbol = ? AND interval = ?
                ORDER BY open_time DESC
                LIMIT ?
            """, (symbol, interval, limit))
            return [dict(row) for row in cursor.fetchall()]

    @staticmethod
    def _interval_to_seconds(interval: str) -> int:
        """将时间周期转换为秒"""
        unit = interval[-1]
        value = int(interval[:-1])

        if unit == 'm':
            return value * 60
        elif unit == 'h':
            return value * 3600
        elif unit == 'd':
            return value * 86400
        else:
            return 3600  # 默认1小时

    # ===== 持仓相关方法 =====

    def update_holding(self, symbol: str, asset_type: str, quantity: float,
                       avg_cost: float, platform: str) -> None:
        """
        更新持仓信息

        :param symbol: 资产代码
        :param asset_type: 资产类型 (crypto/fund/stock)
        :param quantity: 持仓数量
        :param avg_cost: 平均成本
        :param platform: 平台 (binance/xueqiu)
        """
        timestamp = int(time.time())

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO holdings
                (symbol, asset_type, quantity, avg_cost, platform, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (symbol, asset_type, quantity, avg_cost, platform, timestamp))

    def get_holding(self, symbol: str) -> Optional[Dict]:
        """
        获取单个持仓信息

        :param symbol: 资产代码
        :return: 持仓记录字典,不存在返回None
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM holdings WHERE symbol = ?
            """, (symbol,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_holdings(self) -> List[Dict]:
        """
        获取所有持仓信息

        :return: 持仓记录列表
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM holdings ORDER BY last_updated DESC")
            return [dict(row) for row in cursor.fetchall()]

    # ===== 订单相关方法 =====

    def save_order(self, order_data: Dict) -> int:
        """
        保存订单

        :param order_data: 订单数据字典
        :return: 插入的记录ID
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO orders
                (order_id, symbol, side, order_type, price, quantity,
                 timestamp, strategy, status, platform)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                order_data.get('order_id'),
                order_data['symbol'],
                order_data['side'],
                order_data['order_type'],
                order_data['price'],
                order_data['quantity'],
                order_data['timestamp'],
                order_data.get('strategy'),
                order_data['status'],
                order_data['platform']
            ))
            return cursor.lastrowid

    def get_orders(self, symbol: Optional[str] = None,
                   start_time: Optional[int] = None) -> List[Dict]:
        """
        查询订单

        :param symbol: 资产代码 (可选,不指定则查询所有)
        :param start_time: 起始时间戳 (可选)
        :return: 订单记录列表
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            if symbol and start_time:
                cursor.execute("""
                    SELECT * FROM orders
                    WHERE symbol = ? AND timestamp >= ?
                    ORDER BY timestamp DESC
                """, (symbol, start_time))
            elif symbol:
                cursor.execute("""
                    SELECT * FROM orders
                    WHERE symbol = ?
                    ORDER BY timestamp DESC
                """, (symbol,))
            elif start_time:
                cursor.execute("""
                    SELECT * FROM orders
                    WHERE timestamp >= ?
                    ORDER BY timestamp DESC
                """, (start_time,))
            else:
                cursor.execute("""
                    SELECT * FROM orders
                    ORDER BY timestamp DESC
                """)

            return [dict(row) for row in cursor.fetchall()]

    # ===== 数据库维护方法 =====

    def vacuum(self):
        """清理数据库,回收空间"""
        with self._get_connection() as conn:
            conn.execute("VACUUM")

    def get_table_info(self, table_name: str) -> List[Tuple]:
        """获取表结构信息"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            return cursor.fetchall()

    def get_table_count(self, table_name: str) -> int:
        """获取表记录数"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cursor.fetchone()[0]
