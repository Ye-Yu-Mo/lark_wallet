"""
配置管理模块
支持账单导入配置 + 资产同步配置
"""
import json
import os
import re


class Config:
    """配置管理类"""

    def __init__(self, config_file='config.json'):
        """加载配置文件"""
        self.config_file = config_file

        if not os.path.exists(config_file):
            raise FileNotFoundError(f"配置文件不存在: {config_file}")

        with open(config_file, 'r', encoding='utf-8') as f:
            self.data = json.load(f)

        # 验证配置
        self._validate_config()

    def _validate_config(self):
        """验证配置文件格式和必需字段"""
        # 验证accounts
        if 'accounts' not in self.data:
            raise ValueError("配置文件缺少'accounts'字段")

        accounts = self.data['accounts']
        if not isinstance(accounts, dict) or len(accounts) == 0:
            raise ValueError("'accounts'必须是非空字典")

        # 验证每个账本配置
        for account_name, account in accounts.items():
            # 必需字段
            required_fields = ['app_token', 'table_id', 'name', 'data_dir']
            for field in required_fields:
                if field not in account:
                    raise ValueError(f"账本'{account_name}'缺少必需字段'{field}'")

            # 验证app_token格式 (飞书app_token通常以大写字母开头)
            if not re.match(r'^[A-Za-z0-9]{20,}$', account['app_token']):
                raise ValueError(f"账本'{account_name}'的app_token格式不正确")

            # 验证table_id格式
            if not re.match(r'^tbl[A-Za-z0-9]{10,}$', account['table_id']):
                raise ValueError(f"账本'{account_name}'的table_id格式不正确 (应以'tbl'开头)")

            # 验证data_dir存在
            if not os.path.exists(account['data_dir']):
                raise ValueError(f"账本'{account_name}'的数据目录不存在: {account['data_dir']}")

        # 验证mcp_server配置
        if 'mcp_server' not in self.data:
            raise ValueError("配置文件缺少'mcp_server'字段")

        mcp_server = self.data['mcp_server']
        if 'app_id' not in mcp_server or 'app_secret' not in mcp_server:
            raise ValueError("'mcp_server'缺少'app_id'或'app_secret'字段")

        # 验证import_settings
        if 'import_settings' in self.data:
            settings = self.data['import_settings']
            if 'batch_size' in settings:
                batch_size = settings['batch_size']
                if not isinstance(batch_size, int) or batch_size <= 0 or batch_size > 500:
                    raise ValueError("'batch_size'必须是1-500之间的整数")

        # 验证asset_sync配置 (可选,但如果存在则验证)
        if 'asset_sync' in self.data:
            self._validate_asset_sync_config()

    def _validate_asset_sync_config(self):
        """验证资产同步配置"""
        asset_sync = self.data['asset_sync']

        # 验证飞书配置
        if 'feishu' in asset_sync:
            feishu = asset_sync['feishu']
            if 'app_token' not in feishu:
                raise ValueError("asset_sync.feishu 缺少 'app_token'")
            if 'tables' not in feishu:
                raise ValueError("asset_sync.feishu 缺少 'tables'")

            tables = feishu['tables']
            required_tables = ['holdings', 'history', 'logs']
            for table_name in required_tables:
                if table_name not in tables:
                    raise ValueError(f"asset_sync.feishu.tables 缺少 '{table_name}'")

        # 验证binance配置
        if 'binance' in asset_sync and asset_sync['binance'].get('enabled'):
            binance = asset_sync['binance']
            if 'api_key' not in binance or 'api_secret' not in binance:
                raise ValueError("binance已启用但缺少 'api_key' 或 'api_secret'")

        # 验证xueqiu配置
        if 'xueqiu' in asset_sync and asset_sync['xueqiu'].get('enabled'):
            xueqiu = asset_sync['xueqiu']
            if 'cookies' not in xueqiu:
                raise ValueError("xueqiu已启用但缺少 'cookies'")

        # 验证飞书备份配置
        feishu_backup = asset_sync.get('feishu_backup')
        if feishu_backup:
            tables = feishu_backup.get('tables')
            if tables is not None and not isinstance(tables, list):
                raise ValueError("feishu_backup.tables 必须是数组")

            page_size = feishu_backup.get('page_size', 200)
            if not isinstance(page_size, int) or page_size <= 0 or page_size > 500:
                raise ValueError("feishu_backup.page_size 必须在 1-500 之间")

    # ===== 账单导入相关方法 =====

    def get_account(self, account_name):
        """获取账本配置"""
        accounts = self.data.get('accounts', {})
        if account_name not in accounts:
            raise ValueError(f"账本 '{account_name}' 不存在")
        return accounts[account_name]

    def get_mcp_server_config(self):
        """获取MCP服务器配置"""
        config = self.data.get('mcp_server', {})
        command = config.get('command', [])
        command.extend([
            '-a', config.get('app_id'),
            '-s', config.get('app_secret')
        ])
        return command

    def get_import_settings(self):
        """获取导入设置"""
        return self.data.get('import_settings', {
            'batch_size': 500,
            'delay_between_records': 0.001,
            'delay_between_batches': 0.001
        })

    def list_accounts(self):
        """列出所有账本"""
        return list(self.data.get('accounts', {}).keys())

    def update_last_import_timestamp(self, account_name, source_type, timestamp):
        """
        更新账单来源的最后导入时间戳
        :param account_name: 账本名称
        :param source_type: 来源类型 'alipay' 或 'wechat'
        :param timestamp: 时间戳
        """
        account = self.data['accounts'][account_name]
        if 'last_import_timestamp' not in account:
            account['last_import_timestamp'] = {}

        account['last_import_timestamp'][source_type] = timestamp
        self.save()

    def get_last_import_timestamp(self, account_name, source_type):
        """
        获取账单来源的最后导入时间戳
        :param account_name: 账本名称
        :param source_type: 来源类型 'alipay' 或 'wechat'
        :return: 时间戳
        """
        account = self.data['accounts'][account_name]
        return account.get('last_import_timestamp', {}).get(source_type, 0)

    # ===== 资产同步相关方法 =====

    def is_asset_sync_enabled(self):
        """检查是否启用资产同步"""
        return self.data.get('asset_sync', {}).get('enabled', False)

    def get_asset_sync_config(self):
        """获取资产同步配置"""
        if 'asset_sync' not in self.data:
            raise ValueError("配置文件缺少 'asset_sync' 节点")
        return self.data['asset_sync']

    def get_feishu_config(self):
        """获取飞书配置 (资产同步用)"""
        asset_sync = self.get_asset_sync_config()
        if 'feishu' not in asset_sync:
            raise ValueError("asset_sync 缺少 'feishu' 配置")

        feishu = asset_sync['feishu']
        mcp_server = self.data['mcp_server']

        return {
            'app_id': mcp_server['app_id'],
            'app_secret': mcp_server['app_secret'],
            'app_token': feishu['app_token'],
            'tables': feishu['tables']
        }

    def get_binance_config(self):
        """获取币安配置"""
        asset_sync = self.get_asset_sync_config()
        if 'binance' not in asset_sync:
            raise ValueError("asset_sync 缺少 'binance' 配置")
        return asset_sync['binance']

    def get_xueqiu_config(self):
        """获取雪球配置"""
        asset_sync = self.get_asset_sync_config()
        if 'xueqiu' not in asset_sync:
            raise ValueError("asset_sync 缺少 'xueqiu' 配置")
        return asset_sync['xueqiu']

    def get_scheduler_config(self):
        """获取定时任务配置"""
        asset_sync = self.get_asset_sync_config()
        if 'scheduler' not in asset_sync:
            raise ValueError("asset_sync 缺少 'scheduler' 配置")
        return asset_sync['scheduler']

    def get_assets(self):
        """获取资产列表"""
        asset_sync = self.get_asset_sync_config()
        return asset_sync.get('assets', {'crypto': [], 'funds': []})

    def get_database_config(self):
        """获取数据库配置"""
        asset_sync = self.get_asset_sync_config()
        return asset_sync.get('database', {'path': 'data/assets.db'})

    def get_logging_config(self):
        """获取日志配置"""
        asset_sync = self.get_asset_sync_config()
        return asset_sync.get('logging', {
            'level': 'INFO',
            'path': 'logs/',
            'max_bytes': 10485760,
            'backup_count': 5
        })

    def get_feishu_backup_config(self):
        """获取飞书备份配置"""
        asset_sync = self.get_asset_sync_config()
        return asset_sync.get('feishu_backup', {'enabled': False})

    # ===== 通用方法 =====

    def save(self):
        """保存配置到文件"""
        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
