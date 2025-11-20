# 飞书财务管理工具集

两个独立的财务管理工具:
1. **账单导入工具** - 自动解析支付宝/微信账单并导入飞书多维表格
2. **资产同步系统** - 定时从 Binance/雪球获取资产数据并同步到飞书

---

## 工具1: 账单导入工具

### 功能

批量导入支付宝和微信账单到飞书多维表格。

**核心特性**:
- 自动解析账单 (支付宝CSV / 微信XLSX)
- 智能分类 (基于交易对方和关键词)
- 增量导入 (只导入新记录)
- 批量处理 (500条/批次)
- 去重机制

### 快速开始

#### 1. 安装依赖

```bash
uv sync
```

#### 2. 配置

编辑 `config.json`:

```json
{
  "accounts": {
    "jasxu": {
      "app_token": "P0dsbK6vSa...",
      "table_id": "tblGlYT4on...",
      "name": "jasxu的账本",
      "data_dir": "data",
      "last_import_timestamp": {
        "alipay": 0,
        "wechat": 0
      }
    }
  },
  "mcp_server": {
    "app_id": "cli_a997302b31b11013",
    "app_secret": "xZVA2kUH..."
  }
}
```

#### 3. 准备账单文件

将账单文件放到 `data_dir` 目录:
- 支付宝账单: `*.csv` (GBK编码)
- 微信账单: `*.xlsx`

支持子目录递归扫描。

#### 4. 运行导入

```bash
# 导入所有账本
python import.py

# 只导入指定账本
python import.py --account jasxu

# 使用自定义配置
python import.py --config my_config.json
```

### 智能分类

系统会自动将交易分类为:

| 分类 | 示例关键词 |
|------|-----------|
| 餐饮 | 餐厅、外卖、美团、饿了么、咖啡 |
| 交通 | 滴滴、地铁、加油、停车、高铁 |
| 购物 | 淘宝、京东、超市、便利店 |
| 娱乐 | 电影、游戏、健身、KTV |
| 固定支出 | 水电、物业、房租、话费 |
| 家用 | 家政、维修、装修 |
| 医疗 | 医院、药店、体检 |
| 学习办公 | 教育、培训、书籍、办公用品 |
| 理发美容 | 理发、美发、美容、美甲 |
| 家庭支出 | 家人转账 |
| 红包转账 | 红包、转账、提现 |
| 贷款 | 花呗、借呗、信用卡 |

分类规则可在 `lib/smart_categorizer.py` 中自定义。

### 飞书表格结构

目标表格需包含以下字段:

| 字段 | 类型 | 说明 |
|------|------|------|
| 备注 | 文本 | 交易描述 |
| 日期 | 日期 | 交易时间戳(毫秒) |
| 收支 | 单选 | "收入" 或 "支出" |
| 分类 | 单选 | 智能分类结果 |
| 金额 | 数字 | 交易金额 |

### 工作流程

```
1. 扫描data目录 → 找到所有*.csv和*.xlsx
2. 解析账单文件 → 提取交易记录
3. 智能分类 → 基于关键词和交易对方
4. 过滤新记录 → 基于last_import_timestamp
5. 批量导入 → 500条/批次
6. 更新时间戳 → 记录最大导入时间
```

---

## 工具2: 资产同步系统

### 功能

定时同步加密货币和基金资产数据。

**核心特性**:
- 自动资产发现 (Binance余额 / 飞书持仓表)
- 定时同步 (加密货币每小时 / 基金每天)
- 双存储 (SQLite本地 + 飞书云端)
- 每日快照
- 自动备份
- 告警通知

### 快速开始

#### 1. 配置

编辑 `config.json` 中的 `asset_sync` 部分:

```json
{
  "asset_sync": {
    "enabled": true,
    "feishu": {
      "app_token": "P0dsbK6vSa...",
      "tables": {
        "holdings": "tbl持仓表ID",
        "history": "tbl历史表ID",
        "logs": "tbl日志表ID"
      }
    },
    "binance": {
      "enabled": true,
      "api_key": "YOUR_API_KEY",
      "api_secret": "YOUR_SECRET"
    },
    "xueqiu": {
      "enabled": true,
      "cookies": "xq_a_token=..."
    },
    "scheduler": {
      "crypto_sync": {
        "enabled": true,
        "hour": "*",
        "minute": 0
      },
      "fund_sync": {
        "enabled": true,
        "hour": 9,
        "minute": 0
      },
      "snapshot": {
        "enabled": true,
        "hour": 0,
        "minute": 0
      }
    },
    "database": {
      "path": "data/assets.db",
      "backup": {
        "enabled": true,
        "keep_days": 30
      }
    },
    "alerts": {
      "enabled": true,
      "feishu_webhook": "https://open.feishu.cn/..."
    }
  }
}
```

#### 2. 初始化数据库

```bash
python setup_tables.py
```

#### 3. 运行

```bash
# 测试运行 (一次性)
./main.py --run-once

# 只运行单个任务
./main.py --task crypto
./main.py --task fund
./main.py --task snapshot

# 启动定时服务
./main.py
```

### 系统架构

```
数据源 (Binance/雪球)
    ↓
资产发现 (自动/手动)
    ↓
定时任务 (APScheduler)
    ├─ 加密货币同步 (每小时)
    ├─ 基金同步 (每天9:00)
    ├─ 每日快照 (每天0:00)
    └─ 数据库备份 (每天1:00)
    ↓
双存储
    ├─ SQLite (本地高性能)
    └─ 飞书 (云端可视化)
    ↓
监控告警 (飞书机器人)
```

### 定时任务

| 任务 | 时间 | 说明 |
|------|------|------|
| 加密货币同步 | 每小时整点 | 获取价格和持仓 |
| 基金同步 | 每天 9:00 | 只在交易日执行 |
| 每日快照 | 每天 0:00 | 记录总资产 |
| 数据库备份 | 每天 1:00 | 自动备份并清理 |

### 飞书表格结构

#### 持仓表 (holdings)

| 字段 | 类型 | 说明 |
|------|------|------|
| 资产代码 | 文本 | BTC, DOGE, SH510300 |
| 资产名称 | 文本 | 比特币, 狗狗币 |
| 资产类型 | 单选 | 加密货币, 基金 |
| 数量 | 数字 | 持仓数量 |
| 当前价格 | 数字 | 最新价格 |
| 当前市值 | 数字 | = 数量 × 当前价格 |
| 成本价 | 数字 | 平均成本 |
| 总成本 | 数字 | = 数量 × 成本价 |
| 收益 | 数字 | = 当前市值 - 总成本 |
| 收益率 | 数字 | = 收益 / 总成本 × 100% |
| 更新时间 | 日期 | 最后更新时间 |

#### 历史表 (history)

| 字段 | 类型 | 说明 |
|------|------|------|
| 快照日期 | 日期 | 快照时间 |
| 总市值 | 数字 | 所有资产总市值 |
| 总成本 | 数字 | 所有资产总成本 |
| 总收益 | 数字 | = 总市值 - 总成本 |
| 收益率 | 数字 | = 总收益 / 总成本 × 100% |
| 持仓数量 | 数字 | 资产种类数 |

#### 日志表 (logs)

| 字段 | 类型 | 说明 |
|------|------|------|
| 时间 | 日期 | 同步时间 |
| 数据源 | 文本 | binance, xueqiu, system |
| 任务类型 | 文本 | crypto_sync, fund_sync |
| 状态 | 单选 | success, partial, failed |
| 记录数 | 数字 | 成功数量 |
| 错误信息 | 文本 | 错误详情 |
| 耗时 | 数字 | 执行时间 (秒) |

### 数据库备份

```bash
# 手动备份
python -c "from src.utils.backup import create_backup; create_backup()"

# 查看备份
ls -lht data/backups/

# 从备份恢复
python -c "
from src.utils.backup import DatabaseBackup
db = DatabaseBackup('data/assets.db')
db.restore_backup('data/backups/assets_20250120_010000.db')
"
```

### 服务器部署 (systemd)

```bash
# 编辑服务文件中的User和WorkingDirectory
sudo cp feishu-asset-sync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable feishu-asset-sync
sudo systemctl start feishu-asset-sync

# 查看状态
sudo systemctl status feishu-asset-sync

# 查看日志
sudo journalctl -u feishu-asset-sync -f
```

---

## 项目结构

```
feishu/
├── import.py                   # 账单导入工具入口
├── main.py                     # 资产同步系统入口
├── setup_tables.py             # 数据库初始化
├── config.json                 # 统一配置文件
├── pyproject.toml              # uv 项目配置
│
├── lib/                        # 账单导入模块
│   ├── config.py              # 配置管理
│   ├── feishu_client.py       # 飞书API
│   ├── bill_parser.py         # 账单解析
│   ├── smart_categorizer.py   # 智能分类
│   └── logger.py              # 日志系统
│
├── src/                        # 资产同步模块
│   ├── core/                  # 核心模块
│   │   ├── config.py
│   │   ├── database.py
│   │   ├── feishu_client.py
│   │   └── logger.py
│   ├── datasources/           # 数据源
│   │   ├── binance_client.py
│   │   └── xueqiu_client.py
│   ├── schedulers/            # 定时任务
│   │   ├── crypto_sync.py
│   │   ├── fund_sync.py
│   │   └── snapshot.py
│   └── utils/                 # 工具模块
│       ├── asset_discovery.py
│       ├── alert.py
│       └── backup.py
│
├── data/                       # 数据目录
│   ├── assets.db              # SQLite 数据库
│   └── backups/               # 备份文件
│
└── logs/                       # 日志文件
```

---

## 配置文件说明

`config.json` 包含两个工具的配置:

### 账单导入配置

```json
{
  "accounts": {
    "账本名": {
      "app_token": "飞书App Token",
      "table_id": "飞书表格ID",
      "name": "账本显示名称",
      "data_dir": "账单文件目录",
      "last_import_timestamp": {
        "alipay": 1234567890000,
        "wechat": 1234567890000
      }
    }
  },
  "mcp_server": {
    "app_id": "飞书应用ID",
    "app_secret": "飞书应用密钥"
  },
  "import_settings": {
    "batch_size": 500,
    "delay_between_records": 0.0001,
    "delay_between_batches": 0.0001
  }
}
```

### 资产同步配置

```json
{
  "asset_sync": {
    "enabled": true,
    "feishu": {
      "app_token": "飞书App Token",
      "tables": {
        "holdings": "持仓表ID",
        "history": "历史表ID",
        "logs": "日志表ID"
      }
    },
    "binance": {
      "enabled": true,
      "api_key": "YOUR_API_KEY",
      "api_secret": "YOUR_SECRET"
    },
    "xueqiu": {
      "enabled": true,
      "cookies": "xq_a_token=..."
    },
    "scheduler": {
      "crypto_sync": {
        "enabled": true,
        "hour": "*",
        "minute": 0
      },
      "fund_sync": {
        "enabled": true,
        "hour": 9,
        "minute": 0
      },
      "snapshot": {
        "enabled": true,
        "hour": 0,
        "minute": 0
      }
    },
    "database": {
      "path": "data/assets.db",
      "backup": {
        "enabled": true,
        "keep_days": 30
      }
    },
    "alerts": {
      "enabled": true,
      "feishu_webhook": "https://...",
      "min_success_rate": 0.8
    }
  }
}
```

---

## 技术栈

- **Python 3.10+**
- **uv** - 项目和依赖管理
- **APScheduler** - 定时任务调度
- **ccxt** - Binance API
- **pysnowball** - 雪球 API
- **pandas** - 账单解析
- **loguru** - 日志系统
- **SQLite** - 本地数据库
- **Feishu API** - 飞书多维表格

---

## 常见问题

### 账单导入

**Q: 支持哪些账单格式?**
- 支付宝: CSV格式 (导出时选择"标准格式")
- 微信: XLSX格式 (支付-钱包-账单-下载账单)

**Q: 如何避免重复导入?**
系统会记录 `last_import_timestamp`,只导入时间戳大于此值的记录。

**Q: 如何自定义分类规则?**
编辑 `lib/smart_categorizer.py` 中的 `KEYWORD_RULES` 和 `EXACT_MERCHANT_MAP`。

### 资产同步

**Q: 为什么同步失败?**
检查:
- 网络连接
- API Key 是否正确
- Cookies 是否过期
- 查看日志: `tail -f logs/sync_*.log`

**Q: 如何修改同步频率?**
编辑 `config.json` 中的 `scheduler` 配置。

**Q: 备份占用太多空间?**
调整 `database.backup.keep_days` 参数。

---

## 许可证

MIT License

---

## 更新日志

### v1.0.0 (2025-01-20)

**账单导入工具**:
- 支付宝CSV和微信XLSX解析
- 智能分类 (12种分类规则)
- 增量导入机制
- 批量处理和去重

**资产同步系统**:
- Binance加密货币同步
- 雪球基金同步
- 自动资产发现
- SQLite + 飞书双存储
- 定时任务调度
- 每日快照
- 数据库自动备份
- 飞书机器人告警

---

**快速开始**:
- 账单导入: `python import.py`
- 资产同步: `./main.py --run-once`
