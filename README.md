# 飞书记账批量导入工具

通过MCP协议自动将支付宝/微信账单导入到飞书多维表,支持增量导入。

## 特性

- 自动扫描data目录下的账单文件(支持递归扫描)
- 支持支付宝原始账单CSV (GBK编码)
- 支持微信原始账单XLSX
- **智能分类**: 基于关键词和商户名精确匹配,自动生成分类和备注
- **批量导入**: 直接调用飞书API,500条/批,大幅提升性能
- **增量导入**: 按来源(支付宝/微信)记录上次导入时间,下次只导入新记录
- **去重机制**: 基于(时间戳+金额+分类)自动去重
- **错误处理**: 批量失败自动降级为单条创建,确保部分成功
- **API重试**: 指数退避重试机制,应对网络波动和限流
- **日志追踪**: 完整的操作日志,方便排查问题
- 配置化管理,支持多账本
- 进度显示(tqdm)和错误处理

## 快速开始

### 1. 配置账本信息

编辑 `config.json`:

```json
{
  "accounts": {
    "jasxu": {
      "app_token": "xxx",
      "table_id": "xxx",
      "name": "jasxu的账本",
      "data_dir": "历史账单数据",
      "last_import_timestamp": {
        "alipay": 0,
        "wechat": 0
      }
    }
  },
  "mcp_server": {
    "command": ["npx", "-y", "@larksuiteoapi/lark-mcp", "mcp"],
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

**配置说明**:
- `data_dir`: 账单文件所在目录
- `last_import_timestamp`: 按来源记录最后导入时间戳,脚本会自动更新

### 2. 准备账单文件

将账单文件放到配置的data_dir目录:
- 支付宝账单: `*.csv` (原始GBK编码格式)
- 微信账单: `*.xlsx` (原始Excel格式)

脚本会自动扫描目录下的所有文件。

### 3. 运行导入

```bash
# 导入所有账本
uv run import.py

# 只导入jasxu的账本
uv run import.py --account jasxu

# 只导入笨蛋蛋的账本
uv run import.py --account bendandan
```

### 4. 增量导入

脚本会按来源(支付宝/微信)记录最后导入时间戳,下次运行时:
- 只导入时间戳大于上次记录的新记录
- 跳过已导入的历史记录
- 导入成功后自动更新时间戳

## 账单文件格式

### 支付宝账单

原始CSV文件格式 (GBK编码,跳过前24行):
- 交易时间
- 交易分类
- 交易对方
- 收/支
- 金额
- 交易状态

### 微信账单

原始XLSX文件格式 (跳过前16行):
- 交易时间
- 交易类型
- 交易对方
- 收/支
- 金额(元)
- 当前状态

## 获取账单文件

### 支付宝
1. 打开支付宝APP
2. 我的 -> 账单
3. 右上角 "…" -> 开具交易流水证明
4. 下载CSV文件并放到data_dir目录

### 微信
1. 打开微信
2. 我 -> 服务 -> 钱包 -> 账单
3. 常见问题 -> 下载账单
4. 下载XLSX文件并放到data_dir目录

## 配置文件说明

```json
{
  "accounts": {
    "账本key": {
      "app_token": "飞书多维表app_token",
      "table_id": "流水账表table_id",
      "name": "账本显示名称",
      "data_dir": "账单文件目录",
      "last_import_timestamp": {
        "alipay": 0,
        "wechat": 0
      }
    }
  },
  "mcp_server": {
    "command": ["npx", "-y", "@larksuiteoapi/lark-mcp", "mcp"],
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

## 项目结构

```
.
├── config.json              # 配置文件
├── import.py               # 主程序
├── lib/
│   ├── config.py           # 配置管理
│   ├── feishu_client.py    # 飞书API客户端(批量导入)
│   ├── bill_parser.py      # 账单解析器(支持原始格式+去重)
│   ├── smart_categorizer.py # 智能分类器
│   ├── logger.py           # 日志系统
│   └── mcp_client.py       # MCP客户端(已弃用)
├── logs/                   # 日志目录
│   └── import_YYYYMMDD.log # 每日日志文件
└── README.md
```

## 常见问题

### 如何添加新的账单文件?

直接将新的账单文件放到data_dir目录,然后重新运行导入脚本。脚本会自动扫描并导入新记录。

### 重复导入怎么办?

脚本按来源(支付宝/微信)使用时间戳进行增量导入,只要 `last_import_timestamp` 正确维护,就不会重复导入。

### 如何重新导入所有数据?

将对应来源的 `last_import_timestamp` 设置为 `0` 即可。

### 导入速度如何调整?

修改 `config.json` 中的 `import_settings`:
- `batch_size`: 批次大小
- `delay_between_records`: 记录间延迟(秒)
- `delay_between_batches`: 批次间延迟(秒)

### 支持哪些账单格式?

- 支付宝: 原始CSV格式 (GBK编码,前24行为元数据)
- 微信: 原始XLSX格式 (前16行为元数据)

脚本会自动处理原始格式并转换为飞书表格式。
