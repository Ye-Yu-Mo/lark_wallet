import sys
import os
import time
from core.config import Config
from utils.email_sender import EmailSender
from loguru import logger

# 配置日志输出到控制台
logger.remove()
logger.add(sys.stderr, level="DEBUG")

def test_email_sender():
    try:
        print("正在加载配置...")
        config = Config('config.json')
        asset_sync = config.get_asset_sync_config()
        alert_config = asset_sync.get('alerts', {})
        email_config = alert_config.get('email')
        
        if not email_config:
            print("❌ 错误: config.json 中未找到 'alerts.email' 配置")
            return

        print(f"邮件配置: Enabled={email_config.get('enabled')}, User={email_config.get('username')}")
        
        if not email_config.get('enabled'):
            print("⚠️ 警告: 邮件功能在配置中被禁用 (enabled=False)")
            user_input = input("是否临时启用以进行测试? (y/n): ")
            if user_input.lower() == 'y':
                email_config['enabled'] = True
            else:
                return

        sender = EmailSender(email_config)
        
        # 1. 测试发送纯文本邮件
        print("\n正在发送纯文本测试邮件...")
        success = sender.send(
            subject="[Gemini CLI] 纯文本测试",
            content="这是一封来自 Gemini CLI 资产同步系统的测试邮件。\n如果您收到此邮件，说明 SMTP 配置正确。",
            content_type='plain'
        )
        
        if success:
            print("✅ 纯文本邮件发送成功!")
        else:
            print("❌ 纯文本邮件发送失败，请检查日志ảng。")

        # 2. 测试发送 HTML 报表邮件
        print("\n正在发送 HTML 报表测试邮件...")
        html_content = """
        <html>
        <head>
            <style>
                table { border-collapse: collapse; width: 100%; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
                .profit { color: red; }
                .loss { color: green; }
            </style>
        </head>
        <body>
            <h2>资产同步系统 - 测试报表</h2>
            <p>这是一个模拟的 HTML 资产报表。</p>
            
            <h3>资产明细</h3>
            <table>
                <tr>
                    <th>资产名称</th>
                    <th>当前价格</th>
                    <th>持仓数量</th>
                    <th>收益</th>
                </tr>
                <tr>
                    <td>比特币 (BTC)</td>
                    <td>$95,000.00</td>
                    <td>0.1</td>
                    <td class="profit">+¥12,000.00</td>
                </tr>
                <tr>
                    <td>某某基金</td>
                    <td>¥1.2345</td>
                    <td>10000</td>
                    <td class="loss">-¥500.00</td>
                </tr>
            </table>
            <p>测试时间: " + time.strftime("%Y-%m-%d %H:%M:%S") + "</p>
        </body>
        </html>
        """
        
        success = sender.send(
            subject="[Gemini CLI] HTML 报表测试",
            content=html_content,
            content_type='html'
        )
        
        if success:
            print("✅ HTML 邮件发送成功!")
        else:
            print("❌ HTML 邮件发送失败ảng。")

    except Exception as e:
        print(f"\n❌ 发生异常: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # 添加项目根目录到路径
    sys.path.insert(0, os.getcwd())
    test_email_sender()
