import sys
import os
from core.config import Config
from utils.alert import AlertManager

def test_feishu_alert():
    try:
        config = Config('config.json')
        asset_sync = config.get_asset_sync_config()
        alert_config = asset_sync.get('alerts', {})
        
        webhook_url = alert_config.get('feishu_webhook')
        if not webhook_url:
            print("Error: No webhook_url found in config.json")
            return

        print(f"Testing Feishu Webhook: {webhook_url[:10]}...")
        
        alert_manager = AlertManager(webhook_url=webhook_url, enabled=True)
        
        success = alert_manager.send_alert(
            title="测试消息",
            content="这是一条来自 Gemini CLI 的测试消息。\n如果您看到这条消息，说明飞书告警功能正常。",
            level="info"
        )
        
        if success:
            print("✅ Test message sent successfully!")
        else:
            print("❌ Failed to send test message.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Add project root to sys.path
    sys.path.insert(0, os.getcwd())
    test_feishu_alert()
