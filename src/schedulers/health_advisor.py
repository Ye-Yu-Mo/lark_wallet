"""
健康顾问定时任务
定时生成健康建议并发送到飞书和邮件
"""
import time
from typing import Dict
from datetime import datetime
from loguru import logger
from chinese_calendar import is_workday as is_cn_workday

from core.config import Config
from core.health_client import HealthFeishuClient
from core.deepseek_client import DeepseekClient
from utils.health_prompt import build_health_advice_prompt
from utils.alert import AlertManager


class HealthAdvisorTask:
    """
    健康顾问任务

    定时读取健康数据,调用AI生成建议,
    发送到飞书和邮件
    """

    def __init__(self, config: Config):
        """
        初始化任务

        :param config: 配置对象
        """
        self.config = config

        # 获取配置
        health_config = config.get_health_config()
        feishu_config = config.get_feishu_config()
        deepseek_config = config.get_deepseek_config()
        alert_config = config.get_asset_sync_config().get('alerts', {})

        # 初始化健康数据客户端
        self.health_client = HealthFeishuClient(
            app_id=feishu_config['app_id'],
            app_secret=feishu_config['app_secret'],
            app_token=health_config['app_token'],
            table_ids=health_config['tables']
        )

        # 初始化 Deepseek 客户端
        self.deepseek = DeepseekClient(
            api_key=deepseek_config['api_key'],
            base_url=deepseek_config.get('base_url', 'https://api.deepseek.com'),
            model=deepseek_config.get('model', 'deepseek-chat')
        )

        # 初始化告警管理器
        self.alert_manager = AlertManager(
            webhook_url=alert_config.get('feishu_webhook', ''),
            email_config=alert_config.get('email'),
            enabled=alert_config.get('enabled', False)
        )

        logger.info("HealthAdvisorTask 初始化完成")

    def generate_advice(self) -> Dict:
        """
        生成健康建议

        :return: 执行结果
        """
        start_time = time.time()

        result = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'advice': '',
            'errors': []
        }

        try:
            logger.info("开始生成健康建议...")

            # 1. 读取数据
            logger.info("读取健康数据...")
            profile = self.health_client.get_health_profile()
            health_records = self.health_client.get_recent_health_records(days=7)
            meals = self.health_client.get_recent_meals(days=5)
            exercises = self.health_client.get_recent_exercises(days=7)
            fridge_inventory = self.health_client.get_fridge_inventory()
            expiring_ingredients = self.health_client.get_expiring_ingredients(days=3)

            # 判断是否工作日
            is_workday = is_cn_workday(datetime.now().date())
            day_type = "工作日" if is_workday else "休息日"
            logger.info(f"今天是{day_type}")

            # 2. 构建 Prompt
            logger.info("构建 AI prompt...")
            prompt = build_health_advice_prompt(
                profile=profile,
                health_records=health_records,
                meals=meals,
                exercises=exercises,
                fridge_inventory=fridge_inventory,
                expiring_ingredients=expiring_ingredients,
                is_workday=is_workday
            )

            # 3. 调用 Deepseek API
            logger.info("调用 Deepseek API...")
            system_message = "你是一位顶级临床营养师。请直接输出 HTML 内容,不要包含 ```html 包裹。"
            advice = self.deepseek.chat(prompt, max_tokens=2000, temperature=0.7, system_message=system_message)

            if not advice:
                raise Exception("AI 未返回有效建议")

            # 清理可能的 markdown 标记 (以防万一)
            advice = advice.replace('```html', '').replace('```', '').strip()

            result['advice'] = advice
            logger.info("健康建议生成成功")

            # 4. 发送建议
            logger.info("发送健康建议...")
            self._send_advice(advice, is_workday)

            # 5. 记录执行时间
            duration = time.time() - start_time
            logger.info(f"健康建议任务完成, 耗时 {duration:.2f}s")

        except Exception as e:
            logger.error(f"生成健康建议失败: {e}")
            result['success'] = False
            result['errors'].append(str(e))

        return result

    def _send_advice(self, advice: str, is_workday: bool):
        """
        发送健康建议

        :param advice: 建议内容
        :param is_workday: 是否工作日
        """
        if not self.alert_manager.enabled:
            logger.warning("告警功能未启用,跳过发送")
            return

        try:
            today = datetime.now().strftime('%Y年%m月%d日')
            day_type = "工作日" if is_workday else "休息日"

            # 1. 发送飞书消息
            if self.alert_manager.webhook_url:
                self._send_feishu_message(advice, today, day_type)

            # 2. 发送邮件
            if self.alert_manager.email_sender and self.alert_manager.email_sender.enabled:
                self._send_email(advice, today, day_type)

        except Exception as e:
            logger.error(f"发送健康建议失败: {e}")

    def _send_feishu_message(self, advice: str, today: str, day_type: str):
        """
        发送飞书消息

        :param advice: 建议内容 (HTML格式)
        :param today: 今天日期
        :param day_type: 工作日/休息日
        """
        try:
            import requests

            # 将 HTML 转为 Markdown (Feishu卡片只支持Markdown)
            markdown_content = self._html_to_markdown(advice)

            card = {
                "msg_type": "interactive",
                "card": {
                    "config": {
                        "wide_screen_mode": True
                    },
                    "header": {
                        "title": {
                            "tag": "plain_text",
                            "content": f"健康建议 - {today} ({day_type})"
                        },
                        "template": "blue"
                    },
                    "elements": [
                        {
                            "tag": "markdown",
                            "content": markdown_content
                        },
                        {
                            "tag": "hr"
                        },
                        {
                            "tag": "note",
                            "elements": [
                                {
                                    "tag": "plain_text",
                                    "content": f"生成时间: {datetime.now().strftime('%H:%M')}"
                                }
                            ]
                        }
                    ]
                }
            }

            response = requests.post(
                self.alert_manager.webhook_url,
                json=card,
                timeout=10
            )

            if response.status_code == 200:
                logger.info("健康建议已发送到飞书")
            else:
                logger.error(f"发送飞书消息失败: {response.text}")

        except Exception as e:
            logger.error(f"发送飞书消息异常: {e}")

    def _send_email(self, advice: str, today: str, day_type: str):
        """
        发送邮件

        :param advice: 建议内容 (HTML格式)
        :param today: 今天日期
        :param day_type: 工作日/休息日
        """
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart

            email_sender = self.alert_manager.email_sender
            if not email_sender:
                logger.warning("邮件发送器未初始化")
                return

            # 构建邮件
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"健康建议 - {today} ({day_type})"
            msg['From'] = email_sender.username
            msg['To'] = ', '.join(email_sender.recipients)

            # 包装 HTML 内容 (添加基础样式)
            html_content = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; color: #333; max-width: 800px; margin: 0 auto; padding: 20px; }}
                    h3 {{ color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 10px; }}
                    ul {{ line-height: 1.8; }}
                    p {{ line-height: 1.6; }}
                </style>
            </head>
            <body>
                <h1>健康建议 - {today} ({day_type})</h1>
                {advice}
                <hr>
                <p style="text-align: center; color: #999; font-size: 12px;">
                    生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 由 DeepSeek AI 生成
                </p>
            </body>
            </html>
            """

            # 添加 HTML 内容
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg.attach(html_part)

            # 发送邮件
            with smtplib.SMTP_SSL(
                email_sender.smtp_server,
                email_sender.smtp_port
            ) as server:
                server.login(email_sender.username, email_sender.password)
                server.send_message(msg)

            logger.info(f"健康建议已发送到邮箱: {', '.join(email_sender.recipients)}")

        except Exception as e:
            logger.error(f"发送邮件异常: {e}")

    def _html_to_markdown(self, html_text: str) -> str:
        """
        简单的 HTML 转 Markdown (用于飞书卡片显示)

        :param html_text: HTML 文本
        :return: Markdown 文本
        """
        import re

        markdown = html_text

        # 移除可能的完整 HTML 包装
        markdown = re.sub(r'<html[^>]*>.*?<body[^>]*>', '', markdown, flags=re.DOTALL)
        markdown = re.sub(r'</body>.*?</html>', '', markdown, flags=re.DOTALL)
        markdown = re.sub(r'<head>.*?</head>', '', markdown, flags=re.DOTALL)

        # 转换标题
        markdown = re.sub(r'<h1[^>]*>(.*?)</h1>', r'# \1', markdown)
        markdown = re.sub(r'<h2[^>]*>(.*?)</h2>', r'## \1', markdown)
        markdown = re.sub(r'<h3[^>]*>(.*?)</h3>', r'### \1', markdown)

        # 转换加粗
        markdown = re.sub(r'<b[^>]*>(.*?)</b>', r'**\1**', markdown)
        markdown = re.sub(r'<strong[^>]*>(.*?)</strong>', r'**\1**', markdown)

        # 转换列表项
        markdown = re.sub(r'<li[^>]*>(.*?)</li>', r'- \1', markdown)

        # 移除列表标签
        markdown = re.sub(r'</?ul[^>]*>', '', markdown)
        markdown = re.sub(r'</?ol[^>]*>', '', markdown)

        # 转换段落
        markdown = re.sub(r'<p[^>]*>(.*?)</p>', r'\1\n', markdown)

        # 移除其他 HTML 标签
        markdown = re.sub(r'<br\s*/?>', '\n', markdown)
        markdown = re.sub(r'<hr\s*/?>', '\n---\n', markdown)
        markdown = re.sub(r'<[^>]+>', '', markdown)

        # 清理多余空行
        markdown = re.sub(r'\n{3,}', '\n\n', markdown)

        return markdown.strip()


def generate_health_advice(config_path: str = 'config.json') -> Dict:
    """
    生成健康建议 (便捷函数)

    :param config_path: 配置文件路径
    :return: 执行结果
    """
    config = Config(config_path)
    task = HealthAdvisorTask(config)
    return task.generate_advice()


if __name__ == '__main__':
    # 直接运行测试
    import sys
    from pathlib import Path

    # 添加项目根目录到路径
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

    # 设置日志
    from core.logger import setup_logger
    setup_logger(level='DEBUG')

    # 生成建议
    result = generate_health_advice()
    print(f"\n执行结果: {result}")
