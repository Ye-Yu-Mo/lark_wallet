"""
邮件发送模块
支持 SMTP 发送文本和 HTML 邮件
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from typing import List, Optional, Union
from loguru import logger


class EmailSender:
    """邮件发送器"""

    def __init__(self, config: dict):
        """
        初始化邮件发送器

        :param config: 邮件配置字典
            {
                "enabled": True,
                "smtp_server": "smtp.qq.com",
                "smtp_port": 465,
                "username": "xxx@qq.com",
                "password": "xxx",  # 授权码
                "recipients": ["xxx@qq.com"]
            }
        """
        self.config = config
        self.enabled = config.get('enabled', False)
        self.smtp_server = config.get('smtp_server')
        self.smtp_port = config.get('smtp_port', 465)
        self.username = config.get('username')
        self.password = config.get('password')
        self.recipients = config.get('recipients', [])

        if self.enabled and not (self.smtp_server and self.username and self.password and self.recipients):
            logger.warning("邮件功能已启用，但配置不完整，将无法发送")
            self.enabled = False

    def send(self, subject: str, content: str, content_type: str = 'plain') -> bool:
        """
        发送邮件

        :param subject: 邮件主题
        :param content: 邮件内容
        :param content_type: 内容类型 ('plain' 或 'html')
        :return: 是否成功
        """
        if not self.enabled:
            return False

        try:
            message = MIMEMultipart()
            message['From'] = self.username
            message['To'] = ','.join(self.recipients)
            message['Subject'] = Header(subject, 'utf-8')

            # 邮件正文
            msg_content = MIMEText(content, content_type, 'utf-8')
            message.attach(msg_content)

            # 连接 SMTP 服务器
            # 根据端口判断是否使用 SSL
            if self.smtp_port == 465:
                server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
            else:
                server = smtplib.SMTP(self.smtp_server, self.smtp_port)
                if self.smtp_port == 587:
                    server.starttls()

            server.login(self.username, self.password)
            server.sendmail(self.username, self.recipients, message.as_string())
            server.quit()

            logger.info(f"邮件发送成功: {subject}")
            return True

        except Exception as e:
            logger.error(f"邮件发送失败: {e}")
            return False
