"""
Deepseek API 客户端
调用 Deepseek API 生成健康建议
"""
from typing import Dict, Optional
from loguru import logger
import requests


class DeepseekClient:
    """
    Deepseek API 客户端

    调用 Deepseek API 生成健康和饮食建议
    """

    def __init__(self, api_key: str, base_url: str = "https://api.deepseek.com", model: str = "deepseek-chat"):
        """
        初始化客户端

        :param api_key: API密钥
        :param base_url: API基础URL
        :param model: 模型名称
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.model = model

    def chat(self, prompt: str, max_tokens: int = 2000, temperature: float = 0.7, system_message: str = None) -> Optional[str]:
        """
        调用 Deepseek Chat API

        :param prompt: 输入提示词
        :param max_tokens: 最大token数
        :param temperature: 温度参数
        :param system_message: 系统消息(可选)
        :return: AI生成的回复
        """
        try:
            url = f"{self.base_url}/v1/chat/completions"

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }

            messages = []
            if system_message:
                messages.append({
                    "role": "system",
                    "content": system_message
                })
            messages.append({
                "role": "user",
                "content": prompt
            })

            payload = {
                "model": self.model,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "stream": False
            }

            logger.info(f"调用 Deepseek API: {self.model}")
            response = requests.post(url, json=payload, headers=headers, timeout=60)

            if response.status_code == 200:
                result = response.json()
                content = result.get('choices', [{}])[0].get('message', {}).get('content', '')

                # 记录使用的token数
                usage = result.get('usage', {})
                logger.info(f"Deepseek API 调用成功, tokens: {usage.get('total_tokens', 0)}")

                return content.strip()
            else:
                logger.error(f"Deepseek API 调用失败: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            logger.error(f"调用 Deepseek API 异常: {e}")
            return None
