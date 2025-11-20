"""
MCP客户端模块
"""
import json
import subprocess
import sys


class MCPClient:
    """MCP客户端,通过stdio与MCP服务器通信"""

    def __init__(self, command):
        """
        初始化MCP客户端
        :param command: MCP服务器启动命令列表
        """
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        self.request_id = 0

    def send_request(self, method, params=None):
        """发送JSON-RPC 2.0请求"""
        self.request_id += 1
        request = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": method,
            "params": params or {}
        }

        # 发送请求
        request_json = json.dumps(request)
        self.process.stdin.write(request_json + "\n")
        self.process.stdin.flush()

        # 接收响应
        response_line = self.process.stdout.readline()

        if not response_line:
            raise Exception("MCP服务器无响应")

        response = json.loads(response_line)

        if "error" in response:
            raise Exception(f"MCP错误: {response['error']}")

        return response.get("result")

    def initialize(self):
        """初始化MCP连接"""
        return self.send_request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "feishu-batch-import",
                "version": "1.0.0"
            }
        })

    def call_tool(self, tool_name, arguments):
        """调用MCP工具"""
        return self.send_request("tools/call", {
            "name": tool_name,
            "arguments": arguments
        })

    def close(self):
        """关闭连接"""
        if self.process.stdin:
            self.process.stdin.close()
        self.process.wait()
