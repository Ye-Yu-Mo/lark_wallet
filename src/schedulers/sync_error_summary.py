"""
同步错误汇总任务
每日汇总所有同步任务的错误和失败情况
"""
import time
from typing import Dict, List
from datetime import datetime, timedelta
from loguru import logger

from core.config import Config
from core.feishu_client import AssetFeishuClient
from utils.alert import AlertManager


class SyncErrorSummaryTask:
    """
    同步错误汇总任务

    从日志表读取当天的同步记录,
    汇总错误和失败情况并发送通知
    """

    def __init__(self, config: Config):
        """初始化汇总任务"""
        self.config = config

        # 获取配置
        asset_sync = config.get_asset_sync_config()
        feishu_config = config.get_feishu_config()
        alert_config = asset_sync.get('alerts', {})

        # 初始化飞书客户端
        self.feishu = AssetFeishuClient(
            app_id=feishu_config['app_id'],
            app_secret=feishu_config['app_secret'],
            app_token=feishu_config['app_token'],
            table_ids=feishu_config['tables']
        )

        # 初始化告警管理器
        self.alert_manager = AlertManager(
            webhook_url=alert_config.get('feishu_webhook', ''),
            email_config=alert_config.get('email'),
            enabled=alert_config.get('enabled', False)
        )

        logger.info("SyncErrorSummaryTask 初始化完成")

    def generate_error_summary(self) -> Dict:
        """
        生成同步错误汇总

        :return: 汇总结果
        """
        start_time = time.time()

        result = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'date': datetime.now().date().isoformat(),
            'total_syncs': 0,
            'failed_syncs': 0,
            'partial_syncs': 0,
            'success_rate': 100.0,
            'errors': [],
            'error_details': []
        }

        try:
            # 1. 获取今天的同步日志
            logger.info("开始生成同步错误汇总...")

            # 计算今天的时间范围 (0点到现在)
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            start_timestamp = int(today_start.timestamp() * 1000)
            end_timestamp = int(time.time() * 1000)

            # 从飞书日志表获取记录
            logs = self._get_sync_logs(start_timestamp, end_timestamp)

            if not logs:
                logger.info("今天没有同步记录")
                return result

            result['total_syncs'] = len(logs)

            # 2. 统计失败和错误
            for log in logs:
                fields = log.get('fields', {})

                try:
                    # 任务类型
                    task_type = fields.get('任务类型', 'unknown')

                    # 状态
                    status = fields.get('状态', 'unknown')

                    # 错误信息
                    error_msg = fields.get('错误信息', '')

                    # 同步时间
                    sync_time_field = fields.get('同步时间')
                    if isinstance(sync_time_field, int):
                        sync_time = datetime.fromtimestamp(sync_time_field / 1000)
                    else:
                        sync_time = datetime.now()

                    # 记录数
                    record_count = fields.get('记录数', 0)

                    if status == 'failed':
                        result['failed_syncs'] += 1
                        result['error_details'].append({
                            'task_type': task_type,
                            'status': status,
                            'error_msg': error_msg,
                            'sync_time': sync_time.strftime('%H:%M:%S'),
                            'record_count': record_count
                        })
                    elif status == 'partial_success':
                        result['partial_syncs'] += 1
                        result['error_details'].append({
                            'task_type': task_type,
                            'status': status,
                            'error_msg': error_msg,
                            'sync_time': sync_time.strftime('%H:%M:%S'),
                            'record_count': record_count
                        })

                except Exception as e:
                    logger.error(f"处理日志记录失败: {e}")
                    continue

            # 3. 计算成功率
            if result['total_syncs'] > 0:
                success_count = result['total_syncs'] - result['failed_syncs']
                result['success_rate'] = (success_count / result['total_syncs']) * 100

            # 4. 发送汇总通知 (只在有错误时发送)
            if result['failed_syncs'] > 0 or result['partial_syncs'] > 0:
                logger.info(f"发现 {result['failed_syncs']} 个失败, {result['partial_syncs']} 个部分成功")
                self._send_error_summary(result)
            else:
                logger.info("今天所有同步任务都成功,无需发送汇总")

            # 5. 记录日志
            duration = time.time() - start_time

            try:
                self.feishu.log_sync_status(
                    source='system',
                    task_type='error_summary',
                    status='success',
                    record_count=result['failed_syncs'] + result['partial_syncs'],
                    error_msg=None,
                    duration=duration
                )
            except Exception as e:
                logger.error(f"记录汇总日志失败: {e}")

        except Exception as e:
            logger.error(f"生成错误汇总失败: {e}")
            result['success'] = False
            result['errors'].append(str(e))

        return result

    def _get_sync_logs(self, start_time: int, end_time: int) -> List[Dict]:
        """
        从飞书日志表获取同步记录

        :param start_time: 开始时间戳 (毫秒)
        :param end_time: 结束时间戳 (毫秒)
        :return: 日志记录列表
        """
        try:
            # 使用飞书客户端的底层方法获取日志表记录
            app_token = self.feishu.app_token
            table_id = self.feishu.logs_table_id

            if not table_id:
                logger.error("未配置日志表ID")
                return []

            # 构建查询过滤条件
            filter_condition = {
                "conjunction": "and",
                "conditions": [
                    {
                        "field_name": "同步时间",
                        "operator": "isGreaterEqual",
                        "value": [str(start_time)]
                    },
                    {
                        "field_name": "同步时间",
                        "operator": "isLessEqual",
                        "value": [str(end_time)]
                    }
                ]
            }

            # 调用search接口
            import requests

            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"

            headers = {
                "Authorization": f"Bearer {self.feishu.get_tenant_access_token()}",
                "Content-Type": "application/json"
            }

            body = {
                "filter": filter_condition,
                "automatic_fields": True
            }

            response = requests.post(url, headers=headers, json=body, timeout=10)

            if response.status_code == 200:
                data = response.json()
                return data.get('data', {}).get('items', [])
            else:
                logger.error(f"获取日志记录失败: {response.text}")
                return []

        except Exception as e:
            logger.error(f"获取同步日志失败: {e}")
            return []

    def _send_error_summary(self, summary: Dict):
        """
        发送错误汇总通知到飞书

        :param summary: 汇总数据
        """
        if not self.alert_manager.enabled:
            logger.warning("告警功能未启用,跳过发送通知")
            return

        try:
            # 构建错误列表
            error_lines = []
            for detail in summary['error_details'][:10]:  # 最多显示10个
                status_emoji = "❌" if detail['status'] == 'failed' else "⚠️"
                task_name = {
                    'crypto_sync': '加密货币同步',
                    'fund_sync': '基金同步',
                    'snapshot': '每日快照',
                    'distribution_sync': '资产分布同步',
                    'daily_report': '每日报告',
                    'price_alert': '价格告警',
                    'weekly_report': '周报',
                    'monthly_report': '月报',
                    'milestone_alert': '里程碑检查',
                    'holding_period_reminder': '持仓周期提醒'
                }.get(detail['task_type'], detail['task_type'])

                error_msg = detail['error_msg'][:50] if detail['error_msg'] else '无错误信息'

                error_lines.append(
                    f"{status_emoji} **{task_name}** ({detail['sync_time']})\n"
                    f"错误: {error_msg}"
                )

            # 确定模板颜色
            if summary['success_rate'] < 50:
                template = "red"
            elif summary['success_rate'] < 80:
                template = "orange"
            else:
                template = "yellow"

            # 构建卡片消息
            card = {
                "msg_type": "interactive",
                "card": {
                    "config": {
                        "wide_screen_mode": True
                    },
                    "header": {
                        "title": {
                            "tag": "plain_text",
                            "content": f"⚠️ 同步错误汇总 - {summary['date']}"
                        },
                        "template": template
                    },
                    "elements": [
                        {
                            "tag": "div",
                            "fields": [
                                {
                                    "is_short": True,
                                    "text": {
                                        "tag": "lark_md",
                                        "content": f"**总同步次数**\n{summary['total_syncs']} 次"
                                    }
                                },
                                {
                                    "is_short": True,
                                    "text": {
                                        "tag": "lark_md",
                                        "content": f"**成功率**\n{summary['success_rate']:.1f}%"
                                    }
                                }
                            ]
                        },
                        {
                            "tag": "div",
                            "fields": [
                                {
                                    "is_short": True,
                                    "text": {
                                        "tag": "lark_md",
                                        "content": f"**失败次数**\n{summary['failed_syncs']} 次"
                                    }
                                },
                                {
                                    "is_short": True,
                                    "text": {
                                        "tag": "lark_md",
                                        "content": f"**部分成功**\n{summary['partial_syncs']} 次"
                                    }
                                }
                            ]
                        },
                        {
                            "tag": "hr"
                        },
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": "**错误详情**:\n\n" + "\n\n".join(error_lines)
                            }
                        }
                    ]
                }
            }

            # 发送消息
            import requests
            response = requests.post(
                self.alert_manager.webhook_url,
                json=card,
                timeout=10
            )

            if response.status_code == 200:
                logger.info("同步错误汇总已发送到飞书")
            else:
                logger.error(f"发送错误汇总失败: {response.text}")

        except Exception as e:
            logger.error(f"发送错误汇总失败: {e}")


def generate_error_summary(config_path: str = 'config.json') -> Dict:
    """
    生成同步错误汇总 (便捷函数)

    :param config_path: 配置文件路径
    :return: 汇总结果
    """
    config = Config(config_path)
    task = SyncErrorSummaryTask(config)
    return task.generate_error_summary()


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

    # 生成错误汇总
    result = generate_error_summary()
    print(f"\n汇总结果: {result}")
