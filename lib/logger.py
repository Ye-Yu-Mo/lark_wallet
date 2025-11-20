"""
日志模块
"""
import logging
import os
from datetime import datetime


def setup_logger(name='feishu_import', log_file=None):
    """
    设置日志器
    :param name: 日志器名称
    :param log_file: 日志文件路径,默认为logs/import_YYYYMMDD.log
    :return: logger对象
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # 避免重复添加handler
    if logger.handlers:
        return logger

    # 创建logs目录
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 默认日志文件名
    if log_file is None:
        log_file = os.path.join(log_dir, f"import_{datetime.now().strftime('%Y%m%d')}.log")

    # 文件handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)

    # 格式化
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger
