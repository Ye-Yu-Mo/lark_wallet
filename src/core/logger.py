"""
日志系统模块
基于 loguru 实现日志管理,支持文件轮转、多级别输出
"""
import sys
from datetime import datetime
from pathlib import Path
from loguru import logger


def setup_logger(log_path='logs/', level='INFO', max_bytes=10485760, backup_count=5):
    """
    配置日志系统

    :param log_path: 日志文件路径
    :param level: 日志级别 (DEBUG/INFO/WARNING/ERROR)
    :param max_bytes: 单个日志文件最大字节数 (默认10MB)
    :param backup_count: 保留的备份文件数量
    :return: logger 实例
    """
    # 移除默认的 handler
    logger.remove()

    # 确保日志目录存在
    log_dir = Path(log_path)
    log_dir.mkdir(parents=True, exist_ok=True)

    # 生成日志文件名 (按日期)
    log_file = log_dir / f"sync_{datetime.now().strftime('%Y%m%d')}.log"

    # 日志格式
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )

    # 控制台输出 (带颜色)
    logger.add(
        sys.stdout,
        format=log_format,
        level=level,
        colorize=True,
        enqueue=True  # 线程安全
    )

    # 文件输出 (无颜色,支持轮转)
    logger.add(
        log_file,
        format=(
            "{time:YYYY-MM-DD HH:mm:ss} | "
            "{level: <8} | "
            "{name}:{function}:{line} - "
            "{message}"
        ),
        level=level,
        rotation=max_bytes,      # 文件大小达到 max_bytes 时轮转
        retention=backup_count,  # 保留最近 backup_count 个文件
        compression="zip",       # 压缩旧日志
        encoding="utf-8",
        enqueue=True
    )

    logger.info(f"日志系统初始化完成: {log_file}")
    logger.info(f"日志级别: {level}, 文件轮转: {max_bytes} bytes, 保留备份: {backup_count} 个")

    return logger


def get_logger():
    """
    获取 logger 实例
    适用于已经初始化过的场景
    """
    return logger
