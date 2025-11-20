#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书记账批量导入工具
自动扫描data目录下的账单文件并导入
"""
import sys
import time
import argparse
import os
import glob
from tqdm import tqdm
from lib.config import Config
from lib.feishu_client import FeishuClient
from lib.bill_parser import BillParser
from lib.logger import setup_logger


def import_account_bills(config, account_name, feishu_client, logger):
    """
    导入指定账本的所有账单文件
    :param config: 配置对象
    :param account_name: 账本名称
    :param feishu_client: 飞书客户端
    :param logger: 日志器
    """
    account = config.get_account(account_name)
    settings = config.get_import_settings()
    data_dir = account.get('data_dir', '')

    if not os.path.exists(data_dir):
        print(f"数据目录不存在: {data_dir}")
        logger.error(f"数据目录不存在: {data_dir}")
        return

    print(f"\n{'='*60}")
    print(f"开始导入账本: {account['name']}")
    print(f"{'='*60}")
    logger.info(f"开始导入账本: {account['name']} (data_dir: {data_dir})")

    # 递归扫描文件
    csv_files = glob.glob(os.path.join(data_dir, '**', '*.csv'), recursive=True)
    xlsx_files = glob.glob(os.path.join(data_dir, '**', '*.xlsx'), recursive=True)

    # 按来源类型分组
    sources = []
    if csv_files:
        sources.append(('alipay', csv_files))
    if xlsx_files:
        sources.append(('wechat', xlsx_files))

    if not sources:
        print(f"在 {data_dir} 中没有找到账单文件 (*.csv 或 *.xlsx)")
        logger.warning(f"在 {data_dir} 中没有找到账单文件")
        return

    logger.info(f"找到账单文件: {len(csv_files)} 个CSV, {len(xlsx_files)} 个XLSX")

    total_success = 0
    total_failed = 0

    for source_type, files in sources:
        source_name = '支付宝' if source_type == 'alipay' else '微信'
        last_timestamp = config.get_last_import_timestamp(account_name, source_type)

        print(f"\n处理 {source_name} 账单 (上次导入: {last_timestamp})")
        print(f"找到 {len(files)} 个文件")
        logger.info(f"开始处理 {source_name} 账单: {len(files)} 个文件, 上次导入时间戳: {last_timestamp}")

        # 合并所有文件的记录
        all_records = []
        for file_path in files:
            try:
                print(f"  解析: {os.path.basename(file_path)}")
                records = BillParser.parse(file_path, source_type)
                all_records.extend(records)
                logger.info(f"成功解析文件 {os.path.basename(file_path)}: {len(records)} 条记录")
            except Exception as e:
                print(f"  解析失败: {e}")
                logger.error(f"解析文件失败 {os.path.basename(file_path)}: {e}")
                continue

        print(f"  共解析 {len(all_records)} 条记录")
        logger.info(f"{source_name} 总共解析 {len(all_records)} 条记录")

        # 过滤新记录
        new_records = [r for r in all_records if r['日期'] > last_timestamp]

        if not new_records:
            print(f"  没有新记录需要导入")
            logger.info(f"{source_name} 没有新记录需要导入")
            continue

        print(f"  发现 {len(new_records)} 条新记录")
        logger.info(f"{source_name} 发现 {len(new_records)} 条新记录")

        # 按时间排序
        new_records.sort(key=lambda x: x['日期'])

        # 批量导入
        success_count = 0
        fail_count = 0
        max_timestamp = last_timestamp

        batch_size = settings['batch_size']  # 每批最多500条

        # 使用tqdm进度条
        with tqdm(total=len(new_records), desc=f"  导入{source_name}",
                  unit="条", ncols=80,
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:

            # 分批处理
            for i in range(0, len(new_records), batch_size):
                batch = new_records[i:i+batch_size]
                batch_records = [{"fields": r} for r in batch]

                try:
                    # 批量创建记录
                    result = feishu_client.batch_create_records(
                        account['app_token'],
                        account['table_id'],
                        batch_records
                    )

                    batch_success = result.get('success', 0)
                    batch_failed = result.get('failed', 0)

                    success_count += batch_success
                    fail_count += batch_failed

                    logger.info(f"批次 {i//batch_size + 1} 完成: 成功 {batch_success}, 失败 {batch_failed}")

                    # 更新最大时间戳(只统计成功的记录)
                    if batch_success > 0:
                        for r in batch[:batch_success]:
                            max_timestamp = max(max_timestamp, r['日期'])

                    # 更新进度条
                    pbar.update(len(batch))
                    pbar.set_postfix({"成功": success_count, "失败": fail_count})

                    # 如果有错误,打印详情
                    if batch_failed > 0 and result.get('errors'):
                        for error in result['errors'][:3]:  # 只打印前3个错误
                            tqdm.write(f"  错误: {error}")
                            logger.error(f"批次错误: {error}")

                except Exception as e:
                    fail_count += len(batch)
                    tqdm.write(f"  批次 {i//batch_size + 1} 导入失败: {e}")
                    logger.error(f"批次 {i//batch_size + 1} 导入失败: {e}")
                    pbar.update(len(batch))
                    pbar.set_postfix({"成功": success_count, "失败": fail_count})

        print()
        print(f"  完成: 成功 {success_count} | 失败 {fail_count}")
        logger.info(f"{source_name} 导入完成: 成功 {success_count}, 失败 {fail_count}")

        # 更新最后导入时间戳
        if success_count > 0:
            config.update_last_import_timestamp(account_name, source_type, max_timestamp)
            print(f"  更新最后导入时间戳: {max_timestamp}")
            logger.info(f"{source_name} 更新最后导入时间戳: {max_timestamp}")

        total_success += success_count
        total_failed += fail_count

    print(f"\n{'='*60}")
    print(f"账本 {account['name']} 导入完成")
    print(f"总计: 成功 {total_success} | 失败 {total_failed}")
    print(f"{'='*60}")
    logger.info(f"账本 {account['name']} 全部导入完成: 总成功 {total_success}, 总失败 {total_failed}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='飞书记账批量导入工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用说明:
  1. 将支付宝账单 (*.csv) 和微信账单 (*.xlsx) 放到data_dir目录
  2. 运行此脚本,自动扫描并导入所有账单
  3. 脚本会记录上次导入时间,下次只导入新记录

示例:
  # 导入所有账本
  python import.py

  # 只导入jasxu的账本
  python import.py --account jasxu

  # 使用自定义配置文件
  python import.py --config my_config.json
        """
    )

    parser.add_argument(
        '--config',
        default='config.json',
        help='配置文件路径 (默认: config.json)'
    )

    parser.add_argument(
        '--account',
        help='只导入指定账本 (不指定则导入所有账本)'
    )

    args = parser.parse_args()

    # 初始化日志器
    logger = setup_logger()
    logger.info("="*60)
    logger.info("飞书记账批量导入工具启动")
    logger.info("="*60)

    try:
        # 加载配置
        print("加载配置...")
        config = Config(args.config)
        logger.info(f"加载配置文件: {args.config}")

        # 确定要处理的账本
        if args.account:
            accounts = [args.account]
        else:
            accounts = config.list_accounts()

        print(f"将处理 {len(accounts)} 个账本")
        logger.info(f"将处理 {len(accounts)} 个账本: {accounts}")

        # 创建飞书客户端
        print("\n初始化飞书客户端...")
        mcp_config = config.data.get('mcp_server', {})
        client = FeishuClient(mcp_config['app_id'], mcp_config['app_secret'])
        print("客户端初始化成功!")
        logger.info("飞书客户端初始化成功")

        # 处理每个账本
        for account_name in accounts:
            try:
                import_account_bills(config, account_name, client, logger)
            except Exception as e:
                print(f"\n账本 {account_name} 导入失败: {e}", file=sys.stderr)
                logger.error(f"账本 {account_name} 导入失败: {e}", exc_info=True)

        print("\n全部完成!")
        logger.info("全部导入任务完成")

    except KeyboardInterrupt:
        print("\n\n用户中断")
        logger.warning("用户中断导入")
        sys.exit(1)
    except Exception as e:
        print(f"\n错误: {e}", file=sys.stderr)
        logger.error(f"导入过程出错: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
