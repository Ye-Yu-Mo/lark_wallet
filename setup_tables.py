#!/usr/bin/env python3
"""
数据库初始化脚本
创建 SQLite 数据库和表结构
"""
import sys
import os
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from core.database import AssetDB
from core.config import Config


def main():
    """主函数"""
    print("=" * 60)
    print("资产同步系统 - 数据库初始化")
    print("=" * 60)

    try:
        # 加载配置
        print("\n[1/4] 加载配置文件...")
        config = Config('config.json')
        db_config = config.get_database_config()
        db_path = db_config.get('path', 'data/assets.db')
        print(f"✓ 配置加载成功")
        print(f"  数据库路径: {db_path}")

        # 检查数据库是否已存在
        db_exists = os.path.exists(db_path)
        if db_exists:
            print(f"\n⚠ 数据库已存在: {db_path}")
            response = input("是否继续初始化? (y/n): ")
            if response.lower() != 'y':
                print("✗ 初始化已取消")
                return

        # 创建数据库
        print("\n[2/4] 初始化数据库...")
        db = AssetDB(db_path)
        print(f"✓ 数据库创建成功: {db_path}")

        # 验证表结构
        print("\n[3/4] 验证表结构...")
        tables = [
            'price_history',
            'klines',
            'holdings',
            'orders',
            'feishu_holdings',
            'feishu_history',
            'feishu_logs',
            'feishu_backup_meta'
        ]

        for table_name in tables:
            try:
                count = db.get_table_count(table_name)
                print(f"✓ 表 {table_name:<20} 已创建 (记录数: {count})")
            except Exception as e:
                print(f"✗ 表 {table_name:<20} 验证失败: {e}")
                return

        # 验证索引
        print("\n[4/4] 验证索引...")
        import sqlite3
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name, tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            )
            indexes = cursor.fetchall()

            if len(indexes) >= 3:  # 至少3个索引
                for idx_name, table_name in indexes:
                    print(f"✓ 索引 {idx_name:<30} (表: {table_name})")
                print(f"\n✓ 所有索引创建成功 (共{len(indexes)}个)")
            else:
                print(f"⚠ 索引数量不足: {len(indexes)}/3")

        # 显示表结构详情
        print("\n" + "=" * 60)
        print("表结构详情")
        print("=" * 60)

        for table_name in tables:
            info = db.get_table_info(table_name)
            print(f"\n{table_name}:")
            for col in info:
                col_name = col[1]
                col_type = col[2]
                is_pk = " [主键]" if col[5] else ""
                is_not_null = " [NOT NULL]" if col[3] else ""
                print(f"  - {col_name:<20} {col_type:<10}{is_pk}{is_not_null}")

        # 完成
        print("\n" + "=" * 60)
        print("✓ 数据库初始化完成")
        print("=" * 60)
        print(f"\n数据库文件: {os.path.abspath(db_path)}")
        print(f"数据库大小: {os.path.getsize(db_path)} bytes")
        print("\n可以开始使用资产同步系统了!")

    except FileNotFoundError as e:
        print(f"\n✗ 错误: {e}")
        print("请确保 config.json 文件存在")
        sys.exit(1)

    except ValueError as e:
        print(f"\n✗ 配置错误: {e}")
        print("请检查 config.json 中的 asset_sync 配置")
        sys.exit(1)

    except Exception as e:
        print(f"\n✗ 初始化失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
