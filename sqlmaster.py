import os
import pandas as pd
import chardet
from sqlalchemy import create_engine, inspect, Table, MetaData, Column, String, UniqueConstraint, text, DateTime, Date
import argparse
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime

# 创建数据库连接
db_url = "postgresql://db_x:Aa112211@101.132.80.183:5433/db1b41131c723c43d1aeadb8fb3f0175dedb_x"
engine = create_engine(db_url)

# 设置表名
table_name = 'user_info'

# 定义表结构
metadata = MetaData()
user_info_table = Table(
    table_name,
    metadata,
    Column('User ID', String, primary_key=True),
    Column('last_imported_date', Date),
    Column('last_imported_user', String),
    Column('reviewer_type', String),
    Column('comment', String),
    Column('comment_author', String),
    Column('inserted_date', DateTime, default=datetime.utcnow)
)

# 确保表存在
metadata.create_all(engine)

def insert_or_update_user_info(data):
    try:
        with engine.begin() as conn:
            stmt = insert(user_info_table).values(data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['User ID'],
                set_={
                    'last_imported_date': stmt.excluded.last_imported_date,
                    'last_imported_user': stmt.excluded.last_imported_user,
                    'reviewer_type': stmt.excluded.reviewer_type,
                    'comment': stmt.excluded.comment,
                    'comment_author': stmt.excluded.comment_author,
                    'inserted_date': datetime.utcnow()
                }
            )
            result = conn.execute(stmt)
            print(f"成功插入或更新了 {result.rowcount} 条记录")
            return True
    except SQLAlchemyError as e:
        print(f"数据库操作错误：{str(e)}")
        return False

# 这个函数现在只负责插入一条测试数据
def insert_test_data():
    test_data = [{
        'User ID': 'test_user',
        'last_imported_date': datetime.now().date(),
        'last_imported_user': 'test_script',
        'reviewer_type': 'test_type',
        'comment': 'This is a test comment',
        'comment_author': 'test_author',
    }]
    return insert_or_update_user_info(test_data)

if __name__ == "__main__":
    if insert_test_data():
        print(f"表 {table_name} 已创建或更新，包含新增字段，并成功插入测试数据")
    else:
        print(f"表 {table_name} 创建或更新失败")
