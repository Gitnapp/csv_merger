import os
import pandas as pd
import chardet
from sqlalchemy import create_engine, inspect, Table, MetaData, Column, String, UniqueConstraint, text, Date
import argparse
from sqlalchemy.exc import SQLAlchemyError, OperationalError, InternalError
from sqlalchemy.dialects.postgresql import insert
from psycopg2.extras import execute_values, execute_batch
import time
from datetime import datetime, timedelta
from pytz import timezone
import psycopg2
from psycopg2 import errors as psycopg2_errors
import csv

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

def process_dataframe(df):
    # 处理 NaN 值
            # df = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip', dtype={'User ID': str,
    df = df.where(pd.notnull(df), None)
    
    # 将所有列转换为字符串类型
    df = df.astype(str)
    
    # 将 'None' 字符串转换回 None
    df = df.replace('None', None)
    
    # 删除重复行
    df.drop_duplicates(inplace=True)
    
    return df

def read_and_merge_csv_files(input_dir):
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    merged_df = pd.DataFrame()

    for csv_file in csv_files:
        file_path = os.path.join(input_dir, csv_file)
        try:
            encoding = detect_encoding(file_path)
            print(f"检测到 {csv_file} 的编码: {encoding}")
            
            df = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip', dtype={
                'User ID': str,
                'Tweet Count': int,
                'Follower Count': int,
                'Following Count': int,
                'Favorites Count': int,
                'Media Count': int
            })
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        except (pd.errors.ParserError, UnicodeDecodeError) as e:
            print(f"读取文件 {csv_file} 时出错: {e}")

    if merged_df.empty:
        print("没有找到有效的 CSV 文件或所有文件都无法读取。")
        return None

    return process_dataframe(merged_df)

def check_and_grant_permissions(connection, table_name):
    try:
        # 尝试执行一个需要写权限的操作
        connection.execute(text(f"INSERT INTO {table_name} (\"User ID\") VALUES ('test') ON CONFLICT (\"User ID\") DO NOTHING"))
        connection.commit()  # 提交事务
    except SQLAlchemyError as e:
        connection.rollback()  # 回滚事务
        if "permission denied" in str(e).lower():
            print(f"用户没有 {table_name} 表的写权限，尝试授予权限...")
            try:
                connection.execute(text(f"GRANT ALL ON TABLE {table_name} TO db_x"))
                connection.commit()  # 提交事务
                print(f"成功授予 db_x 用户对 {table_name} 表的所有权限")
            except SQLAlchemyError as grant_error:
                connection.rollback()  # 回滚事务
                print(f"授予权限时生错误：{str(grant_error)}")
                raise
        else:
            raise

def merge_csv_files(input_dir, is_test_env, max_retries=3, retry_delay=2):
    merged_df = read_and_merge_csv_files(input_dir)
    if merged_df is None:
        return None, None

    db_url = "postgresql://god:Aa112211@database.pg.polardb.rds.aliyuncs.com:5432/db_x"
    engine = create_engine(db_url)
    
    table_name = 'testenv' if is_test_env else 'user_info'
    
    print(f"尝试连接到数据库并操作表 {table_name}")
    
    for attempt in range(max_retries):
        try:
            with engine.connect() as connection:
                connection.execute(text("BEGIN"))
                connection.execute(text("SELECT pg_sleep(1)"))  # 添加1秒延迟
                try:
                    metadata = MetaData()
                    inspector = inspect(engine)
                    
                    print(f"检查表 {table_name} 是否存在")
                    if not inspector.has_table(table_name):
                        print(f"表 {table_name} 不存在，尝试创建")
                        columns = [Column(col_name, String) for col_name in merged_df.columns]
                        columns.append(Column('inserted_date', Date))
                        columns.append(Column('updated_date', Date))
                        columns.append(Column('exported_date', Date))
                        table = Table(table_name, metadata,
                                      *columns,
                                      UniqueConstraint('User ID', name=f'uq_{table_name}_user_id'))
                        try:
                            table.create(engine)
                            print(f"成功创建了新表 {table_name}，并为 'User ID' 列添加了唯一约束")
                        except SQLAlchemyError as e:
                            print(f"创建表时发生错误：{str(e)}")
                            raise
                    else:
                        print(f"表 {table_name} 已存在，检查唯一约束和必要列")
                        table = Table(table_name, metadata, autoload_with=engine)
                        constraints = inspector.get_unique_constraints(table_name)
                        constraint_names = [c['name'] for c in constraints]
                        
                        print(f"现有的唯一约束：{constraint_names}")
                        
                        if f'uq_{table_name}_user_id' not in constraint_names:
                            print(f"尝试为 {table_name} 表的 'User ID' 列添加唯一约束")
                            try:
                                with engine.begin() as conn:
                                    conn.execute(text(f"ALTER TABLE {table_name} ADD CONSTRAINT uq_{table_name}_user_id UNIQUE (\"User ID\")"))
                                print(f"成功为 {table_name} 表的 'User ID' 列添加了唯一约束")
                            except SQLAlchemyError as e:
                                print(f"添加唯一约束时发生错误：{str(e)}")
                                raise
                        else:
                            print(f"{table_name} 表的 'User ID' 列已经有唯一约束")
                        
                        # 检查必要列是否存在
                        existing_columns = inspector.get_columns(table_name)
                        existing_column_names = [col['name'] for col in existing_columns]
                        
                        for col_name in ['inserted_date', 'updated_date', 'exported_date']:
                            if col_name not in existing_column_names:
                                print(f"添加缺失的列：{col_name}")
                                with engine.begin() as conn:
                                    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {col_name} DATE"))
                                print(f"成功添加列：{col_name}")

                    connection.execute(text("SELECT pg_sleep(1)"))  # 添加1秒延迟

                    # 检查并授予权限
                    check_and_grant_permissions(connection, table_name)

                    connection.execute(text("SELECT pg_sleep(1)"))  # 添加1秒延迟

                    print("准备批量 UPSERT 语句")
                    
                    # 准备数据
                    data = merged_df.where(pd.notnull(merged_df), None).values.tolist()
                    columns = merged_df.columns.tolist()

                    # 构建 UPSERT 语句
                    insert_stmt = f"""
                        INSERT INTO {table_name} ({', '.join(f'"{col}"' for col in columns)}, inserted_date, updated_date)
                        VALUES ({', '.join(['%s' for _ in columns])}, CURRENT_DATE, CURRENT_DATE)
                        ON CONFLICT ("User ID") DO UPDATE SET
                        {', '.join(f'"{col}" = EXCLUDED."{col}"' for col in columns if col != 'User ID')},
                        inserted_date = CURRENT_DATE,
                        updated_date = CURRENT_DATE
                    """

                    print("执行批量 UPSERT")
                    with connection.connection.cursor() as cursor:
                        total_rows = len(data)
                        rows_inserted = 0
                        
                        def execute_batch_with_progress(cursor, sql, argslist, page_size):
                            nonlocal rows_inserted
                            for page in range(0, len(argslist), page_size):
                                batch_data = argslist[page:page + page_size]
                                savepoint_name = f"sp_{page // page_size}"
                                
                                try:
                                    cursor.execute(f"SAVEPOINT {savepoint_name}")
                                    
                                    for batch_attempt in range(max_retries):
                                        try:
                                            execute_batch(cursor, sql, batch_data)
                                            rows_inserted += len(batch_data)
                                            print(f"进度: {rows_inserted}/{total_rows} 行已处理 ({rows_inserted/total_rows*100:.2f}%)")
                                            cursor.execute("SELECT pg_sleep(1)")  # 每批次添加1秒延迟
                                            break  # 如果成功，跳出重试循环
                                        except Exception as batch_error:
                                            if isinstance(batch_error, (psycopg2.Error, psycopg2_errors.Error)):
                                                error_code = batch_error.pgcode if hasattr(batch_error, 'pgcode') else 'Unknown'
                                                print(f"PostgreSQL 错误代码: {error_code}")
                                            
                                            if batch_attempt < max_retries - 1:
                                                print(f"批量插入错误（尝试 {batch_attempt + 1}/{max_retries}）：{str(batch_error)}")
                                                print(f"错误类型: {type(batch_error).__name__}")
                                                print(f"等待 {retry_delay} 秒后重试...")
                                                cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                                                time.sleep(retry_delay)
                                            else:
                                                print("达到最大重试次数，批量插入失败。")
                                                raise
                                    
                                    cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                                except Exception as e:
                                    print(f"处理批次时发生错误：{str(e)}")
                                    cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                                    raise
                        
                        execute_batch_with_progress(cursor, insert_stmt, data, 1000)
                        
                    print(f"成功更新了 {rows_inserted} 行数据")
                    connection.commit()
                    return engine, table_name  # 成功完成，返回结果
                except (SQLAlchemyError, InternalError) as e:
                    connection.rollback()
                    raise e

        except OperationalError as e:
            if "catalog snapshot" in str(e) or "Catalog Version Mismatch" in str(e):
                print(f"发生目录快照错误（尝试 {attempt + 1}/{max_retries}）：{str(e)}")
                if attempt < max_retries - 1:
                    print(f"等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                else:
                    print("达到最大重试次数，操作失败。")
                    raise
            else:
                raise
        except (SQLAlchemyError, InternalError) as e:
            print(f"数据库操作错误：{str(e)}")
            print(f"错误类型：{type(e).__name__}")
            print(f"错误详情：{e.args}")
            if attempt < max_retries - 1:
                print(f"等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
            else:
                raise
        except Exception as e:
            print(f"发生未知错误：{str(e)}")
            print(f"错误类型：{type(e).__name__}")
            print(f"错误详情：{e.args}")
            raise
        finally:
            engine.dispose()

    print(f"合并的数据已尝试保存到 PostgreSQL 数据库中的 {table_name} 表")
    return None, None  # 如果所有尝试都失败，返回 None

def filter_today_data(engine, table_name):
    try:
        with engine.connect() as connection:
            # 定义要排除的关键字
            exclusion_keywords_universal = [
                'toy', 'amazon', 'seller', 'review', 
                'artist', 'illustrator', 'draw', 'fat',
                'ñ', 'á', 'é', 'í', 'ó', 'ú', 'ü',
                'ب', 'ت', 'ث', 'ج', 'ح', 'خ', 'د', 'ذ', 'ر', 'ز', 'س', 'ش', 'ص', 'ض', 'ط', 'ظ', 'ع', 'غ', 'ف', 'ق', 'ك', 'ل', 'م', 'ن', 'و', 'ي'
                'あ', 'い', 'う', 'え', 'か', 'き', 'く', 'け', 'こ', 'さ', 'し', 'す', 'せ', 'そ', 'た', 'ち', 'つ', 'て', 'と', 'な', 'に', 'ぬ', 'ね', 'の', 'は', 'ひ', 'ふ', 'へ', 'ほ', 'ま', 'み', 'む', 'め', 'も', 'や', 'ゆ', 'よ', 'ら', 'り', 'る', 'れ', 'ろ','わ', 'を', 'ん',
                'ア', 'イ', 'ウ', 'エ', 'オ', 'カ', 'キ', 'ク', 'ケ', 'コ', 'サ', 'シ', 'ス', 'セ', 'ソ', 'タ', 'チ', 'ツ', 'テ', 'ト', 'ナ', 'ニ', 'ヌ', 'ネ', 'ハ', 'ヒ', 'フ', 'ヘ', 'ホ', 'マ', 'ミ', 'ム', 'メ', 'モ', 'ヤ', 'ユ', 'ヨ', 'ラ', 'リ', 'ル', 'レ', 'ロ', 'ワ', 'ヲ', 'ン',
                'ㄱ', 'ㄴ', 'ㄷ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅅ', 'ㅇ', 'ㅈ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ', 'ㅏ', 'ㅑ', 'ㅓ', 'ㅕ', 'ㅗ', 'ㅛ', 'ㅜ', 'ㅠ', 'ㅡ', 'ㅣ'
            ]            
            exclusion_keywords_name_username = []
            exclusion_keywords_bio = []
            exclusion_keywords_location = [
                'mexico', 'méxico', 'mx', 'fr', 'spain', 'España', 'á', 'é', 'í', 
                'ó', 'ú', 'ü', 'United Kingdom', 'UK', 'England', 'Netherlands', 
                'NL', 'china', 'New Zealand', 'Portugal', 'ç', 'Germany', 'Brazil', 
                'Poland', 'Japan', 'Belgium', 'Denmark', 'Australia', 'Brasil', 'Ireland', 
                'chile', 'dubai', 'pakistan', 'india', 'france', 'italy', 'Italia', 'netherlands'
            ]

            # 获取数据库的时区
            db_timezone = timezone('Asia/Shanghai')  # 根据实际情况调整时区
            
            # 获取当前日期和时间（考虑时区）
            now = datetime.now(db_timezone)
            today = now.date()
            yesterday = today - timedelta(days=1)

            # 构建 NOT ILIKE 条件
            universal_not_like_keywords = " AND ".join([
                f'"{column}" NOT ILIKE ALL(ARRAY[{", ".join([f"\'%{kw}%\'" for kw in exclusion_keywords_universal])}])'
                for column in ['Name', 'Username', 'Bio', 'Location']
            ])

            not_like_conditions_name_username = " AND ".join([
                f'"Name" NOT ILIKE \'%{kw}%\' AND "Username" NOT ILIKE \'%{kw}%\''
                for kw in exclusion_keywords_name_username
            ])

            not_like_conditions_bio = " AND ".join([
                f'"Bio" NOT ILIKE \'%{kw}%\''
                for kw in exclusion_keywords_bio
            ])

            not_like_conditions_location = " AND ".join([
                f'"Location" NOT ILIKE \'%{kw}%\''
                for kw in exclusion_keywords_location
            ])

            # WHERE DATE(inserted_date) >= :yesterday
            # AND DATE(inserted_date) <= :today

            # 构建查询语句
            query = text(f"""
                SELECT * FROM "{table_name}"
                WHERE DATE(inserted_date) = :today
                AND DATE(inserted_date) = DATE(updated_date)
                AND {universal_not_like_keywords}
                {f'AND {not_like_conditions_name_username}' if not_like_conditions_name_username else ''}
                {f'AND {not_like_conditions_bio}' if not_like_conditions_bio else ''}
                {f'AND {not_like_conditions_location}' if not_like_conditions_location else ''}
                AND CAST("Tweet Count" AS INTEGER) > 150
                AND (
                    (CAST("Follower Count" AS INTEGER) <= 2000 AND "Blue Verified" = 'Yes')
                    OR CAST("Follower Count" AS INTEGER) > 2000
                )
                AND CAST("Follower Count" AS INTEGER) < 1000000
                AND CAST("Media Count" AS INTEGER) > 27
                AND "Can DM" = 'Yes'
                AND TO_TIMESTAMP("Created At", 'Dy Mon DD HH24:MI:SS "+0000" YYYY') < CURRENT_DATE - INTERVAL '3 months'
                AND (exported_date IS NULL OR exported_date < CURRENT_DATE - INTERVAL '1 month')
                ORDER BY inserted_date DESC
            """)

            result = connection.execute(query, {"yesterday": yesterday, "today": today})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            print(f"查询到 {len(df)} 条记录")
            print(f"日期范围：{yesterday} 到 {today}")
            
            if not df.empty:
                print("插入日期范围：")
                print(f"最早：{df['inserted_date'].min()}")
                print(f"最晚：{df['inserted_date'].max()}")
            
            return df

    except Exception as e:
        print(f"发生错误：{e}")
        return pd.DataFrame()

def to_csv_file(filtered_df):
    base_filename = 'merge.csv'
    script_dir = os.path.dirname(__file__)
    
    # 查找可用的文件名
    counter = 0
    while True:
        if counter == 0:
            filename = base_filename
        else:
            filename = f'merge_{counter}.csv'
        
        to_csv_file = os.path.join(script_dir, filename)
        if not os.path.exists(to_csv_file):
            break
        counter += 1
    
    # 将所有列转换为字符串类型，并用空字符串替换 None 值
    filtered_df = filtered_df.astype(str).replace('None', '')
    # 提取 User ID 列并创建一个集合
    exported_user_id_set = set(filtered_df['User ID'])
    
    # 使用 csv 模块写入文件，这样可以更好地控制输出格式
    with open(to_csv_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(filtered_df.columns)  # 写入列名
        for _, row in filtered_df.iterrows():
            writer.writerow(row)
    
    print(f"数据已保存到 {to_csv_file}")

    return exported_user_id_set

def update_exported_date(engine, table_name, exported_user_id_set):
    try:
        with engine.connect() as connection:
            # 获取当前日期和时间（考虑时区）
            now = datetime.now()
            today = now.date()

            # 将 User ID 集合转换为列表
            user_id_list = list(exported_user_id_set)

            # 构建更新语句
            update_stmt = text(f"""
                UPDATE "{table_name}"
                SET exported_date = :today
                WHERE "User ID" = ANY(:user_ids)
                AND DATE(inserted_date) = :today
                AND DATE(inserted_date) = DATE(updated_date)
            """)

            # 执行更新操作
            result = connection.execute(update_stmt, {"today": today, "user_ids": user_id_list})
            connection.commit()

            # 获取更新的行数
            updated_rows = result.rowcount
            print(f"成功更新了 {table_name} 表中 {updated_rows} 行的 exported_date 列") 

    except Exception as e:
        print(f"更新 exported_date 时发生错误：{e}")
        print(f"错误类型：{type(e).__name__}")
        print(f"错误详情：{e.args}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='合并 CSV 文件到数据库')
    parser.add_argument('-t', '--test', action='store_true', help='测试环境中调试')
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(script_dir, 'py_tomerge')

    engine, table_name = merge_csv_files(input_dir, args.test)
    if engine and table_name:
        filtered_df = filter_today_data(engine, table_name)
        print("今天筛选出的数据前20行：")
        print(filtered_df.head(200))
        exported_user_id_set = to_csv_file(filtered_df)
        update_exported_date(engine, table_name, exported_user_id_set)
    else:
        print("无法处理数据或连接数据库。")

