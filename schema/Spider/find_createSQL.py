import os
import sqlite3
import json



def find_sqlite_files(root_dir):
    """
    遍历文件夹，查找所有的 .sqlite 文件
    """
    sqlite_files = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.sqlite'):
                sqlite_files.append(os.path.join(root, file))
    return sqlite_files

def extract_create_table_statements(db_file):
    """
    从 SQLite 数据库文件中提取所有的 CREATE TABLE 语句
    """
    tables = []
    
    try:
        # 连接到 SQLite 数据库
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # 查询所有的表的创建语句
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table';")
        
        # 提取每个表的名称和建表语句
        rows = cursor.fetchall()
        for row in rows:
            table_name = row[0]
            create_table_sql = row[1]
            # 去掉建表语句中的换行符（\n）并替换为空格
            create_table_sql = create_table_sql.replace('\n', '').strip()
            # 去掉所有的 \t 反斜杠加双引号
            create_table_sql = create_table_sql.replace('\t', ' ').strip()
            # 将双引号全部替换为反引号方便JSON对象存储
            create_table_sql = create_table_sql.replace('\"', '`')
            tables.append({
                "table_name": table_name,
                "create_table_sql": create_table_sql
            })

        # 关闭连接
        conn.close()
    except sqlite3.Error as e:
        print(f"Error reading database {db_file}: {e}")
    
    return tables

def main(root_dir):
    # 获取所有的 .sqlite 文件
    sqlite_files = find_sqlite_files(root_dir)
    
    db_results = []
    
    # 遍历每个 .sqlite 文件并提取建表语句
    for sqlite_file in sqlite_files:
        # print(f"Processing database: {sqlite_file}")

        # 获取数据库名称并去掉文件后缀(只取文件名，不带路径）
        db_name = os.path.splitext(os.path.basename(sqlite_file))[0]
        
        # 提取当前数据库中的所有表
        tables = extract_create_table_statements(sqlite_file)
        
        # 将结果组织成字典
        db_results.append({
            "db_id": db_name,
            "tables": tables
        })


    return db_results


