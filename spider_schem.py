import json
import find_createSQL

#spider中数据库路径
database_path = "F:\\Chrome_Download\\spider_data\\test_database"
#spider中tables.json路径
tables_path = "F:\\Chrome_Download\\spider_data\\test_tables.json"
#想要保存的最终文件路径
write_path = "E:\\NL2SQL\\My_NL2SQL\\spider_process\\spider_test_database_schema.json"

#读取JSON文件
def read_json(json_path):
    # JSON读文件
    with open(json_path, 'r') as file:
        data = json.load(file)

    return data

#写为新的JSON文件
def write_json(json_path,data):
    
    # 写入 JSON 文件
    with open(json_path, "w") as json_file:
        json.dump(data, json_file, indent=4)  # indent=4 用来格式化 JSON 输出

    print()
   

#组织新的表形式
def new_tables(item,create_tables_SQL):
    #保存生成好的新的表结构
    tables = []

    #以字典的形式存储数据库中的所有表名
    tables_names_dict = {i: item["table_names_original"][i] for i in range(len(item["table_names_original"]))}
    
    #获取库中全部列名
    columns_names_list = item["column_names_original"]

    #获取库中全部列类型
    columns_type_list = item["column_types"]

    #获取库中的全部主键
    columns_primary_key = item['primary_keys']


    #将列名和列类型对应起来
    columns_with_types = [
        {
        "column_index": idx,           # 自增的 ID，从 0 开始
        "table_index": column[0],  # 表索引
        "name": column[1],   # 列名
        "type": columns_type_list[idx]  # 列类型
        }
        for idx, column in enumerate(columns_names_list)
    ]


    for table_id,table_name in tables_names_dict.items():

        
        #临时存储这个表中的列名
        columns_names = ["*"]

        #临时存储这个表中的列类型
        columns_types = ["text"]

        #临时存储这个表中的主键
        column_primary_key = ""

        #临时存储这个表的建表语句
        create_table_sql = ""

        # 遍历全部列名的子列表，判断其值是否属于当前表
        for column in columns_with_types:
            #获取信息
            column_index = column["column_index"]
            table_index = column["table_index"]
            column_name = column["name"]
            column_type = column["type"]
            if table_index == table_id:
                #属于当前表，就加入这个表的列名那个以及列对应的属性
                columns_names.append(column_name)
                columns_types.append(column_type)
                if column_index in columns_primary_key:
                    column_primary_key = column_name
            else:
                continue

        ###找到对应的建表语句
        for table_with_sql in create_tables_SQL:
            if table_name == table_with_sql["table_name"]:
                create_table_sql = table_with_sql["create_table_sql"]
                break


        #新的表组织形式
        new_table_object = {
            "table_name": table_name,
            "columns_name": columns_names,
            "columns_type": columns_types,
            "primary_key": column_primary_key,
            "create_table_sql":create_table_sql
        }

        #加入列表，并返回
        tables.append(new_table_object)


    return tables

#组织新的外键形式
def new_foreigin_keys(item):

    #临时存储外键
    foreigins_keys = []

    
    #获取库中全部列名
    columns_names_list = item["column_names_original"]

    #获取库中全部表名
    tables_name_lsit = item["table_names_original"]

    #获取库中全部外键
    foreigins_key = item["foreign_keys"]
    
    #将列名和数据库名对应起来
    columns_with_db = [
        {
        "column_index": idx,           # 自增的 ID，从 0 开始
        "table_index": column[0],  # 表索引
        "name": column[1],   # 列名
        "table_name": tables_name_lsit[column[0]]  # 表名
        }
        for idx, column in enumerate(columns_names_list)
        if column[0] != -1 #排除表索引为-1的列名，即*号
    ]

    #循环处理外键
    for foreigin_key in foreigins_key:
        #两个量
        f_k,s_k = foreigin_key
        
        # 查找目标列
        f_target = next(item for item in columns_with_db if item["column_index"] == f_k)
        s_target = next(item for item in columns_with_db if item["column_index"] == s_k)

        # 生成第一个表列名
        f_s = f'{f_target["table_name"]}.{f_target["name"]}'
        s_s = f'{s_target["table_name"]}.{s_target["name"]}'

        foreigins_keys.append(f_s + "=" + s_s)


    return foreigins_keys


    

#生成新的JSON组织形式
def new_json(item,create_all_table_SQL):

    #找到当前数据库中所有的建表语句
    for db_tables in create_all_table_SQL:
        if item["db_id"] == db_tables["db_id"]:
            create_tables_SQL = db_tables["tables"]
            break
        else:
            continue


    #生成新的表组织形式
    tables = new_tables(item,create_tables_SQL)

    #生成新的外键组织形式
    foreigin_keys = new_foreigin_keys(item)

    '''未来可以在这里做表关系推断做关系'''
    #ralationships = rs_generation()


    new_json_object = {
        "db_id": item["db_id"],
        "tables": tables,
        "foreign_keys": foreigin_keys,
        "relationships": []
        }
    
    return new_json_object



if __name__ == "__main__":

    #读取spider中tables.json文件
    data = read_json(tables_path)

    #获取全部的建表语句
    create_all_table_SQL = find_createSQL.main(database_path)

    #存储每一个改进的JSON对象
    json_object = []

    for item in data:

        #改进JSON对象
        new_json_object = new_json(item,create_all_table_SQL)

        #追加JSON对象
        json_object.append(new_json_object)


    write_json(write_path,json_object)


