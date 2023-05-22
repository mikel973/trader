import json
import os
from datetime import datetime
import sys
# pip install pymysql
import pymysql


def do_one_json(task_name, json_name, create_at, update_at):
    # 打开JSON文件并加载数据
    with open(json_name, 'r') as f:
        data = json.load(f)

    # 解析JSON数据
    for item in data:
        file_name = item['CarFileName']
        deal_cid = item['Deals'][0]['DealCid']
        miner_id = item['Deals'][0]['MinerFid']
        print(f"file_name={file_name}, deal_cid={deal_cid}, miner_id={miner_id}")
        do_one_insert(task_name, file_name, deal_cid, miner_id, create_at, update_at)
        # break

def connect():
    # 连接MySQL数据库
    try:
        db = pymysql.connect(
            host='192.168.2.35',
            port=3306,
            user='metaark',
            password='metaark123456',
            database='metaark'
            )

        return db
    except Exception:
        raise Exception("数据库连接失败")


def do_one_insert(task_name, file_name, deal_cid, miner_id, create_at, update_at):
    db = connect()
    cursor = db.cursor()
    try:
        sql_file_id = "SELECT * FROM file WHERE task_name = %s"
        cursor.execute(sql_file_id, [task_name])
        result = cursor.fetchone()
        file_id = result[0]
        #print(f"sql_file_id={sql_file_id}")
        #print(f"file_id={file_id}")

        sql_deal_id = f"SELECT * FROM file_slice WHERE file_name = '{file_name}' AND file_id={file_id}"
        cursor.execute(sql_deal_id)
        result = cursor.fetchall()
        #print(result)
        for row in result:
            #print(row)
            file_slice_id = row[0]

            # 插入一条记录
            create_time = datetime.strptime(create_at, "%Y-%m-%d %H:%M:%S")
            update_time = datetime.strptime(update_at, "%Y-%m-%d %H:%M:%S")
            status = 43
            sql_insert = "INSERT INTO test3 (file_slice_id,file_name,miner_id,deal_id,deal_cid,status,created_at,updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (file_slice_id, file_name, miner_id, 0, deal_cid, status, create_time, update_time)
            num = cursor.execute(sql_insert, val)
            print(f"Done {num} rows ### file_name={file_name}, deal_cid={deal_cid}, miner_id={miner_id}")

        # 提交数据更新
        db.commit()


    except Exception:
        db.rollback()
        print("查询失败")

    cursor.close()
    db.close()


def do_task(directory):
    # 遍历目录中所有的json文件
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            json_path = os.path.join(directory, filename)
            print(json_path)



if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("usage:python main.py 'task_name' 'json_file_path' 'create_at' 'update_at'")
        sys.exit(1)

    task_name = sys.argv[1]
    json_path = sys.argv[2]
    create_at = sys.argv[3]
    update_at = sys.argv[4]
    print(f"task_name={task_name}, json_path={json_path},create_at={create_at},update_at={update_at}")
    do_one_json(task_name, json_path, create_at, update_at)



