import requests
import sqlite3
import json
import time
import pandas as pd


def build_task(name_database, address, page_count=100, replase: bool=False) -> None:
    # 连接到SQLite数据库
    conn = sqlite3.connect(name_database)

    # 创建task
    data = {'page': range(page_count)}
    task = pd.DataFrame(data)
    task['name'] = address
    task['is_done'] = 0
    # 将DataFrame存储到SQLite数据库中 replace  append
    if replase:
        task.to_sql('task', conn, if_exists='replace', index=False)
    else:
        task.to_sql('task', conn, if_exists='append', index=False)

    # 提交更改
    conn.commit()

    # 关闭连接
    conn.close()


def do_deal_task(db_name):

    # 连接到SQLite数据库
    conn = sqlite3.connect(db_name)

    # 从数据库中读取数据到DataFrame
    query_task = "SELECT name,page,is_done FROM task WHERE is_done=0 "
    tasks = pd.read_sql_query(query_task, conn)

    # 遍历每一行数据
    cur = conn.cursor()

    for index, row in tasks.iterrows():
        if (index+1) % 10 == 0:
            print(f"休息 休息，马上回来 ......")
            time.sleep(20)
        page = row['page']
        address = row['name']

        print(f"do {address} -- {page} -- ({index}/{len(tasks)}).")
        url = f'https://filfox.info/api/v1/deal/list?address={address}&pageSize=100&page={page}'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        response = requests.get(url, headers=headers)
        data = response.json()['deals']
        deal = pd.DataFrame(data)
        deal = deal[['id', 'client', 'provider']]
        deal['cid'] = ""
        #print(data)
        result = deal.to_sql(name='deals', con=conn, if_exists='append', index=False)
        if result is None:
            print(f"### 追加第{page}页数据失败:{result}")
            break
        conn.commit()
        print(f"追加第{page}页数据记录:{result}条.")

        # 更新basic表状态
        cur.execute(f"UPDATE task SET is_done=? WHERE page=?", (1, f'{page}'))
        conn.commit()
        # Only for test
        # break

    # 关闭数据库连接
    cur.close()
    conn.close()

    return


def do_cid_task(db_name):

    # 连接到SQLite数据库
    conn = sqlite3.connect(db_name)

    # 从数据库中读取数据到DataFrame
    query_task = "SELECT id,client,cid FROM deals WHERE cid = '' and client='f1jpvx2qvvgil7gowa5pkxjtueeau3zwkoknjwtsi'"
    tasks = pd.read_sql_query(query_task, conn)

    # 遍历每一行数据
    cur = conn.cursor()
    for index, row in tasks.iterrows():
        # if (index+1) % 20 == 0:
        #     print(f"休息 休息，马上回来 ......")
        time.sleep(1)
        id = row['id']
        address = row['client']
        print(f"do id={id} -- ({index}/{len(tasks)}).")

        url = f'https://filfox.info/api/v1/deal/{id}'
        response = requests.get(url)
        data = response.json()
        #print(data)

        if id != data['id']:
            print(f"id not equal {id}-{data['id']}.")
        if address != data['client']:
            print(f"address not equal {address}-{data['client']}.")

        cid = data['pieceCid']
        # 更新basic表状态
        cur.execute(f"UPDATE deals SET cid=? WHERE id=? and client=?", (cid, f'{id}', f'{address}'))
        conn.commit()
        # Only for test
        # break

    # 关闭数据库连接
    cur.close()
    conn.close()

    return


def save_to_csv(db_name):
    # 连接到SQLite数据库
    conn = sqlite3.connect(db_name)

    # 从数据库中读取deal表数据
    df = pd.read_sql_query("SELECT id from deals where client='f1jpvx2qvvgil7gowa5pkxjtueeau3zwkoknjwtsi'", conn)

    # 将数据写入CSV文件

    df.to_csv('deals-2.csv', index=False)

    # 关闭数据库连接
    conn.close()


if __name__ == '__main__':
    db_name = './deals.db'

    # address1 = 'f1x4jjrsot2gevrxiqwgzgjh7kzh6c6kv3kkuyv6a'
    # build_task(db_name, address1, 118, replase=True)
    # address2 = 'f1jpvx2qvvgil7gowa5pkxjtueeau3zwkoknjwtsi'
    # build_task(db_name, address2, 65)

    # do_deal_task(db_name)

    # do_cid_task(db_name)

    save_to_csv(db_name)


