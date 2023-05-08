import tushare as ts
import baostock as bs
import pandas as pd
import sqlite3

from datetime import datetime


class ChinaStock:
    # tushare 接口访问令牌
    __token = ''
    __is_login = False

    def __init__(self, token):
        self.__token = token
        self.__is_login = False

    # 获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
    # https://tushare.pro/document/2?doc_id=25
    # exchange 交易所 SSE 上交所 SZSE 深交所 BSE 北交所
    def stock_ts_basics(self):
        # 'fullname,enname,cnspell,delist_date'
        col_name = 'ts_code,symbol,name,area,industry,market,exchange,list_date,is_hs'
        pro = ts.pro_api(self.__token)
        basic = pro.stock_basic(list_status='L', fields=col_name)
        # basic.to_csv(csv_name, index=True)
        return basic

    def stock_bs_login(self):
        bs.login()
        self.__is_login = True
        return

    def stock_bs_logout(self):
        bs.logout()
        self.__is_login = False
        return

    # 历史A股K线数据
    # http://baostock.com/baostock/index.php/Python_API%E6%96%87%E6%A1%A3
    # 股票代码，sh或sz.+6位数字代码，或者指数代码
    # adjustflag：复权类型，默认不复权：3；1：后复权；2：前复权
    # frequency：默认为d，日k线；d=日k线 w=周k线
    def stock_bs_daily(self, code, from_date):

        if not self.__is_login:
            return None
        rs = bs.query_history_k_data_plus(code, "date,open,high,low,close,volume,turn", start_date=from_date, frequency="d", adjustflag="2")
        daily = rs.get_data()
        return daily

    # frequency="60" or "30" or "15"
    def stock_bs_minutes(self, code, from_date, to_date=None, freq='60'):
        if not self.__is_login:
            return None
        #数据条数限制 1W 以内，需要分段获取
        rs = bs.query_history_k_data_plus(code, "date,time,open,high,low,close,volume",
                                          start_date=from_date,
                                          end_date=to_date,
                                          frequency=freq,
                                          adjustflag="2")
        minutes = rs.get_data()
        return minutes

def update_basic(name_database, force_update: bool = False):
    # 连接到SQLite数据库
    conn = sqlite3.connect(name_database)
    # 创建游标对象
    cur = conn.cursor()
    # 查询是否已经存在basic表
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='basic';")
    result = cur.fetchone()

    if result is None:
        # 如果basic表不存在，则创建basic表
        print("try to create basic table...")
        sql = '''
                    CREATE TABLE basic (
                        id INTEGER PRIMARY KEY,
                        ts_code TEXT,
                        symbol TEXT,
                        name TEXT,
                        area TEXT,
                        industry TEXT,
                        market TEXT,
                        exchange TEXT,
                        list_date TEXT,
                        is_hs TEXT,
                        is_daily INTEGER,
                        is_m60 INTEGER,
                        is_m30 INTEGER,
                        is_m15 INTEGER
                    )
                '''
        cur.execute(sql)
        print("basic table created successfully.")
        conn.commit()
    else:
        # basic 表存在,但是不强制更新，则直接返回
        if not force_update:
            return

    # 更新basic表的数据
    ts_token = "f8ab08eb9eb8223df1758fd93d42870820d74f29268d15ca9ee90a58"
    cs = ChinaStock(ts_token)
    basic = cs.stock_ts_basics()
    if basic is None:
        print('获取股票基本信息失败')
        return

    # 将DataFrame中的数据写入SQLite数据库中的新表basic
    basic['is_daily'] = 0
    basic['is_m60'] = 0
    basic['is_m30'] = 0
    basic['is_m15'] = 0
    result = basic.to_sql(name='basic', con=conn, if_exists='replace', index=True, index_label='id')
    if result is None:
        print('添加记录到数据库失败:'+result)

    # 提交更改
    conn.commit()

    # 关闭游标和连接
    cur.close()
    conn.close()

    print(f"共更新basic数据{len(basic)}条记录.")
    return


def to_float():
    print(name)
def get_stock_daily(db_name):

    # 连接到SQLite数据库
    conn = sqlite3.connect(db_name)

    # 从数据库中读取数据到DataFrame
    query = "SELECT symbol,exchange,list_date FROM basic WHERE is_daily=0 "
    df = pd.read_sql_query(query, conn)

    cs = ChinaStock("f8ab08eb9eb8223df1758fd93d42870820d74f29268d15ca9ee90a58")
    cs.stock_bs_login()

    # 遍历每一行数据
    cur = conn.cursor()
    for index, row in df.iterrows():
        print('exchange:'+row['exchange'] + '  symbol:'+row['symbol'] + '  start:'+row['list_date'])

        from_date = datetime.strptime(row['list_date'], '%Y%m%d').strftime('%Y-%m-%d')
        symbol = row['symbol']
        exchange = row['exchange']

        if exchange == 'SZSE':
            code = 'sz.' + symbol
        elif exchange == 'SSE':
            code = 'sh.' + symbol

        daily = cs.stock_bs_daily(code, from_date)
        if daily is None:
            print(f"### 获取{symbol}历史数据失败:")
            continue

        daily['symbol'] = symbol
        daily['open'] = daily['open'].astype(float)
        daily['high'] = daily['high'].astype(float)
        daily['low'] = daily['low'].astype(float)
        daily['close'] = daily['close'].astype(float)

        # 检查并清理volume列中的异常值
        for vi, vr in daily.iterrows():
            v = vr['volume']
            if not v.isnumeric() and not v.replace('.', '').isnumeric():
                daily.at[vi, 'volume'] = None
                print(f'volume exception: {v}')
        # 将volume列的字符变量转换成float类型
        daily['volume'] = daily['volume'].astype(float)

        result = daily.to_sql(name='daily', con=conn, if_exists='append', index=True)
        if result is None:
            print(f"### 追加{symbol}历史数据失败:{result}")
            continue
        conn.commit()
        print(f"追加{symbol}历史数据记录:{result}条.")

        # 更新basic表状态
        cur.execute(f"UPDATE basic SET is_daily=? WHERE symbol=?", (1, symbol))
        conn.commit()

        # Only for test
        # break

    cs.stock_bs_logout()

    # 关闭数据库连接
    cur.close()
    conn.close()

    return


def get_stock_minutes(db_name, freq='60'):
    # 连接到SQLite数据库
    conn = sqlite3.connect(db_name)

    # 从数据库中读取数据到DataFrame
    query = f"SELECT symbol,exchange,list_date FROM basic WHERE is_m{freq}=0 "
    df = pd.read_sql_query(query, conn)

    cs = ChinaStock("f8ab08eb9eb8223df1758fd93d42870820d74f29268d15ca9ee90a58")
    cs.stock_bs_login()

    # 遍历每一行数据
    cur = conn.cursor()
    for index, row in df.iterrows():
        print('exchange:'+row['exchange'] + '  symbol:'+row['symbol'] + '  start:'+row['list_date'])

        from_date = datetime.strptime(row['list_date'], '%Y%m%d').strftime('%Y-%m-%d')
        symbol = row['symbol']
        exchange = row['exchange']

        if exchange == 'SZSE':
            code = 'sz.' + symbol
        elif exchange == 'SSE':
            code = 'sh.' + symbol

        from_date = '2022-01-01'
        to_date = datetime.now().strftime('%Y-%m-%d')
        minu = cs.stock_bs_minutes(code, from_date, to_date, freq)
        if minu is None:
            print(f"### 获取{symbol}历史数据失败:")
            continue

        minu['symbol'] = symbol
        result = minu.to_sql(name='m'+freq, con=conn, if_exists='append', index=False)
        if result is None:
            print(f"### 追加{symbol}历史数据失败:{result}")
            continue
        conn.commit()
        print(f"追加{symbol}历史数据记录:{result}条.")

        # 更新basic表状态
        cur.execute(f"UPDATE basic SET is_m{freq}=? WHERE symbol=?", (1, symbol))
        conn.commit()

        break

    cs.stock_bs_logout()

    # 关闭数据库连接
    cur.close()
    conn.close()

    return


if __name__ == '__main__':
    # start()
    db_name = '../test-data/stock.db'
    update_basic(db_name)
    get_stock_daily(db_name)
    #get_stock_minutes(db_name, '30')
