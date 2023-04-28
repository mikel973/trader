import tushare as ts
import baostock as bs
import pandas as pd

from backtrader.utils import date2num
from datetime import datetime

class DataStock:
    # tushare 接口访问令牌
    __token = ''

    def __init__(self, token):
        self.__token = token

    # 获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
    # https://tushare.pro/document/2?doc_id=25
    # exchange 交易所 SSE 上交所 SZSE 深交所 BSE 北交所
    def stock_ts_basics(self, csv_name):
        pro = ts.pro_api(self.__token)
        basic = pro.stock_basic(list_status='L', fields='ts_code,symbol,name,industry,market,exchange,list_date')
        basic.to_csv(csv_name, index=False)
        return basic

    # 未复权日线行情，最多60000条 ts_code 000001.SZ
    # https://tushare.pro/document/2?doc_id=27
    def stock_ts_daily(self, tscode, csv_name):
        pro = ts.pro_api(self.__token)
        daily = pro.daily(ts_code=tscode)
        daily.to_csv(csv_name, index=False)
        return daily

    # 历史A股K线数据
    # 股票代码，sh或sz.+6位数字代码，或者指数代码
    # adjustflag：复权类型，默认不复权：3；1：后复权；2：前复权
    # frequency：默认为d，日k线；d=日k线
    def stock_bs_daily(self, code, csv_name):
        bs.login()
        rs = bs.query_history_k_data_plus(code, "date,open,high,low,close,preclose,volume,amount,turn", start_date='2015-01-01', frequency="d", adjustflag="2")
        daily = rs.get_data()
        bs.logout()
        daily.to_csv(csv_name, index=False)
        return daily

    # frequency="w" or "m"
    def stock_bs_weekly(self, code, csv_name):
        bs.login()
        rs = bs.query_history_k_data_plus(code, "date,open,high,low,close,volume,amount,turn", start_date='2015-01-01', frequency="w", adjustflag="2")
        weekly = rs.get_data()
        bs.logout()
        weekly.to_csv(csv_name, index=False)
        return weekly

    # frequency="60" or "30" or "15"
    def stock_bs_minutes(self, code, csv_name):
        bs.login()
        rs = bs.query_history_k_data_plus(code, "date,time,open,high,low,close,volume,amount", start_date='2015-01-01', frequency="60", adjustflag="2")
        minutes = rs.get_data()
        bs.logout()

        minutes['time'] = pd.to_datetime(minutes['time'], format='%Y%m%d%H%M%S%f')
        minutes['time'] = minutes['time'].dt.strftime('%H:%M:%S')

        minutes['date'] = minutes['date'] + ' ' + minutes['time']
        # minutes['date'] = date2num(datetime.combine(minutes['date'], minutes['time']))

        minutes.to_csv(csv_name, index=False)

        return minutes


def start():
    print("start stock")
    ds = DataStock("f8ab08eb9eb8223df1758fd93d42870820d74f29268d15ca9ee90a58")
    ds.stock_ts_basics('../test-data/stock_basic.csv')
    code = 'sz.000001'
    ds.stock_bs_daily(code, '../test-data/'+'daily-'+code+'.csv')

    ds.stock_bs_minutes(code, '../test-data/' + 'min60-' + code + '.csv')
    return


if __name__ == '__main__':
    start()
