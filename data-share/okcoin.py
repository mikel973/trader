import sqlite3
import datetime
import pandas as pd
import okx.MarketData as MarketData

def start():
    conn = sqlite3.connect(db_name)

    symbol = "BTC-USDT"

    api_key = ''
    api_secret_key = ''
    passphrase = ''
    # flag = "1"  # live trading: 0, demo trading: 1
    market_api = MarketData.MarketAPI(api_key, api_secret_key, passphrase, use_server_time=False, flag='1')
    result = market_api.get_candlesticks(instId="BTC-USDT")
    print(result)
    k_data = result['data']
    data_columes = ['datetime', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'confirm']
    k_data['date'] = datetime.datetime.fromtimestamp(int(k_data['datetime'])).strftime('%Y-%m-%d')
    k_data = pd.DataFrame(data=k_data, columns=data_columes)
    k_data = k_data.set_index('datetime')
    k_data.to_csv(str('./test.csv'))

    result = k_data.to_sql(name='daily', con=conn, if_exists='append', index=False)
    if result is None:
        print(f"### 追加{symbol}历史数据失败:{result}")
    else:
        conn.commit()
        print(f"追加{symbol}历史数据记录:{result}条.")

    # 关闭数据库连接
    conn.close()

    return

if __name__ == '__main__':
    start()