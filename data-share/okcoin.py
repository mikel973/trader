import sqlite3
import datetime
import pandas as pd
import okx.MarketData as MarketData

def start(db_name, symbol, freq):

    api_key = ''
    api_secret_key = ''
    passphrase = ''
    # flag = "1"  # live trading: 0, demo trading: 1
    market_api = MarketData.MarketAPI(api_key, api_secret_key, passphrase, use_server_time=False, flag='1')
    result = market_api.get_candlesticks(instId="BTC-USDT", bar=freq)

    k_data = result['data']
    col_name = ['datetime', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'confirm']
    k_data = pd.DataFrame(data=k_data, columns=col_name)

    k_data['symbol'] = f"{symbol}-{freq}"
    k_data['date'] = pd.to_datetime(k_data['datetime'], unit='ms')
    k_data['date'] = k_data['date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    k_data['open'] = k_data['open'].astype(float)
    k_data['high'] = k_data['high'].astype(float)
    k_data['low'] = k_data['low'].astype(float)
    k_data['close'] = k_data['close'].astype(float)
    k_data = k_data.set_index('datetime')

    col_name = ['date', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'confirm','symbol']
    k_data = k_data.reindex(columns=col_name)

    k_data.to_csv(str('./test.csv'))

    conn = sqlite3.connect(db_name)
    result = k_data.to_sql(name='daily', con=conn, if_exists='replace', index=False)
    if result is None:
        print(f"### 追加{symbol}历史数据失败:{result}")
    else:
        conn.commit()
        print(f"追加{symbol}历史数据记录:{result}条.")
    # 关闭数据库连接
    conn.close()

    return

if __name__ == '__main__':
    db_name = './okex.db'
    symbol = 'BTC-USDT'
    freq = '15m'
    start(db_name, symbol, freq)
