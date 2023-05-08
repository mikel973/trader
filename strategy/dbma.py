from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import pandas as pd
#import pandas_datareader as pdr
import datetime
import sqlite3
import backtrader as bt
from backtrader.utils import date2num


class DualMaStrategy(bt.Strategy):
    params = (
        ('sPeriod', 7),
        ('mPeriod', 22),
        ('lPeriod', 41),
        ('printlog', False),
    )

    def log(self, txt, dt=None, doprint=False):
        ''' Logging function fot this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataClose = self.datas[0].close

        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyPrice = None
        self.buyComm = None
        self.size = 0

        # Add a MovingAverageSimple indicator
        self.sSma = bt.indicators.SimpleMovingAverage(self.datas[0], period=self.params.sPeriod)
        self.mSma = bt.indicators.SimpleMovingAverage(self.datas[0], period=self.params.mPeriod)
        self.lSma = bt.indicators.SimpleMovingAverage(self.datas[0], period=self.params.lPeriod)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyPrice = order.executed.price
                self.buyComm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):
        # Simply log the closing price of the series from the reference
        # self.log('Close, %.2f' % self.dataClose[0])

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Check if we are in the market
        if not self.position:

            # Not yet ... we MIGHT BUY if ...
            if self.mSma[0] > self.lSma[0]:

                # BUY, BUY, BUY!!! (with all possible default parameters)
                self.log('BUY CREATE, %.2f' % self.dataClose[0])

                # Keep track of the created order to avoid a 2nd order
                amount_to_invest = (0.95 * self.broker.cash)
                self.size = int(amount_to_invest / self.data.close)
                self.order = self.buy(size=self.size)

        else:

            if self.sSma[0] < self.mSma[0]:
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log('SELL CREATE, %.2f' % self.dataClose[0])

                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell(size=self.size)

    def stop(self):
        self.log('(MA Period %2d-%2d) Ending Value %.2f' %
                 (self.params.sPeriod, self.params.lPeriod, self.broker.getvalue()), doprint=True)

if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    # strats = cerebro.optstrategy(
    #     DualMaStrategy,
    #     sPeriod=range(10, 61))
    strats = cerebro.addstrategy(DualMaStrategy)

    # 读取数据
    database_name = '../test-data/stock.db'
    conn = sqlite3.connect(database_name)
    sql = "SELECT date, open, high, low, close, volume FROM daily WHERE symbol=? ORDER BY date ASC"
    symbol = '000001'
    datefram = pd.read_sql_query(sql, conn, params=[symbol])
    # 把 date 作为日期索引，以符合 Backtrader 的要求
    datefram.index = pd.to_datetime(datefram['date'])
    # datefram.drop('date', axis=1, inplace=True)
    # datefram['open'] = datefram['open'].astype(float)
    # datefram['high'] = datefram['high'].astype(float)
    # datefram['low'] = datefram['low'].astype(float)
    # datefram['close'] = datefram['close'].astype(float)
    # datefram['volume'] = datefram['volume'].astype(float)

    data = bt.feeds.PandasData(dataname=datefram)

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)

    # Set our desired cash start
    cerebro.broker.setcash(100000.0)

    # Add a FixedSize sizer according to the stake
    # cerebro.addsizer(bt.sizers.FixedSize, stake=10)

    # Set the commission
    cerebro.broker.setcommission(commission=0.0003)

    # Run over everything
    cerebro.run(maxcpus=2)

    #cerebro.plot()