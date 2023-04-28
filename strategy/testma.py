from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
import os.path  # To manage paths
import sys  # To find out the script name (in argv[0])
import backtrader as bt
from backtrader.utils import date2num
from datetime import date, datetime, time


class MyCSVData(bt.feed.CSVDataBase):
    def _loadline(self, linetokens):
        itoken = iter(linetokens)

        dttxt = next(itoken)  # Format is YYYY-MM-DD - skip char 4 and 7
        dt = date(int(dttxt[0:4]), int(dttxt[5:7]), int(dttxt[8:10]))

        tmtxt = next(itoken)  # Format if present HH:MM:SS, skip 3 and 6
        tm = time(int(tmtxt[0:2]), int(tmtxt[3:5]), int(tmtxt[6:8]))

        self.lines.datetime[0] = date2num(datetime.combine(dt, tm))
        self.lines.open[0] = float(next(itoken))
        self.lines.high[0] = float(next(itoken))
        self.lines.low[0] = float(next(itoken))
        self.lines.close[0] = float(next(itoken))
        self.lines.volume[0] = float(next(itoken))
        self.lines.openinterest[0] = float(0)

        return True


class DualMaStrategy(bt.Strategy):
    params = (
        ('printlog', False),
    )

    def log(self, txt, dt=None, doprint=False):
        ''' Logging function fot this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.datetime(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataClose = self.datas[0].close

    def notify_order(self, order):
        pass

    def notify_trade(self, trade):
        pass

    def next(self):
        # Simply log the closing price of the series from the reference
        self.log(' %.2f , %.2f , %.2f , %.2f ' % (self.data.lines.open[0], self.data.lines.high[0], self.data.lines.low[0], self.data.lines.close[0], ), doprint=True)

    def stop(self):
        pass


if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    strats = cerebro.addstrategy(DualMaStrategy)

    data = MyCSVData(dataname='../test-data/min60-sz.000001.csv', fromdate=datetime(2023, 4, 4))

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)
    #cerebro.resampledata(data, timeframe=bt.TimeFrame.Minutes)
    cerebro.resampledata(data, timeframe=bt.TimeFrame.Minutes, compression=15)

    # Set our desired cash start
    cerebro.broker.setcash(100000.0)

    # Add a FixedSize sizer according to the stake
    # cerebro.addsizer(bt.sizers.FixedSize, stake=10)

    # Set the commission
    cerebro.broker.setcommission(commission=0.0003)

    # Run over everything
    cerebro.run()

    cerebro.plot()