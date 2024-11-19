# When setting the parameter "what='ASK'" the quoted price for Ask will be used from the incoming messages (field 2) instead of the default Bid price (field 1).

# BID: <tickPrice tickerId=16777217, field=1, price=1.11582, canAutoExecute=1>
# ASK: <tickPrice tickerId=16777219, field=2, price=1.11583, canAutoExecute=1>

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import testcommon
import backtrader as bt # type: ignore
import datetime


class St(bt.Strategy):
    params = (
        ('ma_fast_period', 10),
        ('ma_slow_period', 50)     
    )

    def log(self, inputtxt=None, ts=None, onlydata=False):
        ''' Logging function for this strategy '''
        if onlydata is True:
            # log data
            txt = []
            txt.append('{}'.format(len(self)))
            txt.append('{}'.format(self.data.datetime.datetime(0).isoformat()))
            txt.append(' open BID: ' + '{}'.format(self.datas[0].open[0]))
            txt.append(' high BID: ' + '{}'.format(self.datas[0].high[0]))
            txt.append(' low BID: ' + '{}'.format(self.datas[0].low[0]))
            txt.append(' close BID: ' + '{}'.format(self.datas[0].close[0]))
            txt.append(' volume: ' + '{:.2f}'.format(self.data.volume[0]))
            print(','.join(txt))
        else:
            # log output
            ts = ts or self.datas[0].datetime.datetime(0)
            print(f'{ts}, {inputtxt}')


    def __init__(self):
        #keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close

        #To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None
        
        #add a MovingAverageSimple indicator
        my_fast = bt.indicators.SMA(period=self.params.ma_fast_period)
        my_slow = bt.indicators.SMA(period=self.params.ma_slow_period)
        self.crossover = bt.indicators.CrossOver(my_fast, my_slow)


    def notify_data(self, data, status, *args, **kwargs):
        print('*' * 5, 'DATA NOTIF:', data._getstatusname(status), *args)
        #if self.datas[0]._laststatus == self.datas[0].LIVE and self.datas[1]._laststatus == self.datas[1].LIVE:
        if self.datas[0]._laststatus == self.datas[0].LIVE:
            self.data_live = True

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return
        
        # check if an order has been complete
        # Attention:broker could reject order if not enough cash
        if order.status == order.Completed:
            if order.isbuy():
                self.log(f'BUY EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}')

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log(f'SELL EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}')
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log(f'OPERATION PROFIT, GROSS: {trade.pnl:.2f}, NET: {trade.pnlcomm:.2f}')

    def next(self):   
        # Simply log the closing price of the series from the reference
        self.log(onlydata=True)
        
        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Check if we are in the market
        if not self.position:
            # Not yet ... we MIGHT BUY if ...
            if self.crossover > 0:
                # BUY, BUY, BUY!!! (with all possible default parameters)
                self.log(f'BUY CREATE  @MKT: {self.dataclose[0]:.2f}')

                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()
                self.log(f'BUY CREATED Size: {self.order.size} @ MKT')

        else:
            if self.crossover < 0:
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log(f'SELL CREATE @ MKT: {self.dataclose[0]:.2f}')

                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell()
                self.log(f'SELL CREATED Size: {self.order.size} @ MKT')
