# When setting the parameter "what='ASK'" the quoted price for Ask will be used from the incoming messages (field 2) instead of the default Bid price (field 1).

# BID: <tickPrice tickerId=16777217, field=1, price=1.11582, canAutoExecute=1>
# ASK: <tickPrice tickerId=16777219, field=2, price=1.11583, canAutoExecute=1>

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import testcommon
import backtrader as bt # type: ignore
import datetime


class St(bt.Strategy):
    def logdata(self):
        txt = []
        #txt.append('reqId: ' + '{}'.format(self.datas[0].reqId) + ' ')
        txt.append('{}'.format(len(self)))
        txt.append('{}'.format(self.data.datetime.datetime(0).isoformat()))
        txt.append(' open BID: ' + '{}'.format(self.datas[0].open[0]))
        txt.append(' high BID: ' + '{}'.format(self.datas[0].high[0]))
        txt.append(' low BID: ' + '{}'.format(self.datas[0].low[0]))
        txt.append(' close BID: ' + '{}'.format(self.datas[0].close[0]))
        txt.append(' volume: ' + '{:.2f}'.format(self.data.volume[0]))
        print(','.join(txt))

    data_live = False

    def notify_data(self, data, status, *args, **kwargs):
        print('*' * 5, 'DATA NOTIF:', data._getstatusname(status), *args)
        #if self.datas[0]._laststatus == self.datas[0].LIVE and self.datas[1]._laststatus == self.datas[1].LIVE:
        if self.datas[0]._laststatus == self.datas[0].LIVE:
            self.data_live = True

    # def notify_order(self, order):
    #     if order.status == order.Completed:
    #         buysell = 'BUY ' if order.isbuy() else 'SELL'
    #         txt = '{} {}@{}'.format(buysell, order.executed.size,
    #                                 order.executed.price)
    #         print(txt)

    # bought = 0
    # sold = 0

    def next(self):
        self.logdata()
        if not self.data_live:
            return

        # if not self.bought:
        #     self.bought = len(self)  # keep entry bar
        #     self.buy()
        # elif not self.sold:
        #     if len(self) == (self.bought + 3):
        #         self.sell()

#ib_symbol = 'INTC-STK-SMART-USD'     #stock symbol-type-exchange-currency
#ib_symbol = 'EUR.USD-CASH-IDEALPRO'     #forex pair-type-exchange
#ib_symbol = 'IBUS30-CFD-SMART-USD'       #cfd symbol-type-exchange-currency
#ib_symbol = 'ES-202503-CME'        #future symbol-expire-exchange-{currency}-{multi}
#ib_symbol = 'ES-FUT-CME-USD-202503' #future symbol-type-exchange-currency-expire-{multi}
#ib_symbol = 'DAX-FUT-EUREX-EUR-202503-5' #future symbol-type-exchange-currency-expire-{multi}
#ib_symbol = 'GBL-FOP-EUREX-EUR-20241129-1000'
#ib_symbol = 'SPY-20170721-2400-C-SMART'     #option
#ib_symbol = 'US03076KAA60-ISIN'     #bond
#ib_symbol = 'BTC-PAXOS-USD'     #crypto
ib_symbol = 'GOOG-STK-SMART'
compression = 5