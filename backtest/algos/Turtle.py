from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

import datetime
import os
import sys

import backtrader as bt
from ..util import models
from . import BaseStrategy as base

class Turtle(base.Strategy):
    params = {
        'average_period': 15,
        'short_period': 20,
        'long_period': 55,
    }
    average_period = 15
    total_trades = 0
    wins = 0
    losses = 0
    gross_profits = 0
    gross_losses = 0
    percent_profitable = 0
    profit_factor = 0

    high_20_count = 0
    high_55_count = 0

    low_20_count = 0
    low_55_count = 0

    will_short_close = False
    will_short_close_20 = False
    will_short_close_55 = False
    will_long_close = False
    will_long_close_20 = False
    will_long_close_55 = False

    def __init__(self):
        self.highs = []
        self.lows = []
        self.highs.append(models.LineMemory(20))
        self.highs.append(models.LineMemory(55))
        self.highs.append(models.LineMemory(10))
        self.lows.append(models.LineMemory(10))
        self.lows.append(models.LineMemory(20))
        self.lows.append(models.LineMemory(55))

        self.dataclose = self.datas[0].close
        self.order = None
        self.buyprice = None
        self.buycomm = None # again optional

        # Indicators
        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.average_period
        )

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY {}\tprice {:.2f}\tsize {:.2f}\tCost: {:.2f}\tComm: {:.2f}'.format(
                    order.data._name,
                    order.executed.price,
                    order.executed.size,
                    order.executed.value,
                    order.executed.comm))
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            elif order.issell():
                self.log('SELL {}\tprice {:.2f}\tsize{:.2f}\t Cost: {:.2f}\tComm: {:.2f}'.format(
                    order.data._name,
                    order.executed.price,
                    order.executed.size,
                    order.executed.value,
                    order.executed.comm))
            self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            status_reason = {
                order.Canceled: 'Canceled',
                order.Margin: 'Margin Called',
                order.Rejected: 'Rejected'
            }
            self.log('Order {}: {} {} Cash: {:.2f}, Order: {:.2f}'.format(
                status_reason[order.status],
                'BUY' if order.isbuy() else 'SELL',
                order.data._name,
                self.broker.get_cash(),
                (order.price or 0) * (order.size or 0)
            ))
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        if trade.pnl > 0:
            self.wins += 1
            self.gross_profits += trade.pnl
        else:
            self.losses += 1
            self.gross_losses -= trade.pnl
        self.percent_profitable = self.wins / self.total_trades
        self.profit_factor = self.gross_profits / self.gross_losses

        self.log("PROFIT, gross: {}, net: {}".format(trade.pnl, trade.pnlcomm))

    def next(self):
        self.log("Today's Open: {}, High: {}, Low: {}, Close: {}, ".format(
            self.data_open[0],
            self.data_high[0],
            self.data_low[0],
            self.dataclose[0],
        ))

        for item in self.highs:
            item.push(self.data_high[0]) # self.dataclose[0]
        for item in self.lows:
            item.push(self.data_low[0]) # self.dataclose[0]

        if (
            self.high_20_count + self.high_55_count > 2
            or self.high_20_count + self.high_55_count < 0
        ):
            print("SHIT bad number of long entries")
        if (
            self.low_20_count + self.low_55_count > 2
            or self.low_20_count + self.low_55_count < 0
        ):
            print("SHIT bad number of short entries")

        self.will_short_close = False
        self.will_short_close_20 = False
        self.will_short_close_55 = False
        self.will_long_close = False
        self.will_long_close_20 = False
        self.will_long_close_55 = False
        print(self.position.size)
        if self.order:
            return

        if len(self) > 20:
            if (
                self.high_20_count
                and not self.high_55_count
                and self.data_low[0] < self.lows[0].lowest
            ):
                self.will_long_close = True
                self.will_long_close_20 = True

            if (
                self.low_20_count
                and not self.low_55_count
                and self.data_high[0] > self.highs[2].highest
            ):
                self.will_short_close = True
                self.will_short_close_20 = True

            if (
                self.data_high[0] > self.highs[0].highest
                and not self.high_20_count
            ):
                self.high_20_count += 1
                self.log("LONG, {} 20 day entry".format(self.dataclose[0]))
                self.order = self.buy()

            if self.will_long_close_20:
                self.total_trades += 1
                self.high_20_count = 0
                self.high_55_count = 0
                self.log("LONG CLOSE, {} 10 day low".format(self.dataclose[0]))
                self.order = self.close()

            if len(self) > 55:
                if (
                    self.high_55_count
                    and self.dataclose[0] < self.lows[1].lowest
                ):
                    self.will_long_close = True
                    self.will_long_close_55 = True

                if (
                    self.low_55_count
                    and self.data_high[0] > self.highs[0].highest
                ):
                    self.will_short_close = True
                    self.will_short_close_55 = True

                if (
                    self.data_high[0] > self.highs[1].highest
                    and not self.high_55_count
                ):
                    if self.position.size < 0 and not self.will_short_close:
                        self.order = self.close()
                    self.high_55_count += 1
                    self.log("LONG, {} 55 day entry".format(self.dataclose[0]))
                    self.order = self.buy()

                if self.will_long_close_55:
                    self.total_trades += 1
                    self.high_20_count = 0
                    self.high_55_count = 0
                    self.log("LONG CLOSE, {} 20 day low".format(self.dataclose[0]))
                    self.order = self.close()

            if (
                self.data_low[0] < self.lows[1].lowest
                and not self.low_20_count
            ):
                self.low_20_count += 1
                self.log("SHORT, {} 20 day low".format(self.dataclose[0]))
                self.order = self.sell()

            if self.will_short_close_20:
                self.total_trades += 1
                self.low_20_count = 0
                self.low_55_count = 0
                self.log("SHORT CLOSE, {} 10 day high".format(self.dataclose[0]))
                self.order = self.close()
            if len(self) > 55:
                if (
                    self.data_low[0] < self.lows[2].lowest 
                    and not self.low_55_count
                ):
                    if self.position.size > 0 and not self.will_long_close:
                        self.order = self.close()
                    self.low_55_count += 1
                    self.log("SHORT, {} 55 day low".format(self.dataclose[0]))
                    self.order = self.sell()

                if self.will_short_close_55:
                    self.total_trades += 1
                    self.low_20_count = 0
                    self.low_55_count = 0
                    self.log("SHORT CLOSE, {} 20 day high".format(self.dataclose[0]))
                    self.order = self.close()

    def stop(self):
        print("Total Trades: {}".format(self.total_trades))
        print("Percent Profitable: {}".format(self.percent_profitable))
        print("Profit Factor: {}".format(self.profit_factor))
        super().stop()