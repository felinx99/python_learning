#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015-2023 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime
import os
import pandas as pd
import akshare as ak
import matplotlib.pyplot as plt

# 添加模块所在目录的绝对路径,然后导入模块正常了。否则报错(未安装backtrader) 
import sys
sys.path.append(r'E:\gitcode\backtrader')
import backtrader as bt

class teststrategy(bt.Strategy):
    def __init__(self):
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open
        self.datalow = self.datas[0].low
        self.datahigh = self.datas[0].high

        self.ma5 = bt.indicators.SMA(self.dataclose, period=5)
        self.ma10 = bt.indicators.SMA(self.dataclose, period=10)
        self.ma20 = bt.indicators.SMA(self.dataclose, period=20)
        self.MACD = bt.indicators.MACD(self.datas[0])
        self.macd = self.MACD.macd
        self.signal = self.MACD.signal
        self.rsi = bt.indicators.RSI(self.datas[0])
        self.boll = bt.indicators.BollingerBands(self.datas[0])
        self.atr = bt.indicators.ATR(self.datas[0])
        self.order = None
        self.buyprice = None
        self.comm = None

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))


if __name__ == '__main__':
    cerebro = bt.Cerebro()
    cerebro.broker.set_cash(1000000)
    cerebro.broker.setcommission(0.003)

    data = None
    cerebro.adddata(data)
    cerebro.addstrategy(teststrategy)
    cerebro.run(runonce=False)
    print('final value', cerebro.broker.getvalue())
    cerebro.plot(sytle='candle')
    plt.show()