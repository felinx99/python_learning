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
import os
import shutil
import argparse
from datetime import date,datetime, timedelta
import time
import pytz
import pandas as pd
import json
from collections import Counter

import backtrader as bt


DATA_PATH = 'E:\\datas\\tdx\\1m_2025'
RESULT_PATH = 'E:\\output\\Astock\\stockpicking\\analysis\\tmp'


class VolumeLong(bt.Observer):
    alias = ('VolumeL',)
    lines = ('volumelong',)

    plotinfo = dict(plot=True, subplot=True)

    def next(self):
        self.lines.volumelong[0] = self.data_volume[0] if self.data_open[0] < self.data_close[0] else 0

class VolumeShort(bt.Observer):
    alias = ('VolumeS',)
    lines = ('volumeshort',)

    plotinfo = dict(plot=True, subplot=True)

    def next(self):
        self.lines.volumeshort[0] = self.data_volume[0] if self.data_open[0] > self.data_close[0] else 0


def add_data(cerebro=None, **kwargs):
    datatype = kwargs.get('datatype')
    
    # Get the dates from the args
    fromdate = datetime.strptime(kwargs.get('start'), '%Y%m%d')
    todate = datetime.strptime(kwargs.get('end'), '%Y%m%d')


    tickers = kwargs.get('tickers', None)
    plotreturns = kwargs.get('plotreturns', True)
    
    if datatype == 'file':
        # Set up data feed
        for ticker, listdate in tickers:
            
            tickerpath = os.path.join(DATA_PATH, f'{ticker}.csv')
            data = bt.feeds.stockCSVData(
                name = ticker,
                dataname=tickerpath,
                fromdate=fromdate,
                todate=todate,
                timeframe=bt.TimeFrame.Minutes,
                compression=1,
                reverse=False,
                adjclose=False,
                plot=plotreturns,
                tz=pytz.timezone('Asia/Shanghai')
            )
            #cerebro.adddata(data)
            cerebro.resampledata(
                        data,
                        name=ticker+'_30M',
                        timeframe=bt.TimeFrame.Minutes,
                        compression=30)


def runstrat(**kwargs):  
    # Create a cerebro entity
    cerebro = bt.Cerebro(stdstats=False)

    # Add a strategy
    cerebro.addstrategy(bt.Strategy)
    cerebro.broker.set_cash(100000)

    # Add the resample data instead of the original
    add_data(cerebro, **kwargs)

    cerebro.addobserver(VolumeLong)
    cerebro.addobserver(VolumeShort)

    # Add a simple moving average if requirested
    #cerebro.addindicator(bt.indicators.SMA, period=20)
    cerebro.addsizer(bt.sizers.FixedSize, stake=1)

    # Run over everything
    cerebro.run()

    cerebro.plot(style="candle")

    pass


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument('runmode', nargs=1)
    PARSER.add_argument('strategy', nargs=1)
    PARSER.add_argument('-t', '--tickers', nargs='+')
    PARSER.add_argument('-d', '--datatype', nargs=1)
    PARSER.add_argument('-x', '--exclude', nargs='+')
    PARSER.add_argument('-s', '--start', nargs=1)
    PARSER.add_argument('-e', '--end', nargs=1)
    PARSER.add_argument('--cash', nargs=1, type=int)
    PARSER.add_argument('-v', '--verbose', action='store_true')
    PARSER.add_argument('--plot', '-p', nargs='?', required=False,
                        metavar='kwargs', const='{}',
                        help=('Plot the read data applying any kwargs passed\n'
                              'For example:\n'
                              '  --plot style="candle" (to plot candles)\n'))
    PARSER.add_argument('--plotreturns', action='store_false')
    PARSER.add_argument('-k', '--kwargs', required=False, default='',
                        metavar='kwargs', 
                        help='kwargs in key=value format for strategy')

    ARGS = PARSER.parse_args()
    ARG_ITEMS = vars(ARGS)

    # Parse multiple tickers / kwargs
    TICKERS = ARG_ITEMS['tickers']
    KWARGS = ARG_ITEMS['kwargs']
    EXCLUDE = ARG_ITEMS['exclude']
    PLOT = ARG_ITEMS['plot']
    del ARG_ITEMS['tickers']
    del ARG_ITEMS['kwargs']
    del ARG_ITEMS['exclude']
    del ARG_ITEMS['plot']

    # Remove None values
    STRATEGY_ARGS = {k: (v[0] if isinstance(v, list) else v) for k, v in ARG_ITEMS.items() if v}
    if TICKERS:
        STRATEGY_ARGS['tickers'] = [ticker.strip() for ticker in TICKERS[0].split(',')]
    else:
        csvpath = os.path.dirname(RESULT_PATH)
        TICKER_CSV_PATH = os.path.join(os.path.dirname(csvpath), 'stocklist.csv')
        assert os.path.exists(TICKER_CSV_PATH), f"Error: '{TICKER_CSV_PATH}'"
        TICKERS_DF = pd.read_csv(TICKER_CSV_PATH, usecols=[0,5], skiprows=1, header=None) #read_csv返回的DF数据格式
        # 单列转换
        # TICKERS = TICKERS_DF.iloc[1:, 0].tolist()   #第0列，取第一列数据，并转为list格式
        # 多列转换为tuple列表
        TICKERS_records = TICKERS_DF.to_records(index=False)
        STRATEGY_ARGS['tickers'] = list(TICKERS_records)

    STRATEGY_ARGS['kwargs'] = eval('dict(' + KWARGS + ')')
    STRATEGY_ARGS['plot'] = eval('dict(' + PLOT + ')')

    if EXCLUDE:
        STRATEGY_ARGS['exclude'] = [EXCLUDE] if len(EXCLUDE) == 1 else EXCLUDE
        
    runstrat(**STRATEGY_ARGS)