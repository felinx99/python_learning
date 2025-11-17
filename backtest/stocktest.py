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


#DATA_PATH = 'E:\\datas\\tdx\\1m_2025'
DATA_PATH = 'E:\\datas\\tdx\\day_2018_2025'
RESULT_PATH = 'E:\\output\\Astock\\stockpicking\\analysis\\tmp'
VOLATILITY_PCT = 0.13  # 波动性百分比阈值。用小数表示百分比。
BOX_PERIOD = 45  # 计算波动性的周期长度，单位为交易日。

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

class VolumeRatio(bt.Observer):
    alias = ('VolumeRatio',)
    lines = ('VolumeRatio',)

    plotinfo = dict(plot=True, subplot=True)

    def next(self):
        self.lines.VolumeRatio[0] = self.data_volume[0] > self.data_volume[-1]

class VolumeRatio(bt.Indicator):
    alias = ('VolumeRatio',)
    lines = ('VolumeRatio',)

    plotinfo = dict(plot=True, subplot=True)

    def __init__(self):
        self.lines.VolumeRatio = self.data / self.data(-1)
        super(VolumeRatio, self).__init__()


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
                timeframe=bt.TimeFrame.Days,
                compression=1,
                reverse=False,
                adjclose=False,
                plot=plotreturns,
                tz=pytz.timezone('Asia/Shanghai')
            )
            cerebro.adddata(data)
            #cerebro.resampledata(
            #            data,
            ##            name=ticker+'_30M',
            #            timeframe=bt.TimeFrame.Minutes,
            #            compression=30)

def judge_uptrend(self, dtstr='', spot_list=[]):
    #combo_file={'1-combo':set(), '2-combo':set(), '3-combo':set(), '4-combo':set(), '5-combo':set()}
    
    # process newone
    self.spot_filepath = os.path.join(RESULT_PATH, f'spot_{dtstr}.txt')
    with open(self.spot_filepath, 'w') as f:
        f.write('\n'.join(spot_list))
    
    self.spot_filelist.append(f'spot_{dtstr}.txt')

    # make diff between newone and previous
    # self.diff_filepath = os.path.join(RESULT_PATH, f'diff_{dtstr}.txt')
    oldset = set()
    newset = set(spot_list)
    #combo_file['1-combo'] = newset
    combo_file = newset

    if len(self.spot_filelist) >= 9:   #1连             
        with open(os.path.join(RESULT_PATH, self.spot_filelist[-2]), 'r') as f1:
            oldset = set(f1.read().splitlines())
        #deal with combo
        combo_file = oldset & combo_file

        with open(os.path.join(RESULT_PATH, self.spot_filelist[-3]), 'r') as f1:
            oldset = set(f1.read().splitlines())
        combo_file = oldset & combo_file
    
        with open(os.path.join(RESULT_PATH, self.spot_filelist[-4]), 'r') as f1:
            oldset = set(f1.read().splitlines())
        combo_file = oldset & combo_file

        with open(os.path.join(RESULT_PATH, self.spot_filelist[-5]), 'r') as f1:
            oldset = set(f1.read().splitlines())
        combo_file = oldset & combo_file

        with open(os.path.join(RESULT_PATH, self.spot_filelist[-6]), 'r') as f1:
            oldset = set(f1.read().splitlines())
        combo_file = oldset & combo_file

        with open(os.path.join(RESULT_PATH, self.spot_filelist[-7]), 'r') as f1:
            oldset = set(f1.read().splitlines())
        combo_file = oldset & combo_file

        with open(os.path.join(RESULT_PATH, self.spot_filelist[-8]), 'r') as f1:
            oldset = set(f1.read().splitlines())
        combo_file = oldset & combo_file

        with open(os.path.join(RESULT_PATH, self.spot_filelist[-9]), 'r') as f1:
            oldset = set(f1.read().splitlines())
        combo_file = combo_file - oldset

        # save combo file
        if combo_file:
            self.combo_filepath = os.path.join(RESULT_PATH, f'combo_{dtstr}.txt')

            combo_list = list(combo_file)
            try:
                with open(self.combo_filepath, 'w', encoding='utf-8-sig') as f:
                    json.dump(combo_list, f, indent=4)
            except Exception as e:
                print(f"save combo file error: {e}")

    '''
    if combo_file:
        self.combo_filepath = os.path.join(RESULT_PATH, f'combo_{dtstr}.txt')
        self.combo_filelist.append(f'combo_{dtstr}.txt')

        target_keys = ['5-combo', '4-combo', '3-combo']
        combo_list = {key: list(combo_file[key]) for key in target_keys}

        if len(self.combo_filelist) >= 2:
            try:
                with open(os.path.join(RESULT_PATH, self.combo_filelist[-2]), 'r', encoding='utf-8-sig') as f1:
                    oldcombo = json.load(f1)
            except Exception as e:
                print(f"read combo file error: {e}")              

            #deal with combo
            old5comboset = set(oldcombo['5-combo'])
            new5comboset = combo_file['5-combo']

            #deal with diff
            added5combo = new5comboset - old5comboset
            if added5combo:
                combo_list['new-5-combo'] = list(added5combo)

        try:
            with open(self.combo_filepath, 'w', encoding='utf-8-sig') as f:
                json.dump(combo_list, f, indent=4)
        except Exception as e:
            print(f"save combo file error: {e}")
    '''


def judge_uptrend_most(self):
    spotUnionList = []
    for idx, spotfile in enumerate(self.spot_filelist):
        idx_r = -(idx+1)
        with open(os.path.join(RESULT_PATH, self.spot_filelist[idx_r]), 'r') as fr:
                curList = fr.read().splitlines()
        if idx == 0:
            spotUnionList = curList
        else:
            spotUnionList = spotUnionList + curList
            spotUnion_counts = Counter(spotUnionList)

        if idx==20:
            #保存最近1个月的连涨股票                
            spotUnion20cur_sorted = spotUnion_counts.most_common()
            #spotUnion_sorted = [(element, count) for element, count in spotUnion_counts.most_common() if count > 10]
            spotUnion20cur_save = dict(spotUnion20cur_sorted)
            spotUnion_fname = os.path.join(RESULT_PATH, f'most20_cur.txt')
            with open(spotUnion_fname, 'w', encoding='utf-8-sig') as fw:
                json.dump(spotUnion20cur_save, fw, indent=4)
            #为统计最近1个月数据做准备
            spotUnion20cur_counter = Counter(spotUnion20cur_save)

            up_fname = os.path.join(os.path.dirname(RESULT_PATH), f'most20_cur.txt')
            shutil.copy(spotUnion_fname, up_fname)

        if idx==40:
            spotUnion_sorted = spotUnion_counts.most_common()
            #保存最近2个月连涨股票               
            spotUnion_save = dict(spotUnion_sorted)
            spotUnion_fname = os.path.join(RESULT_PATH, f'most40.txt')

            with open(spotUnion_fname, 'w', encoding='utf-8-sig') as fw:
                json.dump(spotUnion_save, fw, indent=4)

            up_fname = os.path.join(os.path.dirname(RESULT_PATH), f'most40.txt')
            shutil.copy(spotUnion_fname, up_fname)

        if idx == 21:
            spotUnion20List = curList
        elif idx > 21 and idx <= 41:
            spotUnion20List = spotUnion20List + curList
            spotUnion20_counts = Counter(spotUnion20List)

        if idx==41:
            #保存上1个月的连涨股票                
            spotUnion20pre_sorted = spotUnion20_counts.most_common()
            #spotUnion_sorted = [(element, count) for element, count in spotUnion_counts.most_common() if count > 10]
            #spotUnion_counter20 = Counter(dict(spotUnion_sorted))
            spotUnion20pre_save = dict(spotUnion20pre_sorted)
            spotUnion_fname = os.path.join(RESULT_PATH, f'most20_pre.txt')
            with open(spotUnion_fname, 'w', encoding='utf-8-sig') as fw:
                json.dump(spotUnion20pre_save, fw, indent=4)
            #保存最近1个月新出现的连涨股票
            spotUnion20_common = spotUnion20cur_save.keys() & spotUnion20pre_save.keys()

            # 2. 遍历共同元素，并应用筛选条件
            result = {}
            for element in spotUnion20_common:
                count_a = spotUnion20cur_save[element]
                count_b = spotUnion20pre_save[element]
            
                if count_a - count_b > 8:
                    result[element] = count_a-count_b

            result_counter = Counter(result)
            result_sorted = result_counter.most_common()
            result_dict = dict(result_sorted)
            spotUnion_new_fname = os.path.join(RESULT_PATH, f'most20_new.txt')
            with open(spotUnion_new_fname, 'w', encoding='utf-8-sig') as fw:
                json.dump(result_dict, fw, indent=4)

            up_fname = os.path.join(os.path.dirname(RESULT_PATH), f'most20_new.txt')
            shutil.copy(spotUnion_new_fname, up_fname)

        if idx > 41:
            break
#mode说明：0-初始设置，1-重新设置，2-继续但超限, 3-继续且未超限

def set_box(box=None, hl_tuple=(), date='', mode=0):
    if mode == 0:
        box['consolidaDays'] = 0
        box['ThHit'] = 0
    elif mode == 1:
        box['consolidaDays'] = 1
        box['ThHit'] = 0
        box['startdate'] =date
    elif mode == 2:
        box['consolidaDays'] += 1
        box['ThHit'] += 1
    elif mode == 3:
        box['consolidaDays'] += 1
        box['ThHit'] = 0
    
    curhigh, curlow = hl_tuple
    curMid = (curhigh - curlow)/2 + curlow

    box['boxHigh'] = curhigh
    box['boxLow'] = curlow
    box['boxWidth'] = curhigh - curlow
    box['boxMid'] = curMid
    box['volatilityRatio'] = (curhigh - curMid)/curMid
    box['enddate'] = date


def judge_boxbreakout(self, breakt=(), dtstr='', d=None, mid=0):
    breakuplist, breakdownlist = breakt

    curhigh = d.lines.high[0]
    curlow = d.lines.low[0]
    curMid = (curhigh + curlow)/2

    if d.dynamicbox['consolidaDays'] == 0:
        if curhigh > mid*(1+VOLATILITY_PCT) or curlow < mid*(1-VOLATILITY_PCT):
            pass
        else:
            set_box(box=d.dynamicbox, hl_tuple=(curhigh, curlow), date=dtstr, mode=1)
    elif d.dynamicbox['consolidaDays'] < BOX_PERIOD/2.2:
        #BOX生成部分：继续生成或重新生成
        newboxh = max(d.dynamicbox['boxHigh'], curhigh)
        newboxl = min(d.dynamicbox['boxLow'], curlow)
        newboxm = (newboxh + newboxl)/2
        newVolatility = (newboxh - newboxm)/newboxm

        if d.dynamicbox['ThHit'] > 1:
                #step2: save box data
                #待补充相关代码
                #step3: reset box
                set_box(box=d.dynamicbox, hl_tuple=(curhigh, curlow), date=dtstr, mode=1)
                return
        else:
            if curhigh > mid*(1+VOLATILITY_PCT) or curlow < mid*(1-VOLATILITY_PCT): 
                if curhigh > mid*(1+VOLATILITY_PCT):
                    newboxh = mid*(1+VOLATILITY_PCT)
                if curlow < mid*(1-VOLATILITY_PCT):
                    newboxl = mid*(1-VOLATILITY_PCT)
                
                set_box(box=d.dynamicbox, hl_tuple=(newboxh, newboxl), date=dtstr, mode=2)
            else:
                set_box(box=d.dynamicbox, hl_tuple=(newboxh, newboxl), date=dtstr, mode=3)
    else:
        record = {'stock':'', 'box':{}}
        #突破部分
        if d.lines.close[0] > d.dynamicbox['boxHigh'] and curhigh > d.dynamicbox['boxMid']*1.1 :
            #step1: Notify up breakout, 待补充相关代码and curhigh > d.dynamicbox['boxMid']*(1+VOLATILITY_PCT)
            #step2: save box data
            #step3: reset box       
            if d.dynamicbox['consolidaDays'] > BOX_PERIOD:
                record['stock'] = d._name
                record['box'] = d.dynamicbox.copy()
                breakuplist.append(record)
            
            set_box(box=d.dynamicbox, hl_tuple=(curhigh, curlow), date=dtstr, mode=1)
            return

        #双向操作时就考虑向下突破情况，单向操作时扩大box继续
        if d.lines.close[0] < d.dynamicbox['boxLow'] and curlow < d.dynamicbox['boxMid']*0.9:    
            #step1: Notify down breakout
            #step2: save box data
            #step3: reset box
            record['box'] = d.dynamicbox.copy()
     
            set_box(box=d.dynamicbox, hl_tuple=(curhigh, curlow), date=dtstr, mode=1)
            return

        #BOX生成部分：继续生成或重新生成
        newboxh = max(d.dynamicbox['boxHigh'], curhigh)
        newboxl = min(d.dynamicbox['boxLow'], curlow)
        set_box(box=d.dynamicbox, hl_tuple=(newboxh, newboxl), date=dtstr, mode=3)

        '''
        #BOX生成部分：继续生成或重新生成
        newboxh = max(d.dynamicbox['boxHigh'], curhigh)
        newboxl = min(d.dynamicbox['boxLow'], curlow)
        newboxm = (newboxh + newboxl)/2
        newVolatility = (newboxh - newboxm)/newboxm

        if newVolatility > VOLATILITY_PCT:            
            if d.dynamicbox['ThHit'] > 1:
                #step2: save box data
                    #待补充相关代码

                #step3: reset box
                set_box(box=d.dynamicbox, hl_tuple=(curhigh, curlow), date=dtstr, mode=1)
                return
            else:
                thup = newboxm*(1 + VOLATILITY_PCT)
                thdown = newboxm*(1 - VOLATILITY_PCT)
                set_box(box=d.dynamicbox, hl_tuple=(thup, thdown), date=dtstr, mode=2)
        else:
            set_box(box=d.dynamicbox, hl_tuple=(newboxh, newboxl), date=dtstr, mode=3)
        '''

        
     
 
class SmaCross(bt.SignalStrategy):
    params = dict(sma5=5, sma10=10, sma20=20, sma30=30, sma60=60)

    def notify_order(self, order):
        if not order.alive():
            print('{} {} {}@{}'.format(
                bt.num2date(order.executed.dt),
                'buy' if order.isbuy() else 'sell',
                order.executed.size,
                order.executed.price)
            )

    def notify_trade(self, trade):
        if trade.isclosed:
            print('profit {}'.format(trade.pnlcomm))

    def __init__(self):
        self.spot_filelist = []
        self.spot_filepath = ''
        self.combo_filepath = ''
        self.box_fname = '' 
        self.vratio_fname = ''
        
        # Define Indicators
        for i, d in enumerate(self.datas):
            d.dynamicbox = {'consolidaDays':0, 'boxHigh':0.0, 'boxLow':0.0,\
                            'volatilityRatio':0.18, 'startdate':None, 'enddate':None,  \
                            'boxWidth':0.0, 'boxMid':0.0, 'ThHit':0}
            d.sma5 = bt.ind.SMA(d.lines.close, period=self.params.sma5)
            d.sma10 = bt.ind.SMA(d.lines.close, period=self.params.sma10)
            d.sma20 = bt.ind.SMA(d.lines.close, period=self.params.sma20)
            d.sma30 = bt.ind.SMA(d.lines.close, period=self.params.sma30)
            d.sma60 = bt.ind.SMA(d.lines.close, period=self.params.sma60)
            d.vratio = VolumeRatio(d.lines.volume)

            """
            d.crossover5_10 = 1*bt.ind.CrossOver(d.sma5, d.sma10)
            d.crossover5_20 = 2*bt.ind.CrossOver(d.sma5, d.sma20)
            d.crossover5_30 = 4*bt.ind.CrossOver(d.sma5, d.sma30)
            d.crossover5_60 = 8*bt.ind.CrossOver(d.sma5, d.sma60)
            d.crossover = d.crossover5_10 + d.crossover5_20 + d.crossover5_30 + d.crossover5_60
            self.signal_add(bt.SIGNAL_LONG, d.crossover5_10)
            self.signal_add(bt.SIGNAL_LONG, d.crossover5_20)
            self.signal_add(bt.SIGNAL_LONG, d.crossover5_30)
            self.signal_add(bt.SIGNAL_LONG, d.crossover5_60)
            self.signal_add(bt.SIGNAL_LONG, d.crossover)
            """
    def next(self):
        dtstr = ''
        spot_list = []
        boxbreakup = []
        boxbreakdown = []
        vratio_list = []

        
        for idx, d in enumerate(self.datas):
            idx += 1
            dtstr = bt.num2date(d.lines.datetime[0], tz=d._tz).strftime('%Y-%m-%d')
            if idx == len(self.datas): #len(self.datas):
                print(f'deal process -- {idx}/{len(self.datas)} -- {d._name} -- {dtstr}')
            if d.vratio > 4.5 and d.lienes.close[0] > d.lines.open[0] and 'st' not in d._name.lower():
                vratio_list.append(d._name)
            if d.sma5[0] > d.sma10[0] and d.sma10[0] > d.sma20[0] and d.sma20[0] > d.sma30[0] and d.sma30[0] > d.sma60[0]:
                spot_list.append(d._name)
            judge_boxbreakout(self, breakt=(boxbreakup, boxbreakdown), dtstr=dtstr, d=d, mid=d.sma60[0])

        if spot_list:
            judge_uptrend(self, dtstr=dtstr, spot_list=spot_list)
        
        if vratio_list:
            self.vratio_fname = os.path.join(RESULT_PATH, f'vratio_{dtstr}.txt')
            with open(self.vratio_fname, 'w') as fw:
                fw.write('\n'.join(vratio_list))


        if boxbreakup:
            sortedbox = sorted(boxbreakup, key=lambda x: x['box']['consolidaDays'], reverse=True)
            sortedbox_list = [(item ['stock'], item['box']['consolidaDays'], item['box']['startdate'], item['box']['boxHigh'], item['box']['boxLow']) for item  in sortedbox]
            self.box_fname = os.path.join(RESULT_PATH, f'boxup_{dtstr}.txt')
            with open(self.box_fname, 'w') as f:
                for item in sortedbox_list:
                    f.write(str(item) + '\n')
        

        
            

    def stop(self):
        judge_uptrend_most(self)

        #拷贝最新box策略结果 
        if self.box_fname:
            cdir = os.path.dirname(self.box_fname) 
            cdir_p = os.path.dirname(cdir)
            fname = os.path.basename(self.box_fname) # 获取文件名 'a.txt'
            d_fpath = os.path.join(cdir_p, fname) 
            shutil.copy(self.box_fname, d_fpath)
        #拷贝最新volume策略结果
        if self.vratio_fname:
            fname = os.path.basename(self.vratio_fname)
            d_fpath = os.path.join(cdir_p, fname)
            shutil.copy(self.vratio_fname, d_fpath)
        #拷贝最新的combo_uptrend策略结果
        if self.combo_filepath:
            fname = os.path.basename(self.combo_filepath)
            d_fpath = os.path.join(cdir_p, fname)
            shutil.copy(self.combo_filepath, d_fpath)
        
        print('-------------     finished      --------------')
        print(f'total process: {len(self.datas)} datas')

def runstrat(**kwargs):  
    # Create a cerebro entity
    cerebro = bt.Cerebro(stdstats=False)

    # Add a strategy
    #cerebro.addstrategy(bt.Strategy)
    cerebro.addstrategy(SmaCross)
    cerebro.broker.set_cash(100000)

    # Add the resample data instead of the original
    add_data(cerebro, **kwargs)

    cerebro.addobserver(VolumeLong)
    cerebro.addobserver(VolumeShort)

    # Add a simple moving average if requirested
    cerebro.addindicator(bt.indicators.SMA, period=20)
    cerebro.addsizer(bt.sizers.FixedSize, stake=1)

    # Run over everything
    cerebro.run()

    #cerebro.plot(style="candle")

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