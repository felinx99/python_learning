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
import datetime as dt

# 添加模块所在目录的绝对路径,然后导入模块正常了。否则报错(未安装backtrader) 
import sys
sys.path.append(r'E:\gitcode\backtrader')
import backtrader as bt

class teststrategy(bt.Strategy):
    def next(self):
        #金叉买入
        if self.macd[-1] < self.signal[-1]:
            if self.macd[0] > self.signal[0]:
                self.buy(size=500)
                if self.macd[0] < 0:  
                    self.log(f"MACD低位金叉买入,买入价{self.dataclose[0]}")
                elif self.macd[0] == 0:
                    self.log(f"MACD正常金叉买入,买入价{self.dataclose[0]}")
                else:
                    self.log(f"MACD高位金叉买入,买入价{self.dataclose[0]}")

        #高位死叉卖出
        if self.macd[-1] > self.signal[-1]:
            if self.macd[0] < self.signal[-1]:
                if self.macd[0] >= 0:
                    self.sell(size=500)
                    self.log(f"MACD高位死叉卖出,卖出价{self.dataclose[0]}")


    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(f"利润: {trade.pnl} 总利润: {trade.pnlcomm}")

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        
        if order.status in [order.Completed]:
            if order.issell():
                self.log(f'卖出价格:{order.executed.price},' 
                     f'资产: {order.executed.value}, 交易费用:{order.executed.comm}')
            if order.isbuy():
                self.log(f'买进价格:{order.executed.price}, '
                     f'资产: {order.executed.value}, 交易费用:{order.executed.comm}')
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
        elif order.status in []:
            self.log("交易取消，资金不足，交易搭接")

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open
        self.datalow = self.datas[0].low
        self.datahigh = self.datas[0].high

        self.MACD = bt.indicators.MACD(self.datas[0])
        self.macd = self.MACD.macd
        self.signal = self.MACD.signal

        self.order = None
        self.buyprice = None
        self.comm = None


    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

basepath = r'E:\data\CASH'

FROMDATE = dt.datetime(2020, 1, 1, 0, 0, 0)
TODATE = dt.datetime(2023, 1, 31, 23, 59, 59)

fut_symbol = {'name':'GPBUSD', 'symbol':"CASH-GBP-USD-IDEALPRO"}

if __name__ == '__main__':
    cerebro = bt.Cerebro()
    cerebro.broker.set_cash(1000000)
    cerebro.broker.setcommission(0.003)

    #生成文件路径
    fullpath = os.path.join(basepath, fut_symbol['name'], 'MIDPOINT')

    #生成文件名称
    filename = 'CASH_' + fut_symbol['name'] + '_4_hours_MIDPOINT' + '.csv'

    #生成文件完整路径+名称
    fullFileName = os.path.join(fullpath, filename)
    print(f"fullFileName: {fullFileName}")

    # Create the 1st data
    data = bt.feeds.IBCSVOnlyData(
        dataname=fullFileName,
        datainfo=fut_symbol['symbol'], #to create precontract
        fromdate=FROMDATE,
        todate=TODATE,
        timeframe=bt.TimeFrame.Days,
        compression=1,
        sessionstart=FROMDATE,  # internally just the "time" part will be used
        sessionend=TODATE,  # internally just the "time" part will be used
    )
    cerebro.adddata(data)
    cerebro.addstrategy(teststrategy)
    #添加观测器指标
    #cerebro.addobserver(bt.observers.Benchmark)
    #cerebro.addobserver(bt.observers.TimeReturn)
    #cerebro.addobserver(bt.observers.DrawDown)
    #cerebro.addobserver(bt.observers.FundValue)
    #cerebro.addobserver(bt.observers.FundShares)


    #添加分析指标
    comkwargs = dict(
        timeframe=bt.TimeFrame.Minutes, 
        compression=240,
    )
    shkwargs = dict(
        convertrate = True,
        annualize = True,
        riskfreerate = 0.04600, #无风险利率，以十年期美国国债收益率为准，要考虑时间周期，这是年化收益，如果按月计算需要折算成月无风险收益
    ) | comkwargs
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='AnnualReturn')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='Returns', **comkwargs)
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='TimeReturn', timeframe=bt.TimeFrame.NoTimeFrame)

    #cerebro.addanalyzer(bt.analyzers.SharpeRatio, riskfreerate=0.003, annualize=True, _name='SharpeRatio', timeframe=bt.TimeFrame.Years) #Years,Months,Weeks,Days, Minutes
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='SharpeRatio', **shkwargs)
    #cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='SharpeRatio_A' , riskfreerate = 0.04600, timeframe=bt.TimeFrame.Minutes, compression=240)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DrawDown')
    cerebro.addanalyzer(bt.analyzers.TimeDrawDown, _name='TimeDrawDown', **comkwargs)
    #cerebro.addanalyzer(bt.analyzers.Calmar, _name='Calmar', **comkwargs)
    cerebro.addanalyzer(bt.analyzers.GrossLeverage, _name='GrossLeverage')
    #cerebro.addanalyzer(bt.analyzers.LogReturnsRolling, _name='LogReturnsRolling', **comkwargs)
    cerebro.addanalyzer(bt.analyzers.PeriodStats, _name='PeriodStats', **comkwargs)
    #cerebro.addanalyzer(bt.analyzers.PositionsValue, _name='PositionsValue')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='SQN')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='TradeAnalyzer')
    #cerebro.addanalyzer(bt.analyzers.Transactions, _name='Transactions')
    cerebro.addanalyzer(bt.analyzers.VWR, _name='VWR', **comkwargs)
    cerebro.addanalyzer(bt.analyzers.Sortino, _name='Sortino', **comkwargs)

    
    strats = cerebro.run(runonce=False)
    print('final value', cerebro.broker.getvalue())
    # 第一个策略结果输出
    strat = strats[0]
    print("--------------- Analysis Report -----------------")
    print(f'总收益率(gross return ratio): {list(strat.analyzers.TimeReturn.get_analysis().values())[0]}')
    cagr = strat.analyzers.Returns.get_analysis()['rnorm100']
    print(f'复合年均增长率(CAGR Compound Annual Growth Rate): {cagr}')
    annualreturn = strat.analyzers.AnnualReturn.get_analysis()
    for year, ar in annualreturn.items():
        print(f"\t{year}年化收益率(annual return ratio): {ar}")
    print(f'夏普率(sharpe ratio): {strat.analyzers.SharpeRatio.get_analysis()['sharperatio']}')
    print(f'索提诺比率(sortino ratio): {strat.analyzers.Sortino.get_analysis()['sortinoratio']}')
    drawdown = strat.analyzers.DrawDown.get_analysis()
    print(f'最大回撤率(max drawdown ratio): {drawdown['max']['drawdown']}, '
          f'最大回撤周期(max drawdown period):{drawdown['max']['len']}, '
          f'最大回撤金额: {drawdown['max']['moneydown']}')
    print(f'可变权重收益(VWR Variability-Weighted Return):{list(strat.analyzers.VWR.get_analysis().values())[0]}')
    sqn = strat.analyzers.SQN.get_analysis()
    print(f"策略质量(sqn): {sqn['sqn']} {sqn['grade']}")
    tradeanalyzer = strat.analyzers.TradeAnalyzer.get_analysis()
    print(f'总交易次数(total trades): {tradeanalyzer['total']['total']}, '
          f'持仓(open): {tradeanalyzer['total']['open']}, '
          f'完成(closed): {tradeanalyzer['total']['closed']}')
    print(f'最大连胜次数(longest streak win): {tradeanalyzer['streak']['won']['longest']}, '
          f'最大连败次数(longest streak lost): {tradeanalyzer['streak']['lost']['longest']}')
    print(f'总收益(total gross pnl): {tradeanalyzer['pnl']['gross']['total']}, '
          f'净收益(total net pnl):{tradeanalyzer['pnl']['net']['total']}', 
          f'总平均收益(average gross pnl): {tradeanalyzer['pnl']['gross']['average']}, '
          f'净平均收益(average net pnl): {tradeanalyzer['pnl']['net']['average']}')
    print(f'\t--亏损(lost)----次数：{tradeanalyzer['lost']['total']}, '
          f'亏损总额(gross lost):{tradeanalyzer['lost']['pnl']['total']}, '
          f'平均亏损额(average lost):{tradeanalyzer['lost']['pnl']['average']}, '
          f'最大单次亏损(max lost):{tradeanalyzer['lost']['pnl']['max']}')
    print(f'\t--盈利(profit)--次数：{tradeanalyzer['won']['total']}, '
          f'盈利总额(gross profit):{tradeanalyzer['won']['pnl']['total']}, '
          f'平均盈利额(average profit):{tradeanalyzer['won']['pnl']['average']}, '
          f'最大单次盈利(max profit):{tradeanalyzer['won']['pnl']['max']}')
    print(f'\t--多单(long)----次数：{tradeanalyzer['long']['total']}, '
          f'盈利次数: {tradeanalyzer['long']['won']}, 亏损次数: {tradeanalyzer['long']['lost']}, '
          f'多单收益总额(gross pnl):{tradeanalyzer['long']['pnl']['total']}, '
          f'多单平均收益(average profit):{tradeanalyzer['long']['pnl']['average']},'
          f'多单最大单次亏损(max lost):{tradeanalyzer['long']['pnl']['lost']['max']}, '
          f'多单最大单次盈利(max profit):{tradeanalyzer['long']['pnl']['won']['max']}')
    print(f'\t--空单(short)---次数：{tradeanalyzer['short']['total']}, '
          f'盈利次数: {tradeanalyzer['short']['won']}, 亏损次数: {tradeanalyzer['short']['lost']}, '
          f'空单收益总额(gross pnl):{tradeanalyzer['short']['pnl']['total']}, '
          f'空单平均收益(average profit):{tradeanalyzer['short']['pnl']['average']}, '
          f'空单最大单次亏损(max lost):{tradeanalyzer['short']['pnl']['lost']['max']}, '
          f'空单最大单次盈利(max profit):{tradeanalyzer['short']['pnl']['won']['max']}')
    print("--------------- TradeAnalyzer len --------")
    print(f'len: {tradeanalyzer['len']}')
    print("--------------- TimeDrawDown -----------------")
    print(strat.analyzers.TimeDrawDown.get_analysis())
    print("--------------- PeriodStats ------------------")
    print(strat.analyzers.PeriodStats.get_analysis())    
    print("--------------- TimeDrawDown -----------------")
    print(strat.analyzers.TimeDrawDown.get_analysis())
    print("--------------- PeriodStats ------------------")
    print(strat.analyzers.PeriodStats.get_analysis())

    cerebro.plot(sytle='candle')
    plt.show()
    print('hello world')