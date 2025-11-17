import datetime
import os.path
import argparse
import importlib
import dateutil.parser
import time
import pandas as pd
import numpy as np

# Import the backtrader platform
import backtrader as bt
from backtrader import TimeFrame
from .util import observers
from .util import universe as universe_util
from .util import cerebor

mircofut_real_symbols = {
    'EURUSD':"FUT-M6E-USD-CME-20250317-12500-False",
    'GBPUSD':"FUT-M6B-USD-CME-20250317-6250-False",
    'AUDUSD':"FUT-M6A-USD-CME-20250317-10000-False",
    'CHFUSD':"FUT-MSF-USD-CME-20250317-12500-False",
    'USDCAD':"FUT-MCD-USD-CME-20250318-10000-False",
    'USDJPY':"FUT-MJY-USD-CME-20250317-1250000-False",
}

cash_hist_symbols = {
    'GBPUSD':"CASH-GBP-USD-IDEALPRO",
    'AUDUSD':"CASH-AUD-USD-IDEALPRO",
    'USDCAD':"CASH-USD-CAD-IDEALPRO",
    'CHFUSD':"CASH-CHF-USD-IDEALPRO",
    'EURUSD':"CASH-EUR-USD-IDEALPRO",
    'USDJPY':"CASH-USD-JPY-IDEALPRO",   
}

def get_filepath(ticker):
    basepath = os.path.join(os.path.dirname(__file__), '../data/cash/')
    #生成文件路径
    data_path = os.path.join(basepath, ticker, 'MIDPOINT')
    #生成文件名称
    filename = 'CASH_' + ticker + '_4_hours_MIDPOINT' + '.csv'
    #生成文件完整路径+名称
    fullFileName = os.path.join(data_path, filename)

    return fullFileName

def clean_tickers(tickers, start, end):
    #data_path = os.path.join(os.path.dirname(__file__), '../data/price/')   
    out_tickers = []

    for ticker in tickers:       
        d = pd.read_csv(get_filepath(ticker), index_col=0, parse_dates=True)
        start_time = start
        end_time = end

        tailtime = d.tail(1).index[0]
        headtime = d.head(1).index[0]

        tzinfo = tailtime.tz
        if tzinfo is not None:
            start_time = start.replace(tzinfo=tzinfo)
            end_time = end.replace(tzinfo=tzinfo)


        if not (tailtime < start_time or
                headtime > end_time):
            out_tickers.append(ticker)
        else:
            print('Data out of date range:', ticker)

    return out_tickers

def add_analyzers(cerebro=None, **kwargs):
    # Add analyzers
    riskfreerate = kwargs.get('riskfreerate', 0.035)
    plotreturns = kwargs.get('plotreturns', False)

    comkwargs = dict(
        timeframe=bt.TimeFrame.Days, 
        compression=1,
    )
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='SharpeRatio',
                        riskfreerate=riskfreerate,
                        annualize=True,
                        **comkwargs)
    cerebro.addanalyzer(bt.analyzers.Sortino, _name='Sortino',
                        riskfreerate=riskfreerate,
                        annualize=True,
                        **comkwargs)
    cerebro.addanalyzer(bt.analyzers.Returns, _name='Returns', **comkwargs)
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='AnnualReturn')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='TimeReturn', timeframe=bt.TimeFrame.NoTimeFrame)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='LatinDance')
    cerebro.addanalyzer(bt.analyzers.PositionsValue, _name='PositionsValue')
    cerebro.addanalyzer(bt.analyzers.GrossLeverage, _name='GrossLeverage')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='SQN')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='TradeAnalyzer')
    cerebro.addanalyzer(bt.analyzers.VWR, _name='VWR', **comkwargs)

        #添加观测器指标
    if plotreturns:
        cerebro.addobserver(observers.Value)    #自定义观测器
        cerebro.addobserver(bt.observers.Benchmark)
        cerebro.addobserver(bt.observers.TimeReturn)
        cerebro.addobserver(bt.observers.DrawDown)
        cerebro.addobserver(bt.observers.FundValue)
        cerebro.addobserver(bt.observers.FundShares)


def show_analyzers_reslut(results=[], start_value=0, end_value=0):
    # Get analysis results
    drawdown = results[0].analyzers.LatinDance.get_analysis()
    cagr = results[0].analyzers.Returns.get_analysis()['rnorm100']
    ar = results[0].analyzers.AnnualReturn.get_analysis()
    gagr = list(results[0].analyzers.TimeReturn.get_analysis().values())[0]
    sharpe = results[0].analyzers.SharpeRatio.get_analysis()['sharperatio']
    sortino = results[0].analyzers.Sortino.get_analysis()['sortinoratio']
    positions = results[0].analyzers.PositionsValue.get_analysis()
    sqn = results[0].analyzers.SQN.get_analysis()
    vwr = list(results[0].analyzers.VWR.get_analysis().values())[0]
    tradeanalyzer = results[0].analyzers.TradeAnalyzer.get_analysis()
    avg_positions = np.mean([sum(d != 0.0 for d in i) for i in positions.values()])
    leverage = results[0].analyzers.GrossLeverage.get_analysis()
    avg_leverage = np.mean([abs(i) for i in leverage.values()])

    sharpe = 'None' if sharpe is None else round(sharpe, 5)
    print("--------------- 结果输出 -----------------")
    print('Starting Portfolio Value:\t{:.2f}'.format(start_value))
    print('Final Portfolio Value:\t\t{:.2f}'.format(end_value))
    print('ROI:\t\t{:.2f}%'.format(100.0 * ((end_value / start_value) - 1.0)))
    analyzer_results = []
    analyzer_results.append('CAGR:\t\t{:.2f}'.format(cagr))
    analyzer_results.append('Max Drawdown:\t{:.2f}'.format(drawdown['max']['drawdown']))
    analyzer_results.append('Sharpe:\t\t{}'.format(sharpe))
    analyzer_results.append('Sortino:\t{:.5f}'.format(sortino))
    analyzer_results.append('Positions:\t{:.5f}'.format(avg_positions))
    analyzer_results.append('Leverage:\t{:.5f}'.format(avg_leverage))
    print('\n'.join(analyzer_results))


    print(f'投资回报率(ROI): {100*gagr:.2f}%')
    print(f'复合年均增长率(CAGR Compound Annual Growth Rate): {cagr:.2f}%')
    for year, var in ar.items():
        print(f"\t{year}年化收益率(annual return ratio): {100*var:.2f}%")
    print(f'夏普率(sharpe ratio): {sharpe}')
    print(f'索提诺比率(sortino ratio): {sortino:.5f}')
    print(f'最大回撤率(max drawdown ratio): {drawdown['max']['drawdown']:.2f}, '
          f'最大回撤周期(max drawdown period):{drawdown['max']['len']}, '
          f'最大回撤金额: {drawdown['max']['moneydown']}')
    print(f'可变权重收益(VWR Variability-Weighted Return):{vwr}')
    print(f"策略(sqn) 得分{sqn['sqn']} 质量:{sqn['grade']}")
    print(f'总交易次数(total trades): {tradeanalyzer['total']['total']}, '
          f'持仓(open): {tradeanalyzer['total']['open']}, '
          f'完成(closed): {tradeanalyzer['total']['closed']}')
    print(f'最大连胜次数(longest streak win): {tradeanalyzer['streak']['won']['longest']}, '
          f'最大连败次数(longest streak lost): {tradeanalyzer['streak']['lost']['longest']}')
    print(f'净收益(total net pnl):{tradeanalyzer['pnl']['net']['total']}', 
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
    
def adddata(cerebro=None, **kwargs):
    datatype = kwargs.get('datatype')
    start = kwargs.get('start')
    end = kwargs.get('end')
    tickers = kwargs.get('tickers', None)
    universe = kwargs.get('universe', None)
    exclude = kwargs.get('exclude', None)
    plotreturns = kwargs.get('plotreturns', True)
    
    tickers = tickers if (tickers or universe) else ['SPY']
    if universe:
        u = universe_util.get(universe)()
        tickers = [a for a in u.assets if a not in exclude]

    start_date = dateutil.parser.isoparse(start) if start is not None else ''
    end_date = dateutil.parser.isoparse(end) if end is not None else ''
    
    if datatype == 'file':
        tickers = clean_tickers(tickers, start_date, end_date)
        start_time = datetime.datetime(start_date.year, start_date.month, start_date.day)
        end_time = datetime.datetime.combine(end_date, datetime.time(23, 59, 59))
        
                      
        # Set up data feed
        for ticker in tickers:

            data = bt.feeds.IBCSVOnlyData(
                name = ticker,
                dataname=get_filepath(ticker),
                fromdate=start_date,
                todate=end_date,
                imeframe=bt.TimeFrame.Minutes,
                compression=240,
                sessionstart=start_time,  # internally just the "time" part will be used
                sessionend=end_time,  # internally just the "time" part will be used
                reverse=False,
                adjclose=False,
                plot=not plotreturns,
            )
            cerebro.adddata(data)


    elif datatype == 'historical_limit':
        # Set up data feed
        for ticker in tickers:
            data = bt.feeds.IBData(
                name=ticker,     # Data name
                dataname=cash_hist_symbols[ticker], # Symbol name
                todate = '',
                durationStr='1 D',
                barSizeSetting='1 min',
                historical=True,
                what='Midpoint',
                useRTH=0,
                formatDate = 1,
                keepUpToDate = False,
            )
            cerebro.adddata(data)

    elif datatype == 'historical_update':
        for ticker in tickers:
            data = bt.feeds.IBData(
                name=ticker,     # Data name
                dataname=mircofut_real_symbols[ticker], # Symbol name
                todate = '',
                durationStr='1 D',
                barSizeSetting='5 mins',
                historical=True,
                what='Midpoint',
                useRTH=0,
                formatDate = 1,
                keepUpToDate = True,
            )
            cerebro.adddata(data)

    elif datatype == 'realtime':
        for ticker in tickers:
                   
            backfillkwargs = dict(
                dataname=mircofut_real_symbols[ticker], # Symbol name
                todate = '',
                durationStr='1 D',
                barSizeSetting='1 min',
                timeframe = bt.TimeFrame.Minutes,
                compression = 1, 
                historical=True,
                what='Midpoint',
                useRTH=0,
                formatDate = 1,
                keepUpToDate = False,
            )
            #backfill_for_data = bt.feeds.IBData(**backfillkwargs)
            
            data = bt.feeds.IBData(
                name=ticker,     # Data name
                dataname=mircofut_real_symbols[ticker], # Symbol name
                fromdate = start_date,
                enddate = '',
                todate = '',
                timeframe = bt.TimeFrame.Seconds,
                compression = 5,                 
                rtbar=True,
                what='TRADES', 
                qcheck=0.5,
                useRTH=0,
                formatDate = 2,
                backfill_start = False,   
                keepUpToDate = False,       
            )
            cerebro.adddata(data)

            cerebro.replaydata(
                data,
                name=ticker+'_1M',
                timeframe=bt.TimeFrame.Minutes,
                compression=1)


def run(**kwargs):
    runmode = kwargs.get('runmode', None)
    datatype = kwargs.get('datatype', None)
    if runmode == 'backtest':
        assert datatype in ['file', 'historical_limit']
    elif runmode == 'trading':
        assert datatype in ['realtime', 'historical_update']
    else:
        raise ValueError(f"无效的 runmode：{runmode}")

    strategy = kwargs.pop('strategy', 'CrossOver')
    module_path = f'.algos.{strategy}'
    module = importlib.import_module(module_path, 'backtest')
    strategy = getattr(module, strategy)

    cerebro = cerebor.NewCerebro(
        stdstats=True,
        runmode=runmode,
        cheat_on_open=False,
    )

    if datatype != 'file':
        #port gw.live=4001 gw.paper=4002 tws.live=7496 tws.paper=7497
        store = bt.stores.IBStoreInsync(clientId=214, port=4002, runmode=runmode)
        cerebro.addstore(store)
        cerebro.broker = store.getbroker() 

    # Set initial cash amount and commision  
    if runmode == 'backtest':
        cash = kwargs.get('cash', 100000.0) 
        cerebro.broker.setcash(cash) 
    else:
        cash = cerebro.broker.get_cash()

    comminfo = bt.commissions.IBCommInfo(commission= 0.001)
    cerebro.broker.addcommissioninfo(comminfo)
    cerebro.broker.set_slippage_perc(perc=0.0001, slip_open=True, slip_match=False)

    # Add a strategy
    strat_kwargs = kwargs.get('kwargs', {})
    cerebro.addstrategy(strategy, verbose=kwargs.get('verbose', False), **strat_kwargs)

    adddata(cerebro, **kwargs)
    # Set WriterFile output
    writerFile = r'E:\gitcode\mystrategy\logs\bt-writer\writer.csv'
    cerebro.addwriter(bt.WriterFile, out=writerFile, csv=True, csv_counter=True, rounding=2, indent=4)

    # Add analyzers
    add_analyzers(cerebro, **kwargs)

    # Run backtest
    results = cerebro.run(preload=False, runmode=runmode)

    # Show analyzers result
    show_analyzers_reslut(results, cash, cerebro.broker.getvalue())

    # Plot results
    plot = kwargs.get('plot', False)
    if runmode == 'backtest':
        if plot:
            cerebro.plot()
    else:
        store.run()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument('runmode', nargs=1)
    PARSER.add_argument('strategy', nargs=1)
    PARSER.add_argument('-t', '--tickers', nargs='+')
    PARSER.add_argument('-u', '--universe', nargs=1)
    PARSER.add_argument('-d', '--datatype', nargs=1)
    PARSER.add_argument('-x', '--exclude', nargs='+')
    PARSER.add_argument('-s', '--start', nargs=1)
    PARSER.add_argument('-e', '--end', nargs=1)
    PARSER.add_argument('--cash', nargs=1, type=int)
    PARSER.add_argument('-v', '--verbose', action='store_true')
    PARSER.add_argument('-p', '--plot', action='store_true')
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
    del ARG_ITEMS['tickers']
    del ARG_ITEMS['kwargs']
    del ARG_ITEMS['exclude']

    # Remove None values
    STRATEGY_ARGS = {k: (v[0] if isinstance(v, list) else v) for k, v in ARG_ITEMS.items() if v}
    STRATEGY_ARGS['tickers'] = [ticker.strip() for ticker in TICKERS[0].split(',')]
    STRATEGY_ARGS['kwargs'] = eval('dict(' + KWARGS + ')')

    if EXCLUDE:
        STRATEGY_ARGS['exclude'] = [EXCLUDE] if len(EXCLUDE) == 1 else EXCLUDE

    run(**STRATEGY_ARGS)
