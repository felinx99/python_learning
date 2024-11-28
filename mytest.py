'''
import testcommon
import backtrader.indicators as btind # type: ignore

chkdatas = 2
chkvals = []

chkmin = 151  # because of the weekly data
chkind = [btind.SMA]
chkargs = dict()


def test_run(main=False):
    datas = [testcommon.getdata(i) for i in range(chkdatas)]
    testcommon.runtest(datas,
                       testcommon.TestStrategy,
                       main=main,
                       plot=main,
                       chkind=chkind,
                       chkmin=chkmin,
                       chkvals=chkvals,
                       chkargs=chkargs)

'''
import testcommon
import backtrader as bt
import teststrategy
import datetime as dt
import os
import os.path

basepath = r'E:\data\CASH'

FROMDATE = dt.datetime(2023, 1, 1, 0, 0, 0)
TODATE = dt.datetime(2023, 1, 31, 23, 59, 59)
ENDDATE = ''

mircofut_hist_symbols = [
    {'name':'GPBUSD', 'symbol':"CASH-GBP-USD-IDEALPRO"},
#    {'name':'AUDUSD', 'symbol':"CASH-AUD-USD-IDEALPRO"},
#    {'name':'EURUSD', 'symbol':"CASH-EUR-USD-IDEALPRO"},
#    {'name':'USDJPY', 'symbol':"CASH-USD-JPY-IDEALPRO"},
#    {'name':'USDCAD', 'symbol':"CASH-USD-CAD-IDEALPRO"},
#    {'name':'CHFUSD', 'symbol':"CASH-CHF-USD-IDEALPRO"},
]

'''
mircofut_real_symbols = [
    {'name':'GPBUSD', 'symbol':"FUT-M6B-USD-CME-20241216-6250-False"},
    {'name':'AUDUSD', 'symbol':"FUT-M6A-USD-CME-20241216-10000-False"},
    {'name':'EURUSD', 'symbol':"FUT-M6E-USD-CME-20241216-12500-False"},
    {'name':'USDJPY', 'symbol':"FUT-MJY-USD-CME-20241216-1250000-False"},
    {'name':'CADUSD', 'symbol':"FUT-MCD-USD-CME-20241217-10000-False"},
    {'name':'CHFUSD', 'symbol':"FUT-MSF-USD-CME-20241216-12500-False"},
]
'''          

'''
other contract params: '1 D', '30 mins', 'Bid', formatDate=1, keepUpToDate=False

BOND-122014AJ2-USD-SMART    #EndData=datetime(2024, 5, 16) / ''
CFD-IBUS30-USD-SMART    #EndData=datetime(2014, 12, 31) / ''
CMDTY-XAUUSD-USD-SMART  #EndData=datetime(2024, 5, 16) / ''
CRYPTO-ETH-USD-PAXOS    #EndData=datetime(2024, 5, 16) / ''
CONTFUT-ES-USD-CME  #'', Not supoort EndData
CASH-EUR-GBP-IDEALPRO   #EndData=datetime(2024, 5, 16) / ''
IND-DAX-EUR-EUREX   #EndData=datetime(2014, 12, 31) / '', not support bid/ask
FUND-VWELX-USD-FUNDSERV #EndData=datetime(2014, 12, 31) / '', only support trades
STK-AAPL-USD-SMART  #EndData=datetime(2014, 12, 31) / ''
STK-SPY-USD-SMART-ARCA  #EndData=datetime(2014, 12, 31) / ''
STK-EMCGU-USD-SMART #Stock Contract with IPO price  #EndData=datetime(2024, 5, 16) / ''
IOPT-B881G-EUR-SBF #Not Found suitable example for IOPT

FIGI-BBG000B9XRY4-SMART #''

FUT-ES-USD-CME-202809-50-False  #EndData=datetime(2024, 5, 16) / ''
FUT-ES-USD-CME-202309-None-True #not supported

FOP-GBL-EUR-EUREX-20241206-1000-137-C 

OPT-GOOG-USD-SMART-20241220-100-180-C #EndData=datetime(2024, 10, 16) / '' 1M 1hour
WAR-GOOG-EUR-FWB-20201117-001-15000-C
'''

def test_run(args=None):
    # stdstats是观测器开关，True打开Broker(Cash&Value),Trades, Buy&Sell三个观测器。
    # 后面在添加其他观测器。
    cerebro = bt.Cerebro(stdstats=True) 
    store = bt.stores.IBStoreInsync(clientId=214, port=4002, _debug=True)
    cerebro.addstore(store)
    
    for fut_symbol in mircofut_hist_symbols:
        '''
        data = bt.feeds.IBData(
            name=fut_symbol['name'],     # Data name
            dataname=fut_symbol['symbol'], # Symbol name
            todate = TODATE,
            durationStr='3 M',
            barSizeSetting='4 hours',
            historical=True,
            what='Ask',
            useRTH=0,
            formatDate = 1,
            keepUpToDate = False,
        )
        '''

        #生成文件路径
        fullpath = os.path.join(basepath, fut_symbol['name'], 'MIDPOINT')

        #生成文件名称
        filename = 'CASH_' + fut_symbol['name'] + '_4_hours_MIDPOINT' + '.csv'

        #生成文件完整路径+名称
        fullFileName = os.path.join(fullpath, filename)
        print(f"fullFileName: {fullFileName}")

        # Create the 1st data
        data = bt.feeds.IBCSVData(
            dataname=fullFileName,
            datainfo=fut_symbol['symbol'], #to create precontract
            fromdate=FROMDATE,
            todate=TODATE,
            timeframe=bt.TimeFrame.Days,
            compression=1,
            sessionstart=FROMDATE,  # internally just the "time" part will be used
            sessionend=TODATE,  # internally just the "time" part will be used
        )

        '''
        data = bt.feeds.IBData(
               name=fut_symbol['name'], # Data name
               dataname=fut_symbol['symbol'], # Symbol name
               backfill_start=False,
               backfill=False,
               what='TRADES', # TRADES or MIDPOINT
               useRTH=False,
               rtbar=True
              )
        '''
        cerebro.adddata(data)

    cerebro.broker = store.getbroker()
    cerebro.addstrategy(teststrategy.St)

    #添加佣金
    cerebro.broker.setcommission(commission=0.001) 

    #添加观测器指标
    cerebro.addobserver(bt.observers.Benchmark)
    cerebro.addobserver(bt.observers.TimeReturn)
    cerebro.addobserver(bt.observers.DrawDown)
    cerebro.addobserver(bt.observers.FundValue)
    cerebro.addobserver(bt.observers.FundShares)


    #添加分析指标，指标输出需要按时间段输出：按天，按周，按月，完成后输出(回测)
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='AnnualReturn')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='TimeReturn')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, riskfreerate=0.003, annualize=True, _name='SharpeRatio')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='SharpeRatio_A')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DrawDown')
    
    strats=cerebro.run()

    # 第一个策略结果输出
    strat = strats[0]
    print("--------------- AnnualReturn -----------------")
    print(strat.analyzers.AnnualReturn.get_analysis())
    print("--------------- SharpeRatio -----------------")
    print(strat.analyzers.SharpeRatio.get_analysis())
    print("--------------- DrawDown -----------------")
    print(strat.analyzers.DrawDown.get_analysis())

    cerebro.plot()

if __name__ == '__main__':
    test_run()
    bt.stores.ibstores.util.startLoop()
    print("test over")


