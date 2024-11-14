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

FROMDATE = dt.datetime(2014, 12, 1)
TODATE = dt.datetime(2024, 10, 16)
ENDDATE = ''

mircofut_symbols = [
    {'name':'GPBUSD', 'symbol':"FUT-M6B-USD-CME-20241216-6250-False"},
    {'name':'AUDUSD', 'symbol':"FUT-M6A-USD-CME-20241216-10000-False"},
    {'name':'EURUSD', 'symbol':"FUT-M6E-USD-CME-20241216-12500-False"},
    {'name':'JPYUSD', 'symbol':"FUT-MJY-USD-CME-20241216-1250000-False"},
    {'name':'CADUSD', 'symbol':"FUT-MCD-USD-CME-20241217-10000-False"},
    {'name':'CHFUSD', 'symbol':"FUT-MSF-USD-CME-20241216-12500-False"},
]
             
#   FUT-M6B-USD-CME-20241216-6250-False
#   FUT-M6A-USD-CME-20241216-10000-False
#   FUT-M6E-USD-CME-20241216-12500-False
#   FUT-MJY-USD-CME-20241216-1250000-False
#   FUT-MCD-USD-CME-20241217-10000-False
#   FUT-MSF-USD-CME-20241216-12500-False


""

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
    cerebro = bt.Cerebro(stdstats=False)
    store = bt.stores.IBStoreInsync(clientId=214, port=4002, _debug=True)
    
    for fut_symbol in mircofut_symbols:
        '''
        data = bt.feeds.IBData(
            name=fut_symbol['name'],     # Data name
            dataname=fut_symbol['symbol'], # Symbol name
            todate = '',
            durationStr='3 M',
            barSizeSetting='4 hours',
            historical=True,
            what='Trades',
            useRTH=0,
            formatDate = 1,
            keepUpToDate = False,
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
        
        cerebro.adddata(data)

    cerebro.broker = store.getbroker()
    cerebro.addstrategy(teststrategy.St)
    cerebro.run()
    cerebro.plot()

if __name__ == '__main__':
    test_run()
    bt.stores.ibstores.util.startLoop()
    print("test over")


