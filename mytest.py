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

FROMDATE = dt.datetime(2006, 1, 1)
TODATE = dt.datetime(2006, 12, 31)

def test_run(args=None):
    cerebro = bt.Cerebro(stdstats=False)
    store = bt.stores.IBStoreInsync(port=7497,
                              _debug=True
                              )
    data0 = bt.feeds.IBData(
        name="GOOG",     # Data name
        dataname='GOOG', # Symbol name
        secType='STK',   # SecurityType is STOCK
        exchange='SMART',# Trading exchange IB's SMART exchange
        currency='USD',  # Currency of SecurityType
        todate = '',
        durationStr='1 D',
        barSizeSetting='30 mins',
        historical=True,
        what='Trades',
        formatDate = 1, 
        keepUpToDate = True,
        )
    
    cerebro.adddata(data0)

    cerebro.broker = store.getbroker()
    cerebro.addstrategy(teststrategy.St)
    cerebro.run()
    cerebro.plot()

if __name__ == '__main__':
    test_run()
    bt.stores.ibstores.util.startLoop()
    print("test over")


