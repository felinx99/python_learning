from ibapi.client import *
from ibapi.wrapper import *
from ibapi.contract import Contract

import threading
import time
from datetime import datetime, timezone, timedelta

class TradeApp(EWrapper, EClient):
    def __init__(self): 
        EClient.__init__(self, self) 
        
    def accountSummary(self, reqId: int, account: str, tag: str, value: str,currency: str):
        print("AccountSummary. ReqId:", reqId, "Account:", account,"Tag: ", 
              tag, "Value:", value, "Currency:", currency)
    
    def accountSummaryEnd(self, reqId: int):
        print("AccountSummaryEnd. ReqId:", reqId)

    def updateAccountValue(self, key: str, val: str, currency: str,accountName: str):
        print("UpdateAccountValue. Key:", key, "Value:", val, "Currency:", 
              currency, "AccountName:", accountName)
    
    def updatePortfolio(self, contract: Contract, position: Decimal,marketPrice: float, 
                        marketValue: float, averageCost: float, unrealizedPNL: float, 
                        realizedPNL: float, accountName: str):
        print("UpdatePortfolio.", "Symbol:", contract.symbol, "SecType:", contract.secType, 
              "Exchange:",contract.exchange, "Position:", decimalMaxString(position), 
              "MarketPrice:", floatMaxString(marketPrice),"MarketValue:", floatMaxString(marketValue), 
              "AverageCost:", floatMaxString(averageCost), "UnrealizedPNL:", floatMaxString(unrealizedPNL), 
              "RealizedPNL:", floatMaxString(realizedPNL), "AccountName:", accountName)
        
    def updateAccountTime(self, timeStamp: str):
        print("UpdateAccountTime. Time:", timeStamp)
        
    def accountDownloadEnd(self, accountName: str):
        print("AccountDownloadEnd. Account:", accountName)

    def accountSummary(self, reqId: int, account: str, tag: str, value: str,currency: str):
        print("AccountSummary. ReqId:", reqId, "Account:", account,"Tag: ", tag, "Value:", value, "Currency:", currency)
    
    def accountSummaryEnd(self, reqId: int):
        print("AccountSummaryEnd. ReqId:", reqId)

    def currentTime(self, time: int):
        dt = datetime.fromtimestamp(time, timezone.utc)
        print(f"currentTime dt={dt}")

    def historicalData(self, reqId, bar):
        print("HistoricalData. ReqId:", reqId, "BarData.", bar)

    def historicalSchedule(self, reqId: int, startDateTime: str, endDateTime: str, timeZone: str, sessions: ListOfHistoricalSessions):
        print("HistoricalSchedule. ReqId:", reqId, "Start:", startDateTime, "End:", endDateTime, "TimeZone:", timeZone)
        for session in sessions:
            print("tSession. Start:", session.startDateTime, "End:", session.endDateTime, "Ref Date:", session.refDate)

    def historicalDataUpdate(self, reqId: int, bar: BarData):
        print("HistoricalDataUpdate. ReqId:", reqId, "BarData.", bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)


def websocket_con():
    app.run()

from dateutil.relativedelta import relativedelta

dura = '370 D'
duranumber = int(dura.split()[0])
duraunit = dura.split()[1]
todate = datetime(2025, 3, 31, 2, 54, 47, 282310)
units_map = {
        'Y': 'years',
        'M': 'months',
        'W': 'weeks',
        'D': 'days',
        'S': 'seconds'
        }

kwargs = {units_map[duraunit]: duranumber}
fromdate = todate - relativedelta(**kwargs)
print(f"fromdate: {fromdate}")
print(f"todate: {todate}")

app = TradeApp()      
app.connect("127.0.0.1", 7497, clientId=15)

con_thread = threading.Thread(name='ibtest', target=websocket_con, daemon=True)
con_thread.start()

time.sleep(1)

summary = app.reqAccountSummary(reqId=1, groupName='All', tags='NetLiquidation')

account = app.reqAccountUpdates(subscribe=True, acctCode='DU9965348')

contract = Contract()
contract.symbol = "GOOG"
contract.secType = "STK"
contract.exchange = "SMART"
contract.currency = "USD"

data0 = app.reqHistoricalData(
        reqId=101,
        contract=contract,
        endDateTime='',
        durationStr='1 D',
        barSizeSetting='1 min',
        whatToShow='Trades',
        useRTH=0, #0 = Includes data outside of RTH | 1 = RTH data only 
        formatDate = 1, 
        keepUpToDate = True, #0 = False | 1 = True
        chartOptions=[])


app.reqCurrentTime()

print('hello world')