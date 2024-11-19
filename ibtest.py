from ibapi.client import *
from ibapi.wrapper import *
from ibapi.contract import Contract
from ibapi.utils import floatMaxString, intMaxString, decimalMaxString, longMaxString

import threading
import time
from datetime import datetime, timezone, timedelta
import zoneinfo
import matplotlib.dates as mdates

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

    def headTimestamp(self, reqId, headTimestamp):
        print(f"headTimestamp ReqId:", reqId,"headTimestamp:", headTimestamp)

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        print(reqId, contractDetails)

    def contractDetailsEnd(self, reqId: int):
        print("ContractDetailsEnd. ReqId:", reqId)

    def bondContractDetails(self, reqId: int, contractDetails: ContractDetails):
        print(reqId, contractDetails)

    def realtimeBar(self, reqId: TickerId, time:int, open_: float, high: float, low: float, close: float, volume: Decimal, wap: Decimal, count: int):
        print(
            "RealTimeBar. TickerId:", reqId,
            " - Time: ", longMaxString(time) ,
            ", Open: " , floatMaxString(open_) ,
            ", High: " , floatMaxString(high) ,
            ", Low: " , floatMaxString(low) ,
            ", Close: " , floatMaxString(close) ,
            ", Volume: " , decimalMaxString(volume) ,
            ", Count: " , intMaxString(count) ,
            ", WAP: " , decimalMaxString(wap)
        )

    def position(self, account: str, contract: Contract, position: Decimal, avgCost: float):
        print("Position.", "Account:", account, "Contract:", contract, "Position:", position, "Avg cost:", avgCost)
        
    def positionEnd(self):
        print("PositionEnd")

    def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        print(orderId, contract, order, orderState)
    
    def openOrderEnd(self):
        print("OpenOrderEnd")

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


date= datetime(2024, 11, 1, 4, 0, tzinfo=zoneinfo.ZoneInfo(key='US/Eastern'))
dt = mdates.date2num(date)
cover_date = mdates.num2date(dt)
print(f'date: {date}')
print(f"convert date: {cover_date}")

headTimestamp = '20140113-14:30:00'
# 指定日期时间格式
format_str = '%Y%m%d-%H:%M:%S'

# 将字符串转换为 datetime 对象
dt = datetime.strptime(headTimestamp, format_str)


app = TradeApp()      
app.connect("127.0.0.1", 4002, clientId=15)

con_thread = threading.Thread(name='ibtest', target=websocket_con, daemon=True)
con_thread.start()
time.sleep(1)

pos = app.reqPositions()
time.sleep(1)
print(pos)
'''
summary = app.reqAccountSummary(reqId=1, groupName='All', tags='NetLiquidation')

#account = app.reqAccountUpdates(subscribe=True, acctCode='DU9965348')

contract = Contract()
contract.symbol = "M6B"
contract.secType = "FUT"
contract.exchange = "CME"
contract.currency = "USD"
contract.lastTradeDateOrContractMonth ='20241216'
contract.multiplier = "6250"

reqId = 102
app.reqContractDetails(reqId=reqId, contract=contract)



reqId = 103
timestamp = app.reqHeadTimeStamp(reqId, contract, "TRADES", 1, 1)
app.cancelHeadTimeStamp(reqId)

endDateTime = '20230615 16:00:00 US/Eastern'
'''
'''
data0 = app.reqHistoricalData(
        reqId=101,
        contract=contract,
        endDateTime=endDateTime,
        durationStr='1 M',
        barSizeSetting='1 day',
        whatToShow='Trades',
        useRTH=0, #0 = Includes data outside of RTH | 1 = RTH data only 
        formatDate = 1, 
        keepUpToDate = False, #0 = False | 1 = True
        chartOptions=[])
'''
'''
data1 = app.reqRealTimeBars(
    reqId=106,
    contract=contract,
    barSize=5,
    whatToShow='TRADES',
    useRTH=0,
    realTimeBarsOptions=[],
)
'''
app.reqCurrentTime()

print('hello world')