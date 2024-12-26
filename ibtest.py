from ibapi.client import *
from ibapi.wrapper import *
from ibapi.contract import Contract
from ibapi.utils import floatMaxString, intMaxString, decimalMaxString, longMaxString
from ibapi.common import *
from ibapi.account_summary_tags import *

import threading
import time
from datetime import datetime, timezone, timedelta
import zoneinfo
import matplotlib.dates as mdates

import sys
sys.path.append(r'E:\gitcode\backtrader\backtrader\stores')

from ibstores.objects import (
        AccountValue, BarData, BarDataList, CommissionReport, ConnectionStats,
        DOMLevel, DepthMktDataDescription, Dividends, Execution, ExecutionFilter,
        FamilyCode, Fill, FundamentalRatios, HistogramData, HistoricalNews,
        HistoricalSchedule, HistoricalSession, HistoricalTick,
        HistoricalTickBidAsk, HistoricalTickLast, MktDepthData, NewsArticle,
        NewsBulletin, NewsProvider, NewsTick, OptionChain, OptionComputation,
        PnL, PnLSingle, PortfolioItem,
        Position, PriceIncrement, RealTimeBar, RealTimeBarList, ScanDataList,
        ScannerSubscription, SmartComponent, SoftDollarTier, TickAttrib,
        TickAttribBidAsk, TickAttribLast, TickByTickAllLast, TickByTickBidAsk,
        TickByTickMidPoint, TickData, TradeLogEntry, WshEventData)
from ib_insync.util import parseIBDatetime


def bracket_order(parent_order_id, action, quantity, limit_price, take_profit_limit_price, stop_loss_price):
    """
    创建套利订单（Bracket Order）。

    Args:
        parent_order_id: 父订单 ID。
        action: 交易方向 ("BUY" 或 "SELL")。
        quantity: 交易数量。
        limit_price: 父订单限价。
        take_profit_limit_price: 止盈限价。
        stop_loss_price: 止损触发价。

    Returns:
        包含父订单、止盈订单和止损订单的列表。
    """

    # 父订单
    parent = Order()
    parent.orderId = parent_order_id  # 注意大小写，python中是小写
    parent.action = action
    parent.orderType = "LMT"
    parent.totalQuantity = quantity
    parent.lmtPrice = limit_price
    parent.transmit = False  # 设置为 False，直到最后一个子订单发送

    # 止盈订单
    take_profit = Order()
    take_profit.orderId = parent_order_id + 1
    take_profit.action = "SELL" if action == "BUY" else "BUY"
    take_profit.orderType = "LMT"
    take_profit.totalQuantity = quantity
    take_profit.lmtPrice = take_profit_limit_price
    take_profit.parentId = parent_order_id
    take_profit.transmit = False

    # 止损订单
    stop_loss = Order()
    stop_loss.orderId = parent_order_id + 2
    stop_loss.action = "SELL" if action == "BUY" else "BUY"
    stop_loss.orderType = "STP"
    stop_loss.auxPrice = stop_loss_price  # 止损触发价
    stop_loss.totalQuantity = quantity
    stop_loss.parentId = parent_order_id
    stop_loss.transmit = True  # 最后一个子订单，设置为 True

    bracket_order = [parent, take_profit, stop_loss]
    return bracket_order

class TradeApp(EWrapper, EClient):
    def __init__(self): 
        EClient.__init__(self, self) 
        self.realtimebars = list()
        self.positions = dict()
        
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
        print(f"{'\n'} ContractDetails-{reqId}: ( {contractDetails} ){'\n'} ")

    def contractDetailsEnd(self, reqId: int):
        print("ContractDetailsEnd. ReqId:", reqId)

    def bondContractDetails(self, reqId: int, contractDetails: ContractDetails):
        print(reqId, contractDetails)

    def realtimeBar(self, reqId: TickerId, time:int, open_: float, high: float, low: float, close: float, volume: Decimal, wap: Decimal, count: int):
        formatted_num = f"{wap:.4f}"
        print(
            "RTBar", longMaxString(time),
            "-", parseIBDatetime(str(time)) ,
            ", Open:" , floatMaxString(open_) ,
            ", High:" , floatMaxString(high) ,
            ", Low:" , floatMaxString(low) ,
            ", Close:" , floatMaxString(close) ,
            ", Volume:" , decimalMaxString(volume) ,
            ", Count:" , intMaxString(count) ,
            ", WAP:" , formatted_num,
        )

        realtimebar = RealTimeBar(
            time=time,
            open_=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            wap=wap,
            count=count
        )
        self.realtimebars.append(realtimebar)

    def position(self, account: str, contract: Contract, position: Decimal, avgCost: float):
        print(f"Position-{account}, Assert:{contract.symbol}, Contract:{contract.conId}, Position:{position}, Avg cost:{avgCost}")
        mypos = Position(
            account=account,
            contract=contract,
            position=position,
            avgCost=avgCost
        )
        self.positions[contract.symbol] = mypos
        
    def positionEnd(self):
        print("PositionEnd")

    def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        print(f"OpenOrder id-{orderId}, contract: {contract.conId}, order: {order.orderId}, orderState: {orderState.status}")
    
    def openOrderEnd(self):
        print("OpenOrderEnd")

    def orderStatus(self, orderId: OrderId, status: str, filled: Decimal, remaining: Decimal, avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        print(f"OrderStatus id-{orderId}, status: {status}, filled: {filled}, remaining: {remaining}, avgFillPrice: {avgFillPrice}, permId: {permId}, parentId: {parentId}, lastFillPrice: {lastFillPrice}, clientId: {clientId}, whyHeld: {whyHeld}, mktCapPrice: {mktCapPrice}")

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        print("ExecDetails. ReqId:", reqId, "Symbol:", contract.symbol, "SecType:", contract.secType, "Currency:", contract.currency, execution)
    
    def execDetailsEnd(self, reqId: int):
        print("ExecDetailsEnd. ReqId:", reqId)

    def error(self, reqId: TickerId, errorTime: int, errorCode: int, errorString: str, advancedOrderRejectJson = ""):
        print("Error. Id:", reqId, errorTime, "Code:", errorCode, "Msg:", errorString, "AdvancedOrderRejectJson:", advancedOrderRejectJson)

    

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


#port gw.live=4001 gw.paper=4002 tws.live=7496 tws.paper=7497
app = TradeApp()      
app.connect("127.0.0.1", 7497, clientId=15)

con_thread = threading.Thread(name='ib_test', target=websocket_con, daemon=True)
con_thread.start()
time.sleep(1)

pos = app.reqPositions()
time.sleep(1)

summary = app.reqAccountSummary(reqId=1, groupName='All', tags=AccountSummaryTags.AllTags)

account = app.reqAccountUpdates(subscribe=True, acctCode='DU9965348')

contract = Contract()
contract.symbol = "M6B"
contract.secType = "FUT"
contract.exchange = "CME"
contract.currency = "USD"
contract.lastTradeDateOrContractMonth ='20250317'
contract.multiplier = "6250"

reqId = 102
app.reqContractDetails(reqId=reqId, contract=contract)

'''

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

data1 = app.reqRealTimeBars(
    reqId=106,
    contract=contract,
    barSize=5,
    whatToShow='TRADES',
    useRTH=0,
    realTimeBarsOptions=[],
)
time.sleep(10)

orderid = 213
order = Order()
order.orderType = 'LMT'
order.action = 'SELL'
order.totalQuantity = 15
#order.lmtPrice = f"{app.realtimebars[-1].close*1.01:.4f}"
order.tif = 'GTC'
#app.reqOpenOrders()

#app.placeOrder(orderid, contract, order)

print('hello world')