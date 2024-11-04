import datetime
import os
import os.path
import sys
import logging
import time
import threading

# append module root directory to sys.path
#lpath = sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 添加模块所在目录的绝对路径,然后导入模块正常了。否则报错(未安装backtrader) 
import sys
sys.path.append(r'E:\gitcode\backtrader')

import backtrader as bt
import backtrader.stores.ibstore_insync as IB

def websocket_con():
    IB.util.startLoop()


IB.util.logToConsole(logging.INFO)

ib = IB.IBStoreInsync(port=7497, _debug=True)

#myacc = ib.accountValues()
#account = ib.reqAccountUpdates(account='DU9965348')

#contract = ib_insync.Contract(symbol='DAX',lastTradeDateOrContractMonth = "202412", secType='FUT', exchange='EUREX')

#contract = IB.Contract()
#contract.symbol = "EUR"
#contract.secType = "CASH"
#contract.exchange = "IDEALPRO"
#contract.currency = "USD"

contract = IB.Contract()
contract.symbol = "GOOG"
contract.secType = "STK"
contract.exchange = "SMART"
contract.currency = "USD"

print(contract)
cds = ib.reqContractDetails(contract)
assert len(cds)==1

endDateTime = datetime.datetime(2014, 12, 31)
#endDateTime = datetime.datetime.now().astimezone(datetime.timezone.utc)

data0 = ib.reqHistoricalData(
        contract=contract,
        endDateTime=endDateTime,
        durationStr='1 D',
        barSizeSetting='30 mins',
        whatToShow='Bid',
        useRTH=0, #0 = Includes data outside of RTH | 1 = RTH data only 
        formatDate = 1, 
        keepUpToDate = False, #0 = False | 1 = True
        chartOptions=[])

print(data0)
    

