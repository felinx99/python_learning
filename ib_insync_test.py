import ib_insync
import logging
import time

ib_insync.util.startLoop()
ib_insync.util.logToConsole(logging.INFO)

ib = ib_insync.IB()
ib.connect(host='127.0.0.1', port=4002, clientId=11)

contract = ib_insync.Contract()
contract.symbol = "GOOG"
contract.secType = "STK"
contract.exchange = "SMART"
contract.currency = "USD"

print(contract)
cds = ib.reqContractDetails(contract)
assert len(cds)==1

data0 = ib.reqHistoricalData(
        contract=contract,
        endDateTime='',
        durationStr='1 M',
        barSizeSetting='1 day',
        whatToShow='Trades',
        useRTH=0, #0 = Includes data outside of RTH | 1 = RTH data only 
        formatDate = 1, 
        keepUpToDate = True, #0 = False | 1 = True
        chartOptions=[])


print('hello world')