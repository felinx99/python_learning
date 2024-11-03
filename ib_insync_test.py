import ib_insync
import logging
import time

ib_insync.util.logToConsole(logging.INFO)

ib = ib_insync.IB()
ib.connect(host='127.0.0.1', port=7497, clientId=11)

con_thread = threading.Thread(name='ibtest', target=websocket_con, daemon=True)
con_thread.start()

#myacc = ib.accountValues()
#account = ib.reqAccountUpdates(account='DU9965348')

#contract = ib_insync.Contract(symbol='DAX',lastTradeDateOrContractMonth = "202412", secType='FUT', exchange='EUREX')

contract = ib_insync.Contract()
contract.symbol = "EUR"
contract.secType = "CASH"
contract.exchange = "IDEALPRO"
contract.currency = "USD"

print(contract)
cds = ib.reqContractDetails(contract)
assert len(cds)==1

account = ib.reqAccountUpdates(account='DU9965348')

contract = ib_insync.Contract()
contract.symbol = "GOOG"
contract.secType = "STK"
contract.exchange = "SMART"
contract.currency = "USD"

data0 = ib.reqHistoricalData(
        reqId=101,
        contract=contract,
        endDateTime='',
        durationStr='2 Y',
        barSizeSetting='1 day',
        whatToShow='Trades',
        useRTH=0, #0 = Includes data outside of RTH | 1 = RTH data only 
        formatDate = 1, 
        keepUpToDate = True, #0 = False | 1 = True
        chartOptions=[])
    
ib_insync.util.startLoop()

print('hello world')