import datetime
import os
import os.path
import sys
import logging
import time
import threading

from IPython.display import display, clear_output
import matplotlib.pyplot as plt

# append module root directory to sys.path
#lpath = sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 添加模块所在目录的绝对路径,然后导入模块正常了。否则报错(未安装backtrader) 
import sys
sys.path.append(r'E:\gitcode\backtrader')

import backtrader as bt
import backtrader.stores.ibstore_insync as IB


def funCount(func):
    def wrapper(*args, **kwargs):
        wrapper.count += 1
        current_time = time.time()
        if wrapper.count > 1:
            time_diff = current_time - wrapper.last_time
            print(f"函数 {func.__name__} 第 {wrapper.count} 次调用，与上次调用相差 {time_diff:.3f} 秒")
        elif wrapper.count == 1:
            print(f"函数 {func.__name__} 第 {wrapper.count} 次调用")
        wrapper.last_time = current_time
        return func(*args, **kwargs)
    wrapper.count = 0
    wrapper.last_time = 0
    return wrapper

@funCount
def onBarUpdate(bars, hasNewBar):
    #plt.close('all')
    curtime = bars[0].time
    print(f"plot size:{len(bars)}, new data {curtime}")
    #print(f"plot size:{len(bars)}")
    plot = IB.util.barplot(bars=bars[-10:], title=str(curtime))
    clear_output(wait=True)
    display(plot)
    plt.show()

end_event = threading.Event()
def websocket_con():
    print("websocket_con is running...")
        

IB.util.logToConsole(logging.INFO)

ib = IB.IBStoreInsync(clientId=214, port=4002, _debug=True)
#con_thread = threading.Thread(name='ibtest', target=websocket_con, daemon=True)
#con_thread.start()

contract = IB.Contract()
contract.symbol = "USD"
contract.secType = "CASH"
contract.currency = "CAD"
contract.exchange = "IDEALPRO"


print(contract)
cds = ib.reqContractDetails(contract)
print(cds)
assert len(cds)==1

#starttime = ib.reqHeadTimeStamp(contract, 'Trades', 1, 1)
endDateTime = datetime.datetime(2024, 5, 16)
#endDateTime = datetime.datetime.now().astimezone(datetime.timezone.utc)


data0 = ib.reqHistoricalData(
        contract=contract,
        endDateTime=endDateTime,
        durationStr='3 M',
        barSizeSetting='4 hours',
        whatToShow='ASK',
        useRTH=0, #0 = Includes data outside of RTH | 1 = RTH data only 
        formatDate = 1, 
        keepUpToDate = False, #0 = False | 1 = True
        chartOptions=[])


# data0 = ib.reqRealTimeBars(contract, 5, 'TRADES', False)

print(f"totel data :{len(data0)}")
#ib.barUpdateEvent += onBarUpdate

ib.reqPositions()
#ib.sleep(300)
#ib.cancelRealTimeBars(data0)
#ib.disconnect()

while True:
    # 检查信号量是否被设置
    if end_event.is_set():
        break
    print(f"Loop is running...{len(data0)}")
    ib.sleep(1)
