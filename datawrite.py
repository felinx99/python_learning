import ib_insync
import logging
import time
import os
import os.path
import pandas as pd


basepath = r'E:\data'

mircofut_hist_symbols = [
    {'name':'GBPUSD', 'symbol':"CASH-GBP-USD-IDEALPRO"},
    {'name':'AUDUSD', 'symbol':"CASH-AUD-USD-IDEALPRO"},
    {'name':'USDCAD', 'symbol':"CASH-USD-CAD-IDEALPRO"},
    {'name':'CHFUSD', 'symbol':"CASH-CHF-USD-IDEALPRO"},
    {'name':'EURUSD', 'symbol':"CASH-EUR-USD-IDEALPRO"},
    {'name':'USDJPY', 'symbol':"CASH-USD-JPY-IDEALPRO"},   
]

STK_hist_symbols = [
    {'name':'GOOG', 'symbol':"STK-GOOG-USD-SMART-NASDAQ"},
    {'name':'META', 'symbol':"STK-META-USD-SMART-NASDAQ"},
    {'name':'AAPL', 'symbol':"STK-AAPL-USD-SMART-NASDAQ"},
    {'name':'AMZN', 'symbol':"STK-AMZN-USD-SMART-NASDAQ"},
    {'name':'NFLX', 'symbol':"STK-NFLX-USD-SMART-NASDAQ"},
     
]

barSizeList = [
    ('1 day', '1_day'),
    ('4 hours', '4_hours'),
    ('1 hour', '1_hour'),
    ('30 mins', '30_mins'),
    ('15 mins', '15_mins'),
]

tradeModeList = [
    'MIDPOINT',
    'BID',
    'ASK',
    'TRADES'
]

#ib_insync.util.startLoop()
ib_insync.util.logToConsole(logging.INFO)

ib = ib_insync.IB()
ib.connect(host='127.0.0.1', port=4002, clientId=11)
ib.sleep(1)

for assert_symbol in STK_hist_symbols:
    data_name = assert_symbol['name']     # Data name
    data_str = assert_symbol['symbol'] # Symbol name
    # Make the initial contract
    contract = ib_insync.Contract()

    # split the ticker string
    tokens = iter(data_str.split('-'))

    # Symbol and security type are compulsory
    contract.secType = next(tokens)
    contract.symbol = next(tokens)
    contract.currency = next(tokens)
    contract.exchange = next(tokens)
        
    print(contract)
    cds = ib.reqContractDetails(contract)
    assert len(cds)==1
    contract = cds[0].contract

    # headtime = ib.reqHeadTimeStamp(contract, "MIDPOINT", 1, 1)
    # print(f"headTimestamp:", headtime)
    indexA = 0
    for barSizeVar, barSizePath in barSizeList:
        for trademode in tradeModeList:        
            endtime = ''
            barsList = []

            indexA += 1

            #生成文件路径
            fullpath = os.path.join(basepath, contract.secType, data_name, trademode)
            try:
                os.makedirs(fullpath)
                print(f"目录 {fullpath} 已成功创建")
            except FileExistsError:
                print(f"目录 {fullpath} 已存在")
            except OSError as e:
                print(f"创建目录失败: {e}")

            #生成文件名称
            filename = contract.secType + '_' + data_name + '_' + barSizePath + '_' + trademode + '.csv'
            print(f"filename: {filename}")

            #生成文件完整路径+名称
            fullFileName = os.path.join(fullpath, filename)

            if os.path.exists(fullFileName):
                print(f"文件 {filename} 已存在, 跳过本次循环")
                ib.sleep(1)
                ib.reqCurrentTime()
                print('run break ', indexA)
                continue
            
            while True:             
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime=endtime,
                    durationStr='3 M',
                    barSizeSetting=barSizeVar,
                    whatToShow=trademode,
                    useRTH=True,
                    formatDate=1)
                    
                if not bars:
                    print('get reqHistoricalData failed')
                    break

                barsList.append(bars)
                endtime = bars[0].date
                print(f'endtime: {endtime}')
                ib.sleep(2)
            
            # save to CSV file
            allBars = [b for bars in reversed(barsList) for b in bars]

            if allBars is not None:
                df = ib_insync.util.df(allBars)
                
                print(f"fullFileName: {fullFileName}")
                try:
                    df.to_csv(fullFileName, index=False, encoding='utf-8')
                except Exception as e:
                    print(f"csv文件保存失败: {e}")
            else:
                print("None One data")
           
            ib.sleep(1)
            ib.reqCurrentTime()
            print('run over ', indexA)