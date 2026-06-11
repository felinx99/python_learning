import os
import pyarrow as pa
import pandas as pd
import numpy as np
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from backtrader import order
from concurrent.futures import ProcessPoolExecutor
from common import CONFIG, DATAFRAME
from .util.order import OrderNode, OrderBook

TIMESHIFT = 1000
def get_allstock():
    srcpath = CONFIG.tdx_data_path[DATAFRAME['DAY']]/'all_stock_daily.parquet'

    try:
        full_df = pd.read_parquet(srcpath, engine='pyarrow')
    except Exception as e:
        print(f'宽表读取失败：{e}')
    return full_df

def find_growth_segments(df, threshold=0.6, window_size=250):
    """
    df: 必须包含 'close', 'date' 字段
    threshold: 涨幅阈值 (0.6 代表 60%)
    """
    segments = []
    i = 0
    n = len(df)
    
    while i < n:
        # 寻找局部的低点作为起点
        start_price = df['close'].iloc[i]
        start_date = df.index[i]
        
        # 在随后的 window_size 天内寻找最高点
        look_ahead = df.iloc[i : i + window_size]
        if look_ahead.empty: break
            
        max_idx = look_ahead['close'].idxmax()
        max_price = look_ahead['close'].max()
        max_date = max_idx
        
        growth = (max_price - start_price) / start_price
        
        if growth >= threshold:
            # 找到了一个符合要求的段，记录它
            # 为了获取更精确的“建仓期”，我们记录该段开始前的 60 天
            segments.append({
                'start_date': start_date,
                'peak_date': max_date,
                'growth': growth,
                'duration': (max_date - start_date).days
            })
            # 跳过这一段，寻找下一个可能的段
            i = df.index.get_loc(max_date) + 1
        else:
            i += 1
            
    return pd.DataFrame(segments)

snapshot_schema = pa.schema([
    ('AskOrder1', pa.int32()),
    ('AskOrder2', pa.int32()),
    ('AskOrder3', pa.int32()),
    ('AskOrder4', pa.int32()),
    ('AskOrder5', pa.int32()),
    ('AskOrder6', pa.int32()),
    ('AskOrder7', pa.int32()),
    ('AskOrder8', pa.int32()),
    ('AskOrder9', pa.int32()),
    ('AskOrder10', pa.int32()),
    ('AskPrice1', pa.int32()),
    ('AskPrice2', pa.int32()),
    ('AskPrice3', pa.int32()),
    ('AskPrice4', pa.int32()),
    ('AskPrice5', pa.int32()),
    ('AskPrice6', pa.int32()),
    ('AskPrice7', pa.int32()),
    ('AskPrice8', pa.int32()),
    ('AskPrice9', pa.int32()),
    ('AskPrice10', pa.int32()),
    ('AskVolume1', pa.int64()),
    ('AskVolume2', pa.int64()),
    ('AskVolume3', pa.int64()),
    ('AskVolume4', pa.int64()),
    ('AskVolume5', pa.int64()),
    ('AskVolume6', pa.int64()),
    ('AskVolume7', pa.int64()),
    ('AskVolume8', pa.int64()),
    ('AskVolume9', pa.int64()),
    ('AskVolume10', pa.int64()),
    ('BidOrder1', pa.int32()),
    ('BidOrder2', pa.int32()),
    ('BidOrder3', pa.int32()),
    ('BidOrder4', pa.int32()),
    ('BidOrder5', pa.int32()),
    ('BidOrder6', pa.int32()),
    ('BidOrder7', pa.int32()),
    ('BidOrder8', pa.int32()),
    ('BidOrder9', pa.int32()),
    ('BidOrder10', pa.int32()),
    ('BidPrice1', pa.int32()),
    ('BidPrice2', pa.int32()),
    ('BidPrice3', pa.int32()),
    ('BidPrice4', pa.int32()),
    ('BidPrice5', pa.int32()),
    ('BidPrice6', pa.int32()),
    ('BidPrice7', pa.int32()),
    ('BidPrice8', pa.int32()),
    ('BidPrice9', pa.int32()),
    ('BidPrice10', pa.int32()),
    ('BidVolume1', pa.int64()),
    ('BidVolume2', pa.int64()),
    ('BidVolume3', pa.int64()),
    ('BidVolume4', pa.int64()),
    ('BidVolume5', pa.int64()),
    ('BidVolume6', pa.int64()),
    ('BidVolume7', pa.int64()),
    ('BidVolume8', pa.int64()),
    ('BidVolume9', pa.int64()),
    ('BidVolume10', pa.int64()),
    ('DealNum', pa.int64()),
    ('Price', pa.int32()),
    ('SecuCode', pa.int32()),
    ('TickTime', pa.int32()),
    ('TickTimeDiff', pa.int32()),
    ('TotalAskVolume', pa.int64()),
    ('TotalBidVolume', pa.int64()),
    ('TotalDealNum', pa.int64()),
    ('TotalTurnover', pa.float64()),
    ('TotalVolume', pa.int64()),
    ('TradingDay', pa.int32()),
    ('Turnover', pa.float64()), 
    ('Volume', pa.int64()), 
    ('WeightAskPrice', pa.float32()), 
    ('WeightBidPrice', pa.float32()), 
    #('__index_level_0__', pa.int64()),
    # ... 你可以根据需要把所有百个字段写全
])

deal_schema = pa.schema([
    ('BizIndex', pa.int32()),
    ('BuyID', pa.int64()),
    ('Channel', pa.int32()),
    ('DealID', pa.int64()),
    ('DealTime', pa.int32()),
    ('Price', pa.int32()),
    ('SecuCode', pa.int32()),
    ('SellID', pa.int64()),
    ('Side', pa.int32()),
    ('TradingDay', pa.int32()),
    ('Volume', pa.int64()),
    #('__index_level_0__', pa.int64()),
    # ... 你可以根据需要把所有百个字段写全
])

'''
原始记录
order_schema = pa.schema([
    ('BizIndex', pa.int64()),
    ('Channel', pa.int64()),
    ('DBOrderID', pa.int64()),
    ('LastPrice', pa.int64()),
    ('OrderID', pa.int64()),
    ('OrderTime', pa.int64()),
    ('OrderType', pa.int64()),
    ('Price', pa.int64()),
    ('SecuCode', pa.int64()),
    ('TradingDay', pa.int64()),
    ('Volume', pa.float64()),
    #('__index_level_0__', pa.int64()),
    # ... 你可以根据需要把所有百个字段写全
])
'''

order_schema = pa.schema([
    ('BizIndex', pa.int32()),
    ('Channel', pa.int32()),
    ('DBOrderID', pa.int64()),
    ('LastPrice', pa.int32()),
    ('OrderID', pa.int64()),
    ('OrderTime', pa.int32()),
    ('OrderType', pa.int32()),
    ('Price', pa.int32()),
    ('SecuCode', pa.int32()),
    ('TradingDay', pa.int32()),
    ('Volume', pa.int64()),
    #('__index_level_0__', pa.int64()),
    # ... 你可以根据需要把所有百个字段写全
])

orderraw_schema = pa.schema([
    ('BizIndex', pa.int32()),
    ('Channel', pa.int32()),
    ('DBOrderID', pa.int64()),
    ('OrderID', pa.int64()),
    ('OrderTime', pa.int32()),
    ('OrderType', pa.int32()),
    ('Price', pa.int32()),
    ('SecuCode', pa.int32()),
    ('TradingDay', pa.int32()),
    ('Volume', pa.int64()),
    #('__index_level_0__', pa.int64()),
    # ... 你可以根据需要把所有百个字段写全
])

index_schema = pa.schema([
    ('ClosePrice', pa.int32()),
    ('HighPrice', pa.int32()),
    ('LowPrice', pa.int32()),
    ('OpenPrice', pa.int32()),
    ('PrevClosePrice', pa.int32()),
    ('SecuCode', pa.int32()),
    ('SeqNo', pa.int64()),
    ('CumTurnover', pa.float64()),
    ('TickTime', pa.int32()),
    ('CumVolume', pa.int64()),
    ('Turnover', pa.float64()),
    ('Volume', pa.int64()),
    #('__index_level_0__', pa.int64()),
    # ... 你可以根据需要把所有百个字段写全
])

def process_l2_snapshot_data(srcpath, target_stocks):
    # 1. 将文件映射为 Dataset 对象（此时不读入任何实际数据，只读元数据）
    dataset = ds.dataset(srcpath, format="parquet", schema=snapshot_schema)
    metadata = pq.read_metadata(srcpath)
    # 打印所有列名
    print("所有列名：", metadata.schema.names)

    # 如果想看列名 + 数据类型（如 int64, string）
    print("\n详细结构：")
    print(metadata.schema)
    
    # 2. 创建扫描器，同时指定：
    #    - columns: 只要核心列（再次瘦身）
    #    - filter:  只药目标股票（方案二的过滤）
    columns=['SecuCode', 'TickTime', 'TickTimeDiff', 'Price', 'DealNum'],
    scanner = dataset.scanner(
        filter=ds.field('SecuCode').isin(target_stocks)
    )
    
    # 3. 开始流式迭代（方案三的分块）
    # to_batches() 会返回一个迭代器，每次只吐出一小部分符合条件的数据
    for record_batch in scanner.to_batches():
        
        # 将这一小块数据转换为 Pandas DataFrame
        chunk_df = record_batch.to_pandas()
        
        # 如果这一块刚好为空，跳过
        if chunk_df.empty:
            continue
            
        # --- 在这里编写你的量化清洗或指标计算逻辑 ---
        # 此时的 chunk_df 内存极小，通常只有几 MB 到十几 MB
        # 示例：按股票代码进一步分群处理
        for stock_code, group in chunk_df.groupby('SecuCode'):
            # 执行你的出池判断、订单簿还原、或者指标计算
            pass
            
        # 显式清理，确保及时释放内存
        del chunk_df

def process_l2_deal_data(srcpath, target_stocks):
    # 1. 将文件映射为 Dataset 对象（此时不读入任何实际数据，只读元数据）
    dataset = ds.dataset(srcpath, format="parquet", schema=deal_schema)
    metadata = pq.read_metadata(srcpath)
    # 打印所有列名
    print("所有列名：", metadata.schema.names)

    # 如果想看列名 + 数据类型（如 int64, string）
    print("\n详细结构：")
    print(metadata.schema)
    
    # 2. 创建扫描器，同时指定：
    #    - columns: 只要核心列（再次瘦身）
    #    - filter:  只药目标股票（方案二的过滤）
    columns=['SecuCode', 'DealTime', 'Side', 'BuyID', 'SellID', 'DealID'],
    scanner = dataset.scanner(
        filter=ds.field('SecuCode').isin(target_stocks)
    )
    
    # 3. 开始流式迭代（方案三的分块）
    # to_batches() 会返回一个迭代器，每次只吐出一小部分符合条件的数据
    for record_batch in scanner.to_batches():
        
        # 将这一小块数据转换为 Pandas DataFrame
        chunk_df = record_batch.to_pandas()
        
        # 如果这一块刚好为空，跳过
        if chunk_df.empty:
            continue
            
        # --- 在这里编写你的量化清洗或指标计算逻辑 ---
        # 此时的 chunk_df 内存极小，通常只有几 MB 到十几 MB
        # 示例：按股票代码进一步分群处理
        for stock_code, group in chunk_df.groupby('SecuCode'):
            # 执行你的出池判断、订单簿还原、或者指标计算
            pass
            
        # 显式清理，确保及时释放内存
        del chunk_df

def process_l2_order_data(srcpath, target_stocks):
    # 1. 将文件映射为 Dataset 对象（此时不读入任何实际数据，只读元数据）
    dataset = ds.dataset(srcpath, format="parquet", schema=order_schema)
    metadata = pq.read_metadata(srcpath)
    # 打印所有列名
    print("所有列名：", metadata.schema.names)

    # 如果想看列名 + 数据类型（如 int64, string）
    print("\n详细结构：")
    print(metadata.schema)
    
    # 2. 创建扫描器，同时指定：
    #    - columns: 只要核心列（再次瘦身）
    #    - filter:  只药目标股票（方案二的过滤）
    columns=['SecuCode', 'OrderTime', 'OrderType', 'OrderID', 'DBOrderID', 'Volume'],
    scanner = dataset.scanner(
        filter=ds.field('SecuCode').isin(target_stocks)
    )
    
    # 3. 开始流式迭代（方案三的分块）
    # to_batches() 会返回一个迭代器，每次只吐出一小部分符合条件的数据
    for record_batch in scanner.to_batches():
        
        # 将这一小块数据转换为 Pandas DataFrame
        chunk_df = record_batch.to_pandas()
        
        # 如果这一块刚好为空，跳过
        if chunk_df.empty:
            continue
            
        # --- 在这里编写你的量化清洗或指标计算逻辑 ---
        # 此时的 chunk_df 内存极小，通常只有几 MB 到十几 MB
        # 示例：按股票代码进一步分群处理
        for stock_code, group in chunk_df.groupby('SecuCode'):
            # 执行你的出池判断、订单簿还原、或者指标计算
            pass
            
        # 显式清理，确保及时释放内存
        del chunk_df

def process_l2_orderraw_data(srcpath, target_stocks):
    # 1. 将文件映射为 Dataset 对象（此时不读入任何实际数据，只读元数据）
    dataset = ds.dataset(srcpath, format="parquet", schema=order_schema)
    metadata = pq.read_metadata(srcpath)
    # 打印所有列名
    print("所有列名：", metadata.schema.names)

    # 如果想看列名 + 数据类型（如 int64, string）
    print("\n详细结构：")
    print(metadata.schema)
    
    # 2. 创建扫描器，同时指定：
    #    - columns: 只要核心列（再次瘦身）
    #    - filter:  只药目标股票（方案二的过滤）
    columns=['SecuCode', 'OrderTime', 'OrderType', 'OrderID', 'DBOrderID', 'Volume'],
    scanner = dataset.scanner(
        filter=ds.field('SecuCode').isin(target_stocks)
    )
    
    # 3. 开始流式迭代（方案三的分块）
    # to_batches() 会返回一个迭代器，每次只吐出一小部分符合条件的数据
    for record_batch in scanner.to_batches():
        
        # 将这一小块数据转换为 Pandas DataFrame
        chunk_df = record_batch.to_pandas()
        
        # 如果这一块刚好为空，跳过
        if chunk_df.empty:
            continue
            
        # --- 在这里编写你的量化清洗或指标计算逻辑 ---
        # 此时的 chunk_df 内存极小，通常只有几 MB 到十几 MB
        # 示例：按股票代码进一步分群处理
        for stock_code, group in chunk_df.groupby('SecuCode'):
            # 执行你的出池判断、订单簿还原、或者指标计算
            pass
            
        # 显式清理，确保及时释放内存
        del chunk_df

def process_l2_index_data(srcpath, target_stocks):
    # 1. 将文件映射为 Dataset 对象（此时不读入任何实际数据，只读元数据）
    dataset = ds.dataset(srcpath, format="parquet", schema=index_schema)
    metadata = pq.read_metadata(srcpath)
    # 打印所有列名
    print("所有列名：", metadata.schema.names)

    # 如果想看列名 + 数据类型（如 int64, string）
    print("\n详细结构：")
    print(metadata.schema)
    
    # 2. 创建扫描器，同时指定：
    #    - columns: 只要核心列（再次瘦身）
    #    - filter:  只药目标股票（方案二的过滤）
    columns=['SecuCode', 'ClosePrice', 'HighPrice', 'LowPrice', 'OpenPrice', 'Volume'],
    scanner = dataset.scanner(
        filter=ds.field('SecuCode').isin(target_stocks)
    )
    
    # 3. 开始流式迭代（方案三的分块）
    # to_batches() 会返回一个迭代器，每次只吐出一小部分符合条件的数据
    for record_batch in scanner.to_batches():
        
        # 将这一小块数据转换为 Pandas DataFrame
        chunk_df = record_batch.to_pandas()
        
        # 如果这一块刚好为空，跳过
        if chunk_df.empty:
            continue
            
        # --- 在这里编写你的量化清洗或指标计算逻辑 ---
        # 此时的 chunk_df 内存极小，通常只有几 MB 到十几 MB
        # 示例：按股票代码进一步分群处理
        for stock_code, group in chunk_df.groupby('SecuCode'):
            # 执行你的出池判断、订单簿还原、或者指标计算
            pass
            
        # 显式清理，确保及时释放内存
        del chunk_df

def batch_process_l2_data(base_path, date_str, target_stocks, include_types=None):
    """
    更弹性的重构版：可以通过 include_types 动态指定处理哪些数据
    """
    l2_task_mapping = {
        'snapshot': process_l2_snapshot_data,
        'deal':     process_l2_deal_data,
        'order':    process_l2_order_data,
        'order_raw': process_l2_orderraw_data,
        'index':    process_l2_index_data,
    }
    
    # 如果没传，默认处理全部
    types_to_process = include_types if include_types else l2_task_mapping.keys()
    
    for data_type in types_to_process:
        if data_type not in l2_task_mapping:
            print(f"❌ 未知的 L2 数据类型: {data_type}")
            continue
            
        file_path = base_path / f"{data_type}_{date_str}.parquet"
        if file_path.exists():
            l2_task_mapping[data_type](srcpath=file_path, target_stocks=target_stocks)

def cal_tradingday(pfile, collist=None):
    array_arrow = pq.read_table(pfile, columns=collist).column(0)
    trading_days = array_arrow.to_numpy()

    unique_days, counts = np.unique(trading_days, return_counts=True)
    col_str = ", ".join(collist)
    # 3. 优雅地打印结果
    print(f"📊 每日 Level 2 逐笔数据量统计：")
    print("-" * 35)
    print(f"{col_str:<15} | {'频次 (Counts)' :<15}")
    print("-" * 35)

    for day, count in zip(unique_days, counts):
        # 如果读取出来的日期是 bytes 类型（Parquet 中常见的 string 表现），需要 decode 一下
        if isinstance(day, bytes):
            day = day.decode('utf-8')
        print(f"{day:<15} | {count:<15,}") # :, 可以在数字中自动加上千分位逗号，方便肉眼阅读

    print("-" * 35)
    print(f"总计数据量: {len(trading_days):,} 行")

def load_and_merge_orderflow(order_file, deal_file, snapshot_file):
    #cal_tradingday(order_file, collist=['BizIndex'])
    #cal_tradingday(order_file, collist=['Channel'])
    #cal_tradingday(order_file, collist=['LastPrice'])
    cal_tradingday(order_file, collist=['OrderType'])
    cal_tradingday(deal_file, collist=['Side'])

    
    # 1. 读取数据（如果是 Parquet 效率极高）
    order_columns = ['BizIndex', 'Channel', 'DBOrderID', 'LastPrice', 'OrderID', 'OrderTime', 'OrderType', 'Price', 'SecuCode', 'TradingDay', 'Volume', '__index_level_0__']
    deal_columns = ['BizIndex', 'BuyID', 'Channel', 'DealID', 'DealTime', 'Price', 'SecuCode', 'SellID', 'Side', 'TradingDay', 'Volume', '__index_level_0__']

    # 💡 行列同时过滤：既要特定行（filters），又要特定列（columns）
    df_order = pd.read_parquet(order_file, schema=order_schema, columns=order_columns, filters=[('TradingDay', '==', 20260529)])   
    df_deal = pd.read_parquet(deal_file, schema=deal_schema, columns=deal_columns, filters=[('TradingDay', '==', 20260529)])
    df_snapshot = pd.read_parquet(snapshot_file, schema=snapshot_schema, filters=[('TradingDay', '==', 20260529)]) 
    

def rebuild_order_book(df_all):
    # 存储当前活跃的订单清单: { order_seq: [price, remaining_qty, side] }
    active_orders = {}
    
    # 存储盘口价格档位: { price: total_qty }
    bid_levels = {} # 买盘盘口
    ask_levels = {} # 卖盘盘口
    
    # 用于收集最终日线特征的容器
    daily_features = []

    # 流式遍历整条时间线
    for row in df_all.itertuples():
        # ---- 1. 处理委托事件 ----
        if row.event_type == 'E':
            # 记录新订单
            active_orders[row.ApplSeqNum] = [row.Price, row.Qty, row.Side]
            # 更新盘口累计量
            levels = bid_levels if row.Side == 'B' else ask_levels
            levels[row.Price] = levels.get(row.Price, 0) + row.Qty
            
        # ---- 2. 处理成交/撤单事件 ----
        elif row.event_type == 'X':
            # 情况 A：这是一笔撤单
            if getattr(row, 'ExecType', None) == 'C': 
                # 找出是谁撤的单（看买卖双方哪个订单号存在）
                target_seq = row.BidSeq if row.BidSeq > 0 else row.AskSeq
                if target_seq in active_orders:
                    price, rem_qty, side = active_orders[target_seq]
                    levels = bid_levels if side == 'B' else ask_levels
                    # 盘口减去撤单量
                    levels[price] = max(0, levels.get(price, 0) - row.Qty)
                    if levels[price] == 0: del levels[price]
                    # 从活跃订单中抹去
                    del active_orders[target_seq]
            
            # 情况 B：这是正常成交
            else:
                # 削减买方挂单量
                if row.BidSeq in active_orders:
                    p, rem_q, s = active_orders[row.BidSeq]
                    new_q = rem_q - row.Qty
                    bid_levels[p] = max(0, bid_levels.get(p, 0) - row.Qty)
                    if bid_levels[p] == 0: del bid_levels[p]
                    if new_q <= 0: del active_orders[row.BidSeq]
                    else: active_orders[row.BidSeq][1] = new_q
                    
                # 削减卖方挂单量
                if row.AskSeq in active_orders:
                    p, rem_q, s = active_orders[row.AskSeq]
                    new_q = rem_q - row.Qty
                    ask_levels[p] = max(0, ask_levels.get(p, 0) - row.Qty)
                    if ask_levels[p] == 0: del ask_levels[p]
                    if new_q <= 0: del active_orders[row.AskSeq]
                    else: active_orders[row.AskSeq][1] = new_q

        # 💡 [关键点] 在这里，你可以每隔 3 秒（模拟快照）或者在特定时间点
        # 提取当前买卖盘口（bid_levels 和 ask_levels）的十档数据，算完特征后直接丢弃盘口
        # 绝不要把每一步的盘口全部存入硬盘！

def process_single_stock_day(date, stock_code):
    daily_feature_dict={}

    #order_file = CONFIG.base_path['A_LEVEL2']/f"202605/order/{stock_code}.parquet"
    #deal_file = CONFIG.base_path['A_LEVEL2']/f"202605/deal/{stock_code}.parquet"
    #snapshot_file = CONFIG.base_path['A_LEVEL2']/f"202605/snapshot/{stock_code}.parquet"

    order_file = CONFIG.base_path['A_LEVEL2']/f"order/2026/202605/{stock_code}.parquet"
    deal_file = CONFIG.base_path['A_LEVEL2']/f"deal/2026/202605/{stock_code}.parquet"
    snapshot_file = CONFIG.base_path['A_LEVEL2']/f"snapshot/2026/202605/{stock_code}.parquet"
    
    if not (os.path.exists(order_file) and os.path.exists(deal_file) and os.path.exists(snapshot_file)):
        return None
        
    # 1. 融合时序
    #df_all = load_and_merge_orderflow(order_file, deal_file, snapshot_file)
    df_all = playback_and_rebuild(order_file, deal_file, snapshot_file, date=date)
    # 2. 动态重构并提取特征
    #daily_feature_dict = rebuild_order_book_and_extract(df_all)
    
    return daily_feature_dict

def stream_l2_timeline(order_file, deal_file, snapshot_file, batch_size=50000, date=20260101):
    """
    流式双指针归并：同时拉取订单流与成交流，就地按照时间线对齐。
    """
    # 精准提取所需字段
    order_columns = ['Channel', 'DBOrderID', 'LastPrice', 'OrderID', 'OrderTime', 'OrderType', 'Price', 'Volume']
    deal_columns = ['BuyID', 'Channel', 'DealID', 'DealTime', 'Price', 'SellID', 'Side', 'Volume']
    snapshot_columns = ['DealNum', 'Price', 'TickTime', 'TickTimeDiff', 'TotalAskVolume', 'TotalBidVolume', 'TotalDealNum', 'TotalTurnover', 'TotalVolume', 'Turnover', 'Volume', 'WeightAskPrice', 'WeightBidPrice']
    for i in range(1,11):
        snapshot_columns.extend([f'BidPrice{i}', f'BidVolume{i}', f'BidOrder{i}'])
        snapshot_columns.extend([f'AskPrice{i}', f'AskVolume{i}', f'AskOrder{i}'])

    #df_order = pd.read_parquet(order_file, schema=order_schema, columns=order_columns, filters=[('TradingDay', '==', 20260529)])   
    #df_deal = pd.read_parquet(deal_file, schema=deal_schema, columns=deal_columns, filters=[('TradingDay', '==', 20260529)])
    #df_snapshot = pd.read_parquet(snapshot_file, columns=snapshot_columns, schema=snapshot_schema, filters=[('TradingDay', '==', 20260529)])  
    order_stream = stream_from_parquet(order_file, schema=order_schema, cols=order_columns, batch_size=batch_size, filters=(ds.field('TradingDay') == date))
    deal_stream = stream_from_parquet(deal_file, schema=deal_schema, cols=deal_columns, batch_size=batch_size, filters=(ds.field('TradingDay') == date))
    snapshot_stream = stream_from_parquet(snapshot_file, schema=snapshot_schema, cols=snapshot_columns, batch_size=batch_size, filters=(ds.field('TradingDay') == date))
    
    curr_order = next(order_stream, None)
    curr_deal = next(deal_stream, None)
    curr_snapshot = next(snapshot_stream, None)
    
    while curr_order or curr_deal or curr_snapshot:
        # 💡 时间相同时，排序优先级：Order(0) -> Deal(1) -> Snapshot(2) 输出
        candidates = []
        if curr_order: candidates.append((curr_order.OrderTime, 0, 'Order', curr_order))
        if curr_deal:  candidates.append((curr_deal.DealTime, 1, 'Deal', curr_deal))
        if curr_snapshot:  candidates.append((curr_snapshot.TickTime, 2, 'Snapshot', curr_snapshot))

        # 按时间升序排序，时间相同时按优先级(0->1->2)升序排序
        candidates.sort(key=lambda x: (x[0], x[1]))
        best_type, best_row = candidates[0][2], candidates[0][3]

        yield best_type, best_row

        if best_type == 'Order':    
            curr_order = next(order_stream, None)
        elif best_type == 'Deal':   
            curr_deal = next(deal_stream, None)
        elif best_type == 'Snapshot': 
            curr_snapshot = next(snapshot_stream, None)
            

def stream_from_parquet(file_path, schema=None, cols=None, batch_size=5000, filters=None):
    """
    利用 Dataset API 实现真正的【磁盘分块流式读取 + 过滤】，彻底无感避开隐藏索引列问题
    """
    # 1. 将文件映射为 Dataset 对象（仅读取元数据，O(1) 速度极快）
    dataset = ds.dataset(file_path, format="parquet")

    if schema is not None:
        target_cols = cols if cols else dataset.schema.names
        active_schema = pa.schema([schema.field(name) for name in target_cols if name in schema.names])
    else:
        active_schema = None

    for batch in dataset.to_batches(columns=cols, filter=filters, batch_size=batch_size):
        if active_schema is not None:
                batch = batch.cast(active_schema)
            
        # 1. 转换为 DataFrame
        batch_df = batch.to_pandas()
        
        # 💡 2. 核心清洗：在这里将原始金额（元）硬核整型化为（分）
        # 先乘以 100，再用 .round() 消除浮点数乘法带来的微小毛刺（如 .0000001），最后安全转为 int64
        if 'Turnover' in batch_df.columns:
            batch_df['Turnover'] = (batch_df['Turnover'] * 100).round().astype('int64')
            
        if 'TotalTurnover' in batch_df.columns:
            batch_df['TotalTurnover'] = (batch_df['TotalTurnover'] * 100).round().astype('int64')

        if 'CumTurnover' in batch_df.columns:
            batch_df['CumTurnover'] = (batch_df['CumTurnover'] * 100).round().astype('int64')

        if 'TickTime' in batch_df.columns:
            batch_df['TickTime'] = batch_df['TickTime'] - TIMESHIFT
            
        # 如果你的 local_tick_volume 也是放大 100 倍的，也可以在这里把 Volume 统一：
        # if 'Volume' in batch_df.columns:
        #     batch_df['Volume'] = batch_df['Volume'].round().astype('int64')

        # 3. 吐出最纯净的整型流
        for row in batch_df.itertuples(index=False):
            yield row

def dump_topN_nodes(ob, side='B', n_levels=10):
    """
    直观打印 TopN 档位及底层双向链表节点状态，用于排查账本不一致 Bug
    """
    book = ob.bids if side == 'B' else ob.asks
    # Bids 从大到小排，Asks 从小到大排
    reverse_sort = True if side == 'B' else False
    sorted_prices = sorted(book.keys(), reverse=reverse_sort)[:n_levels]
    
    side_str = "🟢 BIDS (买盘)" if side == 'B' else "🔴 ASKS (卖盘)"
    
    print(f"\n{'='*40} {side_str} TOP {n_levels} 链表深度剖析 {'='*40}")
    print(f"{'档位':<4} | {'价格 (Price)':<12} | {'总挂单量':<10} | {'单数':<5} |  双向链表完整节点视图 (Head -> Tail)")
    print("-" * 110)
    
    for i, price in enumerate(sorted_prices, 1):
        price_list = book[price]
        
        # 1. 顺着链表 head -> next 一路向后遍历
        node_chain = []
        curr = price_list.head
        
        # 防死循环安全计数（防止双向链表成环）
        loop_guard = 0 
        while curr:
            # 格式化单个节点信息：[订单ID | 数量]
            node_chain.append(f"🔗 [ID:{curr.ref_id}|Qty:{curr.qty}]")
            curr = curr.next
            
            loop_guard += 1
            if loop_guard > 1000: # 异常防死锁
                node_chain.append("... 🚨 检测到链表成环死循环! 🚨")
                break
                
        # 2. 拼装成直观的箭头链路
        chain_visual = " ➔ ".join(node_chain) if node_chain else "Ø (空链表)"
        
        # 3. 打印当前档位聚合数据与链路
        print(f"L{i:<3} | {price:<12.2f} | {price_list.total_volume:<10} | {price_list.order_count:<5} | {chain_visual}")
        
    print(f"{'='*102}\n")

def playback_and_rebuild(order_file, deal_file, snapshot_file, batch_size=50000, date=20260101):
    """
    从磁盘流式读取并构建订单薄
    :param order_file: 逐笔委托 parquet 文件路径
    :param deal_file: 逐笔成交 parquet 文件路径
    :param market: 市场标识，'SH' 代表沪市，'SZ' 代表深市
    :param batch_size: 磁盘缓存块大小
    """
    # 初始化上一轮构建好的 Level2OrderBook 实例 (Method 3 架构)
    ob = OrderBook() 

    # 初始化本地统计计数器（用于校验单Tick和累计的流式指标）
    local_tick_deal_num = 0
    local_tick_volume = 0
    local_tick_turnover = 0
    
    local_total_dealnum = 0         #累计成交笔数
    local_total_volume = 0          #累计成交量
    local_total_turnover = 0        #累计成交额

    target_decimals = None

    latest_snapshot = None          # 缓存最新的官方快照行数据
    latest_snapshto_ticktime = 0
    latest_snapshto_ticktime_up = 0
    snapshot_verified = True
    weightprice_notmatch = False

    # 缓存快照的十档字典结构
    cached_snap_bids, cached_snap_asks = [], []
    cached_snap_bid_dict, cached_snap_ask_dict = {}, {}
    
    # 直接消费磁盘流，内存开销恒定
    for event_type, row in stream_l2_timeline(order_file, deal_file, snapshot_file, batch_size, date=date):  
        # 提取当前事件的时间戳 (格式形如: 91501420)
        event_time = getattr(row, 'OrderTime', getattr(row, 'DealTime', getattr(row, 'TickTime', 0)))


        if event_type in ['Order', 'Deal']:
            if not snapshot_verified:
                if (event_time // 1000) == ((latest_snapshto_ticktime+TIMESHIFT) // 1000):
                    my_tot_bid_vol, my_tot_ask_vol, my_w_bid_p, my_w_ask_p = ob.get_orderbook_stats()

                    my_w_bid_p = round(my_w_bid_p) if target_decimals == 0 else round(my_w_bid_p, target_decimals)
                    my_w_ask_p = round(my_w_ask_p) if target_decimals == 0 else round(my_w_ask_p, target_decimals)

                    if my_tot_bid_vol == latest_snapshot.TotalBidVolume and my_tot_ask_vol == latest_snapshot.TotalAskVolume and local_total_dealnum == latest_snapshot.TotalDealNum:
                        my_bids = ob.get_topN_snapshot('B', n_levels=10)
                        my_asks = ob.get_topN_snapshot('S', n_levels=10)
                        my_bid_dict = {p: (v, c) for p, v, c in my_bids}
                        my_ask_dict = {p: (v, c) for p, v, c in my_asks}
                    
                        match_levels = (my_bid_dict == cached_snap_bid_dict) and (my_ask_dict == cached_snap_ask_dict)

                        # 🎯 黄金匹配点触发：四维断言全部通过！说明本地成功锁定了快照在这一秒内的微秒级生成瞬间
                        if match_levels:
                            # ---------------- C. 检验：全盘委托总量及加权均价 ----------------
                            # 从本地账本拉取全盘挂单快照 (包含10档外的深层挂单)          
                            if abs(my_w_bid_p - latest_snapshot.WeightBidPrice) > 0.2 or abs(my_w_ask_p - latest_snapshot.WeightAskPrice) > 0.2:
                                weightprice_notmatch = True
                            else:
                                if weightprice_notmatch:
                                    print(f"🔍 [{event_time}|{event_type}:{latest_snapshto_ticktime+TIMESHIFT}] 超时全盘卖方/买方不一致已恢复")
                                    weightprice_notmatch = False
                                snapshot_verified = True
                                # 每一个 Snapshot 落地后，单 Tick 计数器必须清零，开始下一轮3秒的流式累加
                                local_tick_deal_num = 0
                                local_tick_volume = 0
                                local_tick_turnover = 0
                                #print(f"🔍 [{event_time}|{event_type}:{latest_snapshto_ticktime+TIMESHIFT}] snapshot秒内无挂单Order, 无成交Deal")


                if event_time > latest_snapshto_ticktime_up:
                    my_tot_bid_vol, my_tot_ask_vol, my_w_bid_p, my_w_ask_p = ob.get_orderbook_stats()

                    my_w_bid_p = round(my_w_bid_p) if target_decimals == 0 else round(my_w_bid_p, target_decimals)
                    my_w_ask_p = round(my_w_ask_p) if target_decimals == 0 else round(my_w_ask_p, target_decimals)

                    if my_tot_bid_vol == latest_snapshot.TotalBidVolume and my_tot_ask_vol == latest_snapshot.TotalAskVolume and local_total_dealnum == latest_snapshot.TotalDealNum:
                        my_bids = ob.get_topN_snapshot('B', n_levels=10)
                        my_asks = ob.get_topN_snapshot('S', n_levels=10)
                        my_bid_dict = {p: (v, c) for p, v, c in my_bids}
                        my_ask_dict = {p: (v, c) for p, v, c in my_asks}
                    
                        match_levels = (my_bid_dict == cached_snap_bid_dict) and (my_ask_dict == cached_snap_ask_dict)

                        # 🎯 黄金匹配点触发：四维断言全部通过！说明本地成功锁定了快照在这一秒内的微秒级生成瞬间
                        if match_levels:
                            # ---------------- C. 检验：全盘委托总量及加权均价 ----------------
                            # 从本地账本拉取全盘挂单快照 (包含10档外的深层挂单)          
                            if abs(my_w_bid_p - latest_snapshot.WeightBidPrice) > 0.2 or abs(my_w_ask_p - latest_snapshot.WeightAskPrice) > 0.2:
                                print(f"🔍 [{event_time}|{event_type}:{latest_snapshto_ticktime+TIMESHIFT}] 超时snapshot无法匹配，累积卖单/买单加权价不匹配")
                                weightprice_notmatch = True
                            else:
                                if weightprice_notmatch:
                                    print(f"🔍 [{event_time}|{event_type}:{latest_snapshto_ticktime+TIMESHIFT}] 超时全盘卖方/买方不一致已恢复")
                                    weightprice_notmatch = False
                                snapshot_verified = True
                                # 每一个 Snapshot 落地后，单 Tick 计数器必须清零，开始下一轮3秒的流式累加
                                local_tick_deal_num = 0
                                local_tick_volume = 0
                                local_tick_turnover = 0
                                #print(f"🔍 [{event_time}|{event_type}:{latest_snapshto_ticktime+TIMESHIFT}] snapshot秒内无挂单Order, 无成交Deal")
                        else:
                            snapshot_verified = True
                            print(f"🔍 [{event_time}|{event_type}:{latest_snapshto_ticktime+TIMESHIFT}] 超时snapshot无法匹配，十档不匹配")
                            #dump_topN_nodes(ob, side='B', n_levels=10)
                    else:
                        snapshot_verified = True
                        if local_total_dealnum != latest_snapshot.TotalDealNum or local_total_volume != latest_snapshot.TotalVolume or local_total_turnover != latest_snapshot.TotalTurnover:
                            print(f"⚡ [{event_time}|{event_type}:{latest_snapshto_ticktime+TIMESHIFT}] 超时累计成交断层 | "
                                f"本地累计(笔数:{local_total_dealnum},量:{local_total_volume},额:{local_total_turnover}) vs "
                                f"官方累积(笔数:{latest_snapshot.TotalDealNum},量:{latest_snapshot.TotalVolume},额:{latest_snapshot.TotalTurnover})")
                        if local_tick_volume != latest_snapshot.Volume or abs(local_tick_turnover-latest_snapshot.Turnover)>50:
                            print(f"🚨 [{event_time}|{event_type}:{latest_snapshto_ticktime+TIMESHIFT}] 超时单Tick成交异常 | "
                                f"本地Tick(笔数:{local_tick_deal_num},量:{local_tick_volume},额:{local_tick_turnover}) vs "
                                f"官方Tick(笔数:{latest_snapshot.DealNum},量:{latest_snapshot.Volume},额:{latest_snapshot.Turnover})")
                        if abs(my_w_bid_p - latest_snapshot.WeightBidPrice) > 0.2:
                            print(f"🔍 [{event_time}|{event_type}:{latest_snapshto_ticktime+TIMESHIFT}] 超时全盘买方不一致 | "
                                f"自制总买量:{my_tot_bid_vol} (官方:{latest_snapshot.TotalBidVolume}) | 自制买均价:{my_w_bid_p:.1f} (官方:{latest_snapshot.WeightBidPrice:.1f})")
                                
                        if abs(my_w_ask_p - latest_snapshot.WeightAskPrice) > 0.2:
                            print(f"🔍 [{event_time}|{event_type}:{latest_snapshto_ticktime+TIMESHIFT}] 超时全盘卖方不一致 | "
                                f"自制总卖量:{my_tot_ask_vol} (官方:{latest_snapshot.TotalAskVolume}) | 自制卖均价:{my_w_ask_p:.1f} (官方:{latest_snapshot.WeightAskPrice:.1f})")

        # ==========================================
        # 核心逻辑 A：处理逐笔委托 (Order Stream)
        # ==========================================
        if event_type == 'Order':
            #print(", ".join(f"{k}={v}" for k, v in row._asdict().items()))
            #print(f"OrderTime:f{event_time}")
            order_id = int(row.OrderID)

            if row.OrderType == 0:    # 委买
                ob.insert_order(order_id, row.Price, row.Volume, 'B', row.OrderTime)
            elif row.OrderType == 10: # 委卖
                ob.insert_order(order_id, row.Price, row.Volume, 'S', row.OrderTime)
            elif row.OrderType == -1: # 撤买 (沪市特有：委托表直接下发撤单记录)
                ob.cancel_order(order_id, row.Volume)
            elif row.OrderType == -11:# 撤卖
                ob.cancel_order(order_id, row.Volume)
                
            elif row.OrderType == 1: #市价 委买
                ob.insert_order(order_id, row.LastPrice, row.Volume, 'B', row.OrderTime)                
            elif row.OrderType == 2:    # 限价 委买
                ob.insert_order(order_id, row.Price, row.Volume, 'B', row.OrderTime)
            elif row.OrderType == 3: #本方最优 委买
                ob.insert_order(order_id, ob.get_bbo_bid(), row.Volume, 'B', row.OrderTime)
            elif row.OrderType == 11: # 市价/本方最优委卖
                ob.insert_order(order_id, row.LastPrice, row.Volume, 'S', row.OrderTime)       
            elif row.OrderType == 12: # 限价 委卖
                ob.insert_order(order_id, row.Price, row.Volume, 'S', row.OrderTime)
            elif row.OrderType == 13: # 市价/本方最优委卖
                ob.insert_order(order_id, ob.get_bbo_ask(), row.Volume, 'S', row.OrderTime)
       
        # ==========================================
        # 核心逻辑 B：处理逐笔成交与撤单 (Deal Stream)
        # ==========================================
        elif event_type == 'Deal':
            #print(", ".join(f"{k}={v}" for k, v in row._asdict().items()))
            #print(f"DealTime:{event_time}")
            side = row.Side
            
            # 1. 正常成交 (0: 主动买入, 1: 主动卖出)
            if side in [0, 1]:
                # 交易所 L2 机制：一笔成交双边扣减
                ob.execute_trade(ref_id=row.BuyID, exec_qty=row.Volume)
                ob.execute_trade(ref_id=row.SellID, exec_qty=row.Volume)
                
                # 增量累加本地的成交统计数据
                if event_time >= 92500000:
                    deal_turnover = row.Price * row.Volume
                    
                    local_tick_deal_num += 1
                    local_tick_volume += row.Volume
                    local_tick_turnover += deal_turnover
                    
                    local_total_dealnum += 1
                    local_total_volume += row.Volume
                    local_total_turnover += deal_turnover
                
            # 2. 独立撤单记录 (深市全部在此处下发，部分清洗后的沪市亦同)
            elif side == -1:  # 买单撤单
                ob.cancel_order(ref_id=row.BuyID, cancel_qty=row.Volume)
            elif side == -11: # df
                ob.cancel_order(ref_id=row.SellID, cancel_qty=row.Volume)

        # ==========================================
        # 核心逻辑 C：快照比对与修正检测 (Snapshot Stream)
        # ==========================================
        elif event_type == 'Snapshot':
            latest_snapshto_ticktime = row.TickTime
            # 💡 9:15 ~ 9:25 属于数据准备阶段，快照静默跳过，不建立基准，也不做断言
            if latest_snapshto_ticktime < 92500000-TIMESHIFT or (latest_snapshto_ticktime>=145700000 - TIMESHIFT and latest_snapshto_ticktime<150000000 - TIMESHIFT):
                latest_snapshot = None
                latest_snapshto_ticktime_up = 0
                snapshot_verified = True
                continue

            #print(f"snapshot:{row.TickTime+TIMESHIFT}")

            if target_decimals is None:
                target_decimals = 0 if row.WeightBidPrice % 1 == 0 else 1
            
            latest_snapshot = row
            latest_snapshto_ticktime_up = latest_snapshto_ticktime + 999 + TIMESHIFT #不能取1000，截止时间为999毫秒，否则超时会计算下一秒的数据。取999时超时判断对应>=，取998时对应>。
            snapshot_verified = False

            weightprice_notmatch = False

            # 性能优化：在 O(1) 阶段提前解析好快照的十档字典，杜绝在逐笔高频循环中重复调用 getattr
            cached_snap_bids = [(getattr(row, f'BidPrice{i}'), int(getattr(row, f'BidVolume{i}')), int(getattr(row, f'BidOrder{i}'))) for i in range(1, 11) if getattr(row, f'BidPrice{i}') > 0]
            cached_snap_asks = [(getattr(row, f'AskPrice{i}'), int(getattr(row, f'AskVolume{i}')), int(getattr(row, f'AskOrder{i}'))) for i in range(1, 11) if getattr(row, f'AskPrice{i}') > 0]
            cached_snap_bid_dict = {p: (v, c) for p, v, c in cached_snap_bids}
            cached_snap_ask_dict = {p: (v, c) for p, v, c in cached_snap_asks}

        if event_type in ['Order', 'Deal']:
            if not snapshot_verified:
                my_tot_bid_vol, my_tot_ask_vol, my_w_bid_p, my_w_ask_p = ob.get_orderbook_stats()


                my_w_bid_p = round(my_w_bid_p) if target_decimals == 0 else round(my_w_bid_p, target_decimals)
                my_w_ask_p = round(my_w_ask_p) if target_decimals == 0 else round(my_w_ask_p, target_decimals)

                if my_tot_bid_vol == latest_snapshot.TotalBidVolume and my_tot_ask_vol == latest_snapshot.TotalAskVolume and local_total_dealnum == latest_snapshot.TotalDealNum:
                    my_bids = ob.get_topN_snapshot('B', n_levels=10)
                    my_asks = ob.get_topN_snapshot('S', n_levels=10)
                    my_bid_dict = {p: (v, c) for p, v, c in my_bids}
                    my_ask_dict = {p: (v, c) for p, v, c in my_asks}
                
                    match_levels = (my_bid_dict == cached_snap_bid_dict) and (my_ask_dict == cached_snap_ask_dict)

                    # 🎯 黄金匹配点触发：四维断言全部通过！说明本地成功锁定了快照在这一秒内的微秒级生成瞬间
                    if match_levels:
                        # ---------------- C. 检验：全盘委托总量及加权均价 ----------------
                        # 从本地账本拉取全盘挂单快照 (包含10档外的深层挂单)          
                        if abs(my_w_bid_p - latest_snapshot.WeightBidPrice) > 0.2 or abs(my_w_ask_p - latest_snapshot.WeightAskPrice) > 0.2:
                            weightprice_notmatch = True
                        else:
                            if weightprice_notmatch:
                                print(f"🔍 [{event_time}|{event_type}:{latest_snapshto_ticktime+TIMESHIFT}] 全盘卖方/买方不一致已恢复")
                                weightprice_notmatch = False
                            snapshot_verified = True
                            # 每一个 Snapshot 落地后，单 Tick 计数器必须清零，开始下一轮3秒的流式累加
                            local_tick_deal_num = 0
                            local_tick_volume = 0
                            local_tick_turnover = 0
                        pass

        # 💡 [量化切片提示]：可在时序推进时就地计算订单薄因子，无需保留历史行，即用即扔。
        # if row.OrderTime % 3000 == 0:
        #     features = ob.get_snapshot(5)
            
    return ob

if __name__ == '__main__':
    #stock_df = get_allstock()
    stocklist = [2631, 688017, 688693] #(20260529, "002631"), (20260529, "688017"), (20260529, "688693"), 
    tasks = [(20260529, "003031"), (20260529, "000531"), (20260529, "002008"), (20260529, "600641"), (20260529, "601101"), (20260529, "603032"), (20260529, "605186"), (20260529, "688545"), (20260529, "300776"), (20260529, "301458")]
    #process_type = ['snapshot', 'deal', 'order', 'order_raw', 'index']
    process_type = ['index']
    base_dir = CONFIG.base_path['LEVEL2_TEMP']

    # 场景 A：我想跑一整天全量数据
    #batch_process_l2_data(base_dir, "20260522", stocklist)

    # 场景 B：由于发生订单簿错位，今天我只想重新跑逐笔委托和逐笔成交来还原订单簿
    # batch_process_l2_data(base_dir, "20260522", stocklist, include_types=process_type)
    results = []
    #with ProcessPoolExecutor(max_workers=os.cpu_count() - 2) as executor:
        # 并发执行，吃满多核
        #results = list(executor.map(lambda p: process_single_stock_day(*p), tasks))

    for task in tasks:
        task_result = process_single_stock_day(task[0], task[1])
        if task_result is not None:
            results.append(task_result)
        
    # 将所有人最终提取出的日线特征聚合成一个紧凑的明细表，直接扔进你的基础日线回测系统
    #df_l2_features = pd.DataFrame([r for r in results if r is not None])
    #df_l2_features.to_parquet("E:/gitcode/AXOrderBook/data/daily_l2_features.parquet")