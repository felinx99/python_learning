import os
import pyarrow as pa
import pandas as pd
import numpy as np
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from backtrader import order
from collections import namedtuple
from concurrent.futures import ProcessPoolExecutor
from common import CONFIG, DATAFRAME
from .util.order import OrderNode, OrderBook


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

def stream_from_snapshot(file_path, batch_size=5000, filters=None):
    """
    利用 Dataset API 实现真正的【磁盘分块流式读取 + 过滤】，并动态计算跨批次 DealTimeNext
    """
    # 1. 将文件映射为 Dataset 对象（仅读取元数据，O(1) 速度极快）
    snapshot_columns = ['DealNum', 'Price', 'TickTime', 'TickTimeDiff', 'TotalAskVolume', 'TotalBidVolume', 'TotalDealNum', 'TotalTurnover', 'TotalVolume', 'Turnover', 'Volume', 'WeightAskPrice', 'WeightBidPrice']
    for i in range(1,11):
        snapshot_columns.extend([f'BidPrice{i}', f'BidVolume{i}', f'BidOrder{i}'])
        snapshot_columns.extend([f'AskPrice{i}', f'AskVolume{i}', f'AskOrder{i}'])
    schema = snapshot_schema
    dataset = ds.dataset(file_path, format="parquet")

    if schema is not None:
        active_schema = pa.schema([schema.field(name) for name in snapshot_columns if name in schema.names])
    else:
        active_schema = None


    for batch in dataset.to_batches(columns=snapshot_columns, filter=filters, batch_size=batch_size):
        if active_schema is not None:
            batch = batch.cast(active_schema)
            
        # 转换当前分块为 DataFrame
        batch_df = batch.to_pandas()
        
        # 核心清洗：金额转分逻辑（保持你原有的逻辑不变）
        if 'Turnover' in batch_df.columns:
            batch_df['Turnover'] = (batch_df['Turnover'] * 100).round().astype('int64')
        if 'TotalTurnover' in batch_df.columns:
            batch_df['TotalTurnover'] = (batch_df['TotalTurnover'] * 100).round().astype('int64')
        if 'CumTurnover' in batch_df.columns:
            batch_df['CumTurnover'] = (batch_df['CumTurnover'] * 100).round().astype('int64')
        if 'WeightBidPrice' in batch_df.columns:
            batch_df['WeightBidPrice'] = (batch_df['WeightBidPrice'] * 10).round().astype('int32')
        if 'WeightAskPrice' in batch_df.columns:
            batch_df['WeightAskPrice'] = (batch_df['WeightAskPrice'] * 10).round().astype('int32')

        # 3. 逐行处理并利用状态机流式吐出
        for row in batch_df.itertuples(index=False):
            decimals = 0 if row.WeightBidPrice % 10 == 0 else 1
            yield row, decimals, row.TickTime+2000



def stream_from_deal(file_path, batch_size=5000, filters=None):
    """
    利用 Dataset API 实现真正的【磁盘分块流式读取 + 过滤】，并动态计算跨批次 DealTimeNext
    """
    # 1. 将文件映射为 Dataset 对象（仅读取元数据，O(1) 速度极快）
    # 精准提取所需字段
    cols = ['BuyID', 'Channel', 'DealID', 'DealTime', 'Price', 'SellID', 'Side', 'Volume']
    schema = deal_schema
    dataset = ds.dataset(file_path, format="parquet")

    if schema is not None:
        target_cols = cols if cols else dataset.schema.names
        active_schema = pa.schema([schema.field(name) for name in target_cols if name in schema.names])
    else:
        active_schema = None

    deal_buffer = []  # 存放处于“等待未来有效成交时间”的行数据
    RowNextType = None     # 动态生成的 namedtuple 类型，避免重复创建

    for batch in dataset.to_batches(columns=cols, filter=filters, batch_size=batch_size):
        if active_schema is not None:
            batch = batch.cast(active_schema)
            
        # 转换当前分块为 DataFrame
        batch_df = batch.to_pandas()
        
         # 3. 逐行处理并利用状态机流式吐出
        for row in batch_df.itertuples(index=False):
            # 延迟初始化：根据当前行的 fields，动态追加 'DealTimeNext' 字段名
            if RowNextType is None:
                RowNextType = namedtuple('RowNext', row._fields + ('DealTimeNext',))
            
            if row.Side in (-1, -11):
                # 🦖 场景 A：当前是撤单行。它不能作为别人的 Next，且它自己也在等 Next，直接扔进缓冲区
                deal_buffer.append(row)
            else:
                # 🚀 场景 B：当前是非撤单的有效行！
                # 它的 DealTime，就是目前缓冲区里所有孤儿节点苦苦等待的 'DealTimeNext'
                for b_row in deal_buffer:
                    yield RowNextType(*b_row, row.DealTime)
                deal_buffer.clear()
                
                # 当前行自己未来也需要被下一个有效行拯救，因此把自己放入缓冲区
                deal_buffer.append(row)                                         
    
    # 💡 4. 边界收尾：全天数据读完后，冲刷最后一包滞留在缓冲区的数据    
    if deal_buffer:
        for b_row in deal_buffer:
            # 已经到了全天数据的尽头，后面再也没有有效成交了，DealTimeNext 填充为 18:00:00.000秒
            yield RowNextType(*b_row, 150000000)
        deal_buffer.clear()

def stream_from_order(file_path, batch_size=5000, filters=None):
    """
    利用 Dataset API 实现真正的【磁盘分块流式读取 + 过滤】，并动态计算跨批次 DealTimeNext
    """
    # 1. 将文件映射为 Dataset 对象（仅读取元数据，O(1) 速度极快）
    cols = ['Channel', 'DBOrderID', 'LastPrice', 'OrderID', 'OrderTime', 'OrderType', 'Price', 'Volume']
    schema = order_schema
    dataset = ds.dataset(file_path, format="parquet")

    if schema is not None:
        target_cols = cols if cols else dataset.schema.names
        active_schema = pa.schema([schema.field(name) for name in target_cols if name in schema.names])
    else:
        active_schema = None


    current_order_time = None
    order_buffer = []

    def flush_and_reorder_buffer(bucket):
        """
        内部辅助函数：对同一时间戳内的所有订单按照业务规则进行高速度重排
        """
        # ⚡ 性能优化点：如果当前时间段内没有 3 或 13 号优先单，直接原样返回，不浪费算力
        if not any(r.OrderType in (3, 13) for r in bucket):
            return bucket

        # 1. 寻找重排的锚定目标：第一笔出现的普通买单(1/2) 和 普通卖单(11/12)
        first_buy_row = None
        first_sell_row = None
        for r in bucket:
            if r.OrderType in (1, 2) and first_buy_row is None:
                first_buy_row = r
            if r.OrderType in (11, 12) and first_sell_row is None:
                first_sell_row = r

        # 只有当对应的锚定目标存在时，才需要搬移优先单
        move_3 = first_buy_row is not None
        move_13 = first_sell_row is not None

        if not move_3 and not move_13:
            return bucket

        # 2. 抽离需要提前的优先单，保留其他单据的相对顺序
        type3_moved = []
        type13_moved = []
        remaining_rows = []

        for r in bucket:
            if move_3 and r.OrderType == 3:
                type3_moved.append(r)
            elif move_13 and r.OrderType == 13:
                type13_moved.append(r)
            else:
                remaining_rows.append(r)

        # 3. 重新拼装：在普通首单出现的前一刻，精准把优先单队列整体插入
        final_bucket = []
        for r in remaining_rows:
            if move_3 and r is first_buy_row:
                final_bucket.extend(type3_moved)
                move_3 = False  # 确保只插入一次
            if move_13 and r is first_sell_row:
                final_bucket.extend(type13_moved)
                move_13 = False  # 确保只插入一次
            final_bucket.append(r)

        return final_bucket

    for batch in dataset.to_batches(columns=cols, filter=filters, batch_size=batch_size):
        if active_schema is not None:
            batch = batch.cast(active_schema)
            
        # 转换当前分块为 DataFrame
        batch_df = batch.to_pandas()
        
         # 3. 逐行处理并利用状态机流式吐出
        for row in batch_df.itertuples(index=False):
            t_time = row.OrderTime
            
            if current_order_time is None:
                current_order_time = t_time
                order_buffer.append(row)
            elif t_time == current_order_time:
                # 同一微秒/毫秒内的单子，继续压入缓存
                order_buffer.append(row)
            else:
                # 时间戳切换了！触发上一个时间块的重排并吐出
                for sorted_row in flush_and_reorder_buffer(order_buffer):
                    yield sorted_row
                # 清空并初始化下一个时间块
                order_buffer = [row]
                current_order_time = t_time
    
    # 💡 4. 边界收尾：全天数据读完后，冲刷最后一包滞留在缓冲区的数据
    if order_buffer:
        for sorted_row in flush_and_reorder_buffer(order_buffer):
            yield sorted_row
        order_buffer.clear()


def stream_l2_timeline(order_file, deal_file, batch_size=50000, date=20260101):
    """
    流式双指针归并：同时拉取订单流与成交流，就地按照时间线对齐。
    """
    
    #df_order = pd.read_parquet(order_file, schema=order_schema, columns=order_columns, filters=[('TradingDay', '==', 20260529)])   
    #df_deal = pd.read_parquet(deal_file, schema=deal_schema, columns=deal_columns, filters=[('TradingDay', '==', 20260529)])
    #df_snapshot = pd.read_parquet(snapshot_file, columns=snapshot_columns, schema=snapshot_schema, filters=[('TradingDay', '==', 20260529)])  
    print(order_file)
    order_stream = stream_from_order(order_file, batch_size=batch_size, filters=(ds.field('TradingDay') == date))
    deal_stream = stream_from_deal(deal_file, batch_size=batch_size, filters=(ds.field('TradingDay') == date))
    
    curr_order = next(order_stream, None)
    curr_deal = next(deal_stream, None)
    
    while curr_order or curr_deal:
        # 💡 时间相同时，排序优先级：Order(0) -> Deal(1) -> Snapshot(2) 输出
        candidates = []
        if curr_order: candidates.append((curr_order.OrderTime, 0, 'Order', curr_order))
        if curr_deal:  candidates.append((curr_deal.DealTime, 1, 'Deal', curr_deal))

        # 按时间升序排序，时间相同时按优先级(0->1->2)升序排序
        candidates.sort(key=lambda x: (x[0], x[1]))
        best_type, best_row = candidates[0][2], candidates[0][3]

        yield best_type, best_row

        if best_type == 'Order':    
            curr_order = next(order_stream, None)
        elif best_type == 'Deal':   
            curr_deal = next(deal_stream, None)

def check_snapshot(ob=None, snapshot=None, decimals=0):
    total_dealnum, total_dealvolume, total_turnover = ob.get_deal_status()

    my_tot_bid_vol, my_tot_ask_vol, my_w_bid_p, my_w_ask_p = ob.get_orderbook_stats()
    my_w_bid_p = round(my_w_bid_p*10)
    my_w_ask_p = round(my_w_ask_p*10)

    # 对于整数价格：标准整数，实际为小数，最大相差0.5。放大10倍后应该用6做容差
    # 对于小数价格：一般相差0.1，原来用0.2做容差。放大10倍后应该用2做容差
    price_tolerance = 6 if decimals == 0 else 2
    
    if (snapshot.TickTime>=92500000 and snapshot.TickTime<145700000 or snapshot.TickTime>=150000000) and (getattr(snapshot, 'BidPrice5', 0)!=0 or getattr(snapshot, 'AskPrice5', 0)!=0):
        #is_opencallending = True
        match_all = my_tot_bid_vol == snapshot.TotalBidVolume and my_tot_ask_vol == snapshot.TotalAskVolume and \
                total_dealvolume == snapshot.TotalVolume and total_turnover == snapshot.TotalTurnover and \
                (abs(my_w_bid_p - snapshot.WeightBidPrice) < price_tolerance and abs(my_w_ask_p - snapshot.WeightAskPrice) < price_tolerance)
        if match_all:
            my_bids = ob.get_topN_snapshot('B', n_levels=10)
            my_asks = ob.get_topN_snapshot('S', n_levels=10)
            my_bid_dict = {p: (v, c) for p, v, c in my_bids}
            my_ask_dict = {p: (v, c) for p, v, c in my_asks}

            cached_snap_bids = [(getattr(snapshot, f'BidPrice{i}'), int(getattr(snapshot, f'BidVolume{i}')), int(getattr(snapshot, f'BidOrder{i}'))) for i in range(1, 11) if getattr(snapshot, f'BidPrice{i}') > 0]
            cached_snap_asks = [(getattr(snapshot, f'AskPrice{i}'), int(getattr(snapshot, f'AskVolume{i}')), int(getattr(snapshot, f'AskOrder{i}'))) for i in range(1, 11) if getattr(snapshot, f'AskPrice{i}') > 0]
            snap_bid = {p: (v, c) for p, v, c in cached_snap_bids}
            snap_ask = {p: (v, c) for p, v, c in cached_snap_asks}
        
            match_levels = (my_bid_dict == snap_bid) and (my_ask_dict == snap_ask)
            if match_levels:
                return True, 0


    else:
        #集合竞价阶段，无十档数据无交易
        #is_opencallending = False
        match_all = total_dealvolume == snapshot.TotalVolume and total_turnover == snapshot.TotalTurnover
        if match_all:
            return True, 0
    
    return False, -1 

def playback_and_rebuild(order_file, deal_file, snapshot_file, need_checksnapshot=True, batch_size=50000, date=20260101):
    """
    从磁盘流式读取并构建订单薄
    :param order_file: 逐笔委托 parquet 文件路径
    :param deal_file: 逐笔成交 parquet 文件路径
    :param market: 市场标识，'SH' 代表沪市，'SZ' 代表深市
    :param batch_size: 磁盘缓存块大小
    """
    # 初始化上一轮构建好的 Level2OrderBook 实例 (Method 3 架构)
    ob = OrderBook() 
    
    if need_checksnapshot:
        snapshot_stream = stream_from_snapshot(snapshot_file, batch_size=batch_size, filters=(ds.field('TradingDay') == date))
        curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
        while curr_snapshot.TotalBidVolume==0 and curr_snapshot.TotalAskVolume==0:
            curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
        local_total_cumvolume = 0           #累积交易量，用于快速比较是否需要进行snapshot检验，不符的肯定不用检验
        
    # 直接消费磁盘流，内存开销恒定
    for event_type, row in stream_l2_timeline(order_file, deal_file, batch_size, date=date):  
        # 提取当前事件的时间戳 (格式形如: 91501420)
        event_time = getattr(row, 'OrderTime', getattr(row, 'DealTime', 0))

        if event_type == 'Order':
            if row.OrderType == 0:    # 委买
                ob.insert_order(row.OrderID, row.Price, row.Volume, 'B', row.OrderTime)
            elif row.OrderType == 10: # 委卖
                ob.insert_order(row.OrderID, row.Price, row.Volume, 'S', row.OrderTime)
            elif row.OrderType in (-1, -11): # 撤买/撤卖 
                ob.cancel_order(row.OrderID, row.Volume)

            elif row.OrderType == 1: #市价委买
                ob.insert_order(row.OrderID, row.LastPrice, row.Volume, 'B', row.OrderTime)                
            elif row.OrderType == 2:    # 限价委买
                ob.insert_order(row.OrderID, row.Price, row.Volume, 'B', row.OrderTime)
            elif row.OrderType == 3: #本方最优委买
                ob.insert_order(row.OrderID, ob.get_bbo_bid(), row.Volume, 'B', row.OrderTime)
            elif row.OrderType == 11: # 市价委卖
                ob.insert_order(row.OrderID, row.LastPrice, row.Volume, 'S', row.OrderTime)       
            elif row.OrderType == 12: # 限价委卖
                ob.insert_order(row.OrderID, row.Price, row.Volume, 'S', row.OrderTime)
            elif row.OrderType == 13: # 本方最优委卖
                ob.insert_order(row.OrderID, ob.get_bbo_ask(), row.Volume, 'S', row.OrderTime)
        # ==========================================
        elif event_type == 'Deal':
            if row.Side in [0, 1]:
                # 交易所 L2 机制：一笔成交双边扣减                            
                ob.execute_trade(ref_id=row.BuyID, exec_qty=row.Volume, exec_price=row.Price)
                ob.execute_trade(ref_id=row.SellID, exec_qty=row.Volume, exec_price=row.Price)

                if need_checksnapshot:
                    local_total_cumvolume += row.Volume
                    if local_total_cumvolume == curr_snapshot.TotalDealNum:
                        curr_snapshot_timeout = row.DealTimeNext

            elif row.Side == -1:  # 买单撤单
                ob.cancel_order(ref_id=row.BuyID, cancel_qty=row.Volume)
            elif row.Side == -11: # df
                ob.cancel_order(ref_id=row.SellID, cancel_qty=row.Volume)

        if need_checksnapshot:
            # 1. 判断snapshot是否与当前ob是否相同。
            # 2. 不相同: 则读入下一个order/deal数据
            # 3. 相同: 读入下一个snapshot,判断与当前ob是否相同，相同继续读入下一个snapshot，直到不相同。
            if local_total_cumvolume == curr_snapshot.TotalVolume:
                if curr_snapshot.TickTime == 92504000:
                    pass
                verified, _ = check_snapshot(ob=ob, snapshot=curr_snapshot, decimals=target_decimals)
                while verified:
                    curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
                    if  curr_snapshot != None and local_total_cumvolume == curr_snapshot.TotalVolume:
                        verified, _ = check_snapshot(ob=ob, snapshot=curr_snapshot, decimals=target_decimals)
                    else:
                        verified = False    #此分支不能删除，在while循环多次后需要退出
            else:
                if event_time > curr_snapshot_timeout:
                    print(f"⚡ [{event_time}|{event_type}:{curr_snapshot.TickTime}] snapshot检验超时 ")
                    verified = True
                    while verified:
                        curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
                        if curr_snapshot != None and local_total_cumvolume == curr_snapshot.TotalVolume:
                            verified, _ = check_snapshot(ob=ob, snapshot=curr_snapshot, decimals=target_decimals)
                        else:   
                            verified = False    #此分支不能删除，在while循环多次后需要退出
         
    return ob

def process_single_stock_day(date, stock_code):
    daily_feature_dict={}

    #order_file = CONFIG.base_path['A_LEVEL2']/f"202605/order/{stock_code}.parquet"
    #deal_file = CONFIG.base_path['A_LEVEL2']/f"202605/deal/{stock_code}.parquet"
    #snapshot_file = CONFIG.base_path['A_LEVEL2']/f"202605/snapshot/{stock_code}.parquet"

    order_file = CONFIG.base_path['A_LEVEL2']/f"order/2026/202605/{stock_code}.parquet"
    deal_file = CONFIG.base_path['A_LEVEL2']/f"deal/2026/202605/{stock_code}.parquet"
    snapshot_file = CONFIG.base_path['A_LEVEL2']/f"snapshot/2026/202605/{stock_code}.parquet"
    
    assert order_file.exists(), f"Error: '{order_file}'"
    assert deal_file.exists(), f"Error: '{deal_file}'"
    assert snapshot_file.exists(), f"Error: '{snapshot_file}'"
        

    ob_all = playback_and_rebuild(order_file, deal_file, snapshot_file, date=date)

    
    return ob_all

if __name__ == '__main__':
    #stock_df = get_allstock()
    stocklist = [2631, 688017, 688693] #(20260529, "002631"), (20260529, "688017"), (20260529, "688693"), 
    tasks = [(20260529, "003031"), (20260529, "000531"), (20260529, "002008"), (20260529, "600641"), (20260529, "601101"), (20260529, "603032"), (20260529, "605186"), (20260529, "688545"), (20260529, "300776"), (20260529, "301458")]
    #tasks = [(20260529, "600641"), (20260529, "301458"), (20260529, "601101") ]

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
        task_result = process_single_stock_day(date=task[0], stock_code=task[1])
        if task_result is not None:
            results.append(task_result)