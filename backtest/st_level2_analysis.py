import logging
import datetime
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
from dataclasses import dataclass

from enum import IntEnum

# --- 配置区 ---
LOG_FILE = CONFIG.base_path['LEVEL2_TEMP']/'logs'/f"level2_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.log"

# 配置日志
# 1. 定义自定义 Formatter
class BracketAlignedFormatter(logging.Formatter):
    def format(self, record):
        bracket_level = f"[{record.levelname}]"
        record.aligned_levelname = f"{bracket_level:<11}" 
        return super().format(record)
# 2. 实例化 Handlers
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
stream_handler = logging.StreamHandler()

# 3. 🎯 修正这里：参数名改为 fmt
formatter = BracketAlignedFormatter(
    fmt='%(asctime)s %(aligned_levelname)s %(message)s',
    datefmt='%H:%M:%S'
)

# 4. 绑定格式化器
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# 5. 初始化配置
logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, stream_handler]
)

class TimeFrame(IntEnum):
    Ticks = 0
    MicroSeconds = 1
    Seconds = 2
    Minutes = 3
    Days = 4
    Weeks = 5
    Months = 6
    Years = 7
    NoTimeFrame = 8

@dataclass
class Resample:
   timeframe: TimeFrame = 0
   compression: int = 1

   def __init__(self, timeframe=TimeFrame.Seconds, compression=1):
       self.timeframe = timeframe
       self.compression = compression

FREQ_MAP = {
    TimeFrame.Seconds: "s",
    TimeFrame.Minutes: "min", 
    TimeFrame.Days: "D",
    TimeFrame.Weeks: "W",
    TimeFrame.Months: "ME",
    TimeFrame.Years: "YE"
}

LABEL_MAP = {
    TimeFrame.Seconds: "s",
    TimeFrame.Minutes: "m",
    TimeFrame.Days: "day",
    TimeFrame.Weeks: "w",
    TimeFrame.Months: "mon",
    TimeFrame.Years: "y"
}

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


def parse_deal_time(date_str, time_series):
    """
    高效解析整型时间戳 (如 93000100) 为标准的 Datetime
    """
    time_str = time_series.astype(str).str.zfill(9)
    hh = time_str.str[0:2]
    mm = time_str.str[2:4]
    ss = time_str.str[4:6]
    ms = time_str.str[6:9]
    dt_str = str(date_str) + " " + hh + ":" + mm + ":" + ss + "." + ms
    return pd.to_datetime(dt_str, format="%Y%m%d %H:%M:%S.%f")

def generate_and_save_bars(srcfile, stock_code, date_str, resamples=[]):
    """
    功能 1：从逐笔成交 Tick 合成多周期 Bar 并保存为 Parquet
    :param deal_df: 包含 Price, Volume, Turnover, DealTime(或ActionTime) 的 DataFrame
    :param stock_code: 股票代码, 如 '300128'
    :param date_str: 日期字符串, 如 '20260529'
    """
    deal_columns = ['DealTime', 'Price', 'Side', 'Volume']
    df = pd.read_parquet(srcfile, schema=deal_schema, columns=deal_columns, filters=[('TradingDay', '==', date_str)]) 

    # 1. 确保时间戳建立索引
    df = df[~df['Side'].isin([-1, -11])]
    df.index = pd.DatetimeIndex(parse_deal_time(date_str, df['DealTime']), name='date')
    df['Turnover'] = df['Price'] * df['Volume']
    
    generated_files = {}
    
    # 3. 循环 Resample 合成各周期 K 线
    for r in resamples:
        # 组装 Pandas 频度 (例如: "1T", "5T", "1D")
        freq = f"{r.compression}{FREQ_MAP[r.timeframe]}"
        # 组装文件命名标签 (例如: "1m", "5m", "1day")
        tf_label = f"{r.compression}{LABEL_MAP[r.timeframe]}"

        output_dir = CONFIG.inferred_path['MINUTES_DATA_PATH']/f"{tf_label}"/'CN_STOCK'/'202605'/f"{stock_code}"
        output_dir.mkdir(parents=True, exist_ok=True)

        # A股标准：右闭包，右标签（1分钟棒标记为当前分钟的结束）
        # 对于 1D 数据，通常 label='left' 或默认即可，这里统一用符合分钟线直觉的配置或根据后续对比调整
        if r.timeframe == TimeFrame.Days:
            # 天级 K 线通常采用默认的左闭包，使得时间戳保持为当天 00:00:00
            resampler = df.resample(freq)
            datefmt = CONFIG.date_fmt[DATAFRAME['DAY']]
        else:
            # 分钟/秒级高频 K 线严格遵循右闭包、右标签规则
            resampler = df.resample(freq, closed='right', label='right')
            datefmt  = CONFIG.date_fmt[DATAFRAME['MINUTE1']]
        
        bar_df = resampler.agg({
            'Price': ['first', 'max', 'min', 'last'],
            'Volume': 'sum',
            'Turnover': 'sum'
        })
        
        # 扁平化多级表头
        bar_df.columns = ['open', 'high', 'low', 'close', 'volume', 'turnover']
        bar_df = bar_df.reset_index()
        
        # 剔除非交易时段生成的无成交量的死棒 (比如中午 11:30-13:00)
        bar_df = bar_df[bar_df['volume'] > 0].reset_index()
        
        bar_df = bar_df.astype({
            'open': 'int32',
            'high': 'int32',
            'low': 'int32',
            'close': 'int32',
            'volume': 'int64',
            'turnover': 'int64',
            'date': 'datetime64[s]'  # 🚀 固化为秒级精度，去除不必要的毫秒/纳秒冗余
        })
        
        # 4. 固化存储为 Parquet 文件
        file_name = f"{stock_code}_{date_str}.csv"
        file_path = output_dir / file_name
        
        #bar_df.to_parquet(file_path, index=False)
        bar_df.to_csv(file_path, sep=',', encoding='utf-8-sig', index=False, date_format=datefmt, float_format='%.2f')
        
        generated_files[tf_label] = bar_df
        print(f"💾 成功生成并保存: {file_path} | 共 {len(bar_df)} 行")
        
    return generated_files

def cal_itemcounts(pfile, collist=None, plot=False):
    array_arrow = pq.read_table(pfile, columns=collist).column(0)
    select_items = array_arrow.to_numpy()

    unique_items, counts = np.unique(select_items, return_counts=True)
    if plot:
        col_str = ", ".join(collist)
        # 3. 优雅地打印结果
        print(f"📊 每日 Level 2 逐笔数据量统计：")
        print("-" * 35)
        print(f"{col_str:<15} | {'频次 (Counts)' :<15}")
        print("-" * 35)

        for item, count in zip(unique_items, counts):
            # 如果读取出来的日期是 bytes 类型（Parquet 中常见的 string 表现），需要 decode 一下
            if isinstance(item, bytes):
                item = item.decode('utf-8')
            print(f"{item:<15} | {count:<15,}") # :, 可以在数字中自动加上千分位逗号，方便肉眼阅读

        print("-" * 35)
        print(f"总计数据量: {len(select_items):,} 行")
    return unique_items.tolist()

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



def stream_from_deal(file_path, filters=None):
    """
    利用 Dataset API 实现真正的【磁盘分块流式读取 + 过滤】，并动态计算跨批次 DealTimeNext
    """
    # 1. 将文件映射为 Dataset 对象（仅读取元数据，O(1) 速度极快）
    # 精准提取所需字段
    deal_columns = ['BuyID', 'DealID', 'DealTime', 'Price', 'SellID', 'Side', 'Volume']
    df_deal = pd.read_parquet(file_path, schema=deal_schema, columns=deal_columns, filters=filters)
    df_deal = df_deal.sort_values(by=['DealTime', 'DealID'], ascending=True, kind='stable').reset_index(drop=True)


    CLOSE_TIME = 153000000  
    valid_times = df_deal['DealTime'].where(~df_deal['Side'].isin([-1, -11]))

    df_deal['DealTimeNext'] = valid_times.shift(-1).bfill()
    df_deal.loc[df_deal.index[-1], 'DealTimeNext'] = CLOSE_TIME
    df_deal['DealTimeNext'] = df_deal['DealTimeNext'].fillna(CLOSE_TIME).astype(int)
    for row in df_deal.itertuples(index=False):
        yield row

def stream_from_order(file_path, filters=None):
    """
    利用 Dataset API 实现真正的【磁盘分块流式读取 + 过滤】，并动态计算跨批次 DealTimeNext
    """
    # 1. 将文件映射为 Dataset 对象（仅读取元数据，O(1) 速度极快）
    order_columns = ['LastPrice', 'OrderID', 'OrderTime', 'OrderType', 'Price', 'Volume']
    df_order = pd.read_parquet(file_path, schema=order_schema, columns=order_columns, filters=filters)
    df_order = df_order.sort_values(by=['OrderTime', 'OrderID'], ascending=True, kind='stable').reset_index(drop=True)

    for row in df_order.itertuples(index=False):
        yield row
    


def stream_l2_timeline(order_file, deal_file, batch_size=50000, date=20260101):
    """
    流式双指针归并：同时拉取订单流与成交流，就地按照时间线对齐。
    """
    
    #df_order = pd.read_parquet(order_file, schema=order_schema, columns=order_columns, filters=[('TradingDay', '==', 20260529)])   
    #df_deal = pd.read_parquet(deal_file, schema=deal_schema, columns=deal_columns, filters=[('TradingDay', '==', 20260529)])
    #df_snapshot = pd.read_parquet(snapshot_file, columns=snapshot_columns, schema=snapshot_schema, filters=[('TradingDay', '==', 20260529)])  
    order_stream = stream_from_order(order_file, filters=(ds.field('TradingDay') == date))
    deal_stream = stream_from_deal(deal_file, filters=(ds.field('TradingDay') == date))
    
    curr_order = next(order_stream, None)
    curr_deal = next(deal_stream, None)
    
    while curr_order or curr_deal:
        # 💡 时间相同时，排序优先级：Order(0) -> Deal(1) -> Snapshot(2) 输出
        candidates = []
        if curr_order: candidates.append((curr_order.OrderTime, curr_order.OrderID, 'Order', curr_order))
        if curr_deal:  candidates.append((curr_deal.DealTime, curr_deal.DealID, 'Deal', curr_deal))

        # 按时间升序排序
        # 相同时间下按订单号升序排序
        # 不能只按订单号，有的撤单订单号很小。要结合时间一起双排序
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
            elif row.OrderType in (-1, -11): # 撤买/撤卖,竞价阶段的撤销无deal记录，不能删除
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
                    if local_total_cumvolume == curr_snapshot.TotalVolume:
                        curr_snapshot_timeout = row.DealTimeNext

            elif row.Side == -1:  # 买单撤单
                ob.cancel_order(ref_id=row.BuyID, cancel_qty=row.Volume)
            elif row.Side == -11: # df
                ob.cancel_order(ref_id=row.SellID, cancel_qty=row.Volume)

        if need_checksnapshot and curr_snapshot is not None:
            # 1. 判断snapshot是否与当前ob是否相同。
            # 2. 不相同: 则读入下一个order/deal数据
            # 3. 相同: 读入下一个snapshot,判断与当前ob是否相同，相同继续读入下一个snapshot，直到不相同。
            if event_time >= 145655820:
                pass
            if local_total_cumvolume == curr_snapshot.TotalVolume:
                verified, _ = check_snapshot(ob=ob, snapshot=curr_snapshot, decimals=target_decimals)
                while verified:
                    curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
                    if  curr_snapshot != None and local_total_cumvolume == curr_snapshot.TotalVolume:
                        verified, _ = check_snapshot(ob=ob, snapshot=curr_snapshot, decimals=target_decimals)
                    else:
                        verified = False    #此分支不能删除，在while循环多次后需要退出
            else:
                if event_time >= curr_snapshot_timeout:
                    logging.error(f"⚡ [snapshot:{curr_snapshot.TickTime}|{event_type}:{event_time}] snapshot检验超时 ")
                    verified = True
                    while verified:
                        curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
                        if curr_snapshot != None and local_total_cumvolume == curr_snapshot.TotalVolume:
                            verified, _ = check_snapshot(ob=ob, snapshot=curr_snapshot, decimals=target_decimals)
                        else:   
                            verified = False    #此分支不能删除，在while循环多次后需要退出
         
    return ob

if __name__ == '__main__':

    #stocklist = [2631, 688017, 688693] #(20260529, "002631"), (20260529, "688017"), (20260529, "688693"), 

    min1 = Resample(timeframe=TimeFrame.Minutes, compression=1)
    min5 = Resample(timeframe=TimeFrame.Minutes, compression=5)
    day1 = Resample(timeframe=TimeFrame.Days, compression=1)
    base_dir = CONFIG.base_path['LEVEL2_TEMP']

    checkyear = '2026'
    checkmonth = '202605'
    pb_data = []

    for month in ['05']:
        directory = CONFIG.base_path['A_LEVEL2']/'order'/checkyear/checkmonth
        # srcfile_list = [p.stem for p in directory.glob('*.parquet')]
        srcfile_list = ['000002','000668', '002631','300776',  '688017', '688693']

        for stock_code in srcfile_list:
            order_file = CONFIG.base_path['A_LEVEL2']/f"order/{checkyear}/{checkyear}{month}/{stock_code}.parquet"
            deal_file = CONFIG.base_path['A_LEVEL2']/f"deal/{checkyear}/{checkyear}{month}/{stock_code}.parquet"
            snapshot_file = CONFIG.base_path['A_LEVEL2']/f"snapshot/{checkyear}/{checkyear}{month}/{stock_code}.parquet"

            assert order_file.exists(), f"Error: '{order_file}'"
            assert deal_file.exists(), f"Error: '{deal_file}'"
            assert snapshot_file.exists(), f"Error: '{snapshot_file}'"

            #check_deal(deal_file, stock_code=stock_code)
        
            tradingdays = cal_itemcounts(deal_file, collist=['TradingDay'])

            for date in tradingdays:
                date = 20260529
                logging.info(f"{date}:{order_file}")
                # result_df = check_data_integrity(order_file, deal_file, snapshot_file, date=date, stock_code=stock_code)
                # if not result_df.empty:
                #     pb_data.append(result_df)
                ob_all = playback_and_rebuild(order_file, deal_file, snapshot_file, date=date)

                
                # my_bars = generate_and_save_bars(srcfile=deal_file, stock_code=stock_code, date_str=date, resamples=[min1, min5, day1])
                # verify_bars_against_csv(my_bars, stock_code=task[1], date_str=task[0])

        # if pb_data:
        #     final_df = pd.concat(pb_data, ignore_index=True) 
        #     pb_path = CONFIG.base_path['LEVEL2_TEMP']/f'{checkmonth}.csv'
        #     final_df.to_csv(pb_path, sep=',', encoding='utf-8-sig', index=False, float_format='%.2f')

    # for exchange, dateframe in itertools.product(CONFIG.EXCHANGE, DATAFRAME):
    #     directory = CONFIG.inferred_path['TDX_INSTALL_DATADIR']/exchange/CONFIG.src_dir[dateframe]  
    #     new_filelist = [(p, dateframe) for p in directory.glob(CONFIG.file_extension[dateframe])]
    #     task_filelist.extend(new_filelist)
