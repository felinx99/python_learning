import logging
import datetime
import psutil
import traceback
import gc
import pyarrow as pa
import pandas as pd
import numpy as np
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from backtrader import order
from collections import namedtuple
from concurrent.futures import ProcessPoolExecutor
from common import CONFIG, DATAFRAME
from dataclasses import dataclass
from enum import IntEnum
from multiprocessing import Pool
from api.timeprofile import TimeProfile
from .util.order import OrderNode, OrderBook


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

schema_dict = {
        'order_schema' : order_schema,
        'order_raw_schema' : orderraw_schema,
        'deal_schema' : deal_schema,
        'snapshot_schema' : snapshot_schema,
    }

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

def stream_from_snapshot(file_path, filters=None):
    snapshot_columns = ['DealNum', 'Price', 'TickTime', 'TickTimeDiff', 'TotalAskVolume', 'TotalBidVolume', 'TotalDealNum', 'TotalTurnover', 'TotalVolume', 'Turnover', 'Volume', 'WeightAskPrice', 'WeightBidPrice']
    for i in range(1,11):
        snapshot_columns.extend([f'BidPrice{i}', f'BidVolume{i}', f'BidOrder{i}'])
        snapshot_columns.extend([f'AskPrice{i}', f'AskVolume{i}', f'AskOrder{i}'])
   
    df_snapshot = pd.read_parquet(file_path, schema=snapshot_schema, columns=snapshot_columns, filters=filters)
    df_snapshot = df_snapshot.sort_values(by=['TickTime'], ascending=True, kind='stable').reset_index(drop=True)
    # 核心清洗：金额转分逻辑（保持你原有的逻辑不变）

    df_snapshot['Turnover'] = (df_snapshot['Turnover'] * 100).round().astype('int64')
    df_snapshot['TotalTurnover'] = (df_snapshot['TotalTurnover'] * 100).round().astype('int64')
    df_snapshot['WeightBidPrice'] = (df_snapshot['WeightBidPrice'] * 10).round().astype('int32')
    df_snapshot['WeightAskPrice'] = (df_snapshot['WeightAskPrice'] * 10).round().astype('int32')

    for row in df_snapshot.itertuples(index=False):
        decimals = 0 if row.WeightBidPrice % 10 == 0 else 1
        yield row, decimals, row.TickTime+2000



def stream_from_deal(file_path, filters=None):
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
    order_columns = ['LastPrice', 'OrderID', 'OrderTime', 'OrderType', 'Price', 'Volume']
    df_order = pd.read_parquet(file_path, schema=order_schema, columns=order_columns, filters=filters)
    df_order = df_order.sort_values(by=['OrderTime', 'OrderID'], ascending=True, kind='stable').reset_index(drop=True)

    for row in df_order.itertuples(index=False):
        yield row
    


def stream_l2_timeline(order_file, deal_file, filters=None):
    """
    流式双指针归并：同时拉取订单流与成交流，就地按照时间线对齐。
    """
    
    #df_order = pd.read_parquet(order_file, schema=order_schema, columns=order_columns, filters=[('TradingDay', '==', 20260529)])   
    #df_deal = pd.read_parquet(deal_file, schema=deal_schema, columns=deal_columns, filters=[('TradingDay', '==', 20260529)])
    #df_snapshot = pd.read_parquet(snapshot_file, columns=snapshot_columns, schema=snapshot_schema, filters=[('TradingDay', '==', 20260529)])  
    order_stream = stream_from_order(order_file, filters=filters)
    deal_stream = stream_from_deal(deal_file, filters=filters)
    
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

def check_10_levels(ob=None, snapshot=None, decimals=0):
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
            # --- 门限值配置 ---
            VOL_THRESHOLD = 15  # 允许第一档量的绝对值偏差

            # 1. 提取本地数据与快照数据
            my_bids = ob.get_topN_snapshot('B', n_levels=10)
            my_asks = ob.get_topN_snapshot('S', n_levels=10)
            my_bid_dict = {p: (v, c) for p, v, c in my_bids}
            my_ask_dict = {p: (v, c) for p, v, c in my_asks}

            cached_snap_bids = [(getattr(snapshot, f'BidPrice{i}'), int(getattr(snapshot, f'BidVolume{i}')), int(getattr(snapshot, f'BidOrder{i}'))) for i in range(1, 11) if getattr(snapshot, f'BidPrice{i}') > 0]
            cached_snap_asks = [(getattr(snapshot, f'AskPrice{i}'), int(getattr(snapshot, f'AskVolume{i}')), int(getattr(snapshot, f'AskOrder{i}'))) for i in range(1, 11) if getattr(snapshot, f'AskPrice{i}') > 0]

            # 2. 进入状态机分支判断
            if not cached_snap_asks: # 🔴 涨停状态：快照卖盘为空，只比较买盘 (Bids)
                
                snap_bid = {p: (v, c) for p, v, c in cached_snap_bids}
                
                # 容错：若本地也没买盘或长度对不上，直接判定不匹配
                if len(my_asks):
                    match_levels = False
                else:
                    # 剥离买一档（列表第一项）进行带门限的模糊比较
                    p1_my, v1_my, c1_my = my_bids[0]
                    p1_snap, v1_snap, c1_snap = cached_snap_bids[0]
                    
                    # 剔除买一档后，其余 9 档严格对比
                    my_bid_rest = {p: vc for p, vc in my_bid_dict.items() if p != p1_my}
                    snap_bid_rest = {p: vc for p, vc in snap_bid.items() if p != p1_snap}
                    
                    diff = abs(v1_my - v1_snap) 
                    match_rest = (my_bid_rest == snap_bid_rest)
                    match_p1 = (p1_my == p1_snap) and (c1_my == c1_snap) and (diff<=VOL_THRESHOLD)
                    match_levels = match_rest and match_p1
         
            elif not cached_snap_bids: # 🟢 跌停状态：快照买盘为空，只比较卖盘 (Asks)
                
                snap_ask = {p: (v, c) for p, v, c in cached_snap_asks}
                
                # 容错
                if len(my_bids):
                    match_levels = False
                else:
                    # 剥离卖一档进行带门限的模糊比较
                    p1_my, v1_my, c1_my = my_asks[0]
                    p1_snap, v1_snap, c1_snap = cached_snap_asks[0]
                    
                    # 剔除卖一档后，其余 9 档严格对比
                    my_ask_rest = {p: vc for p, vc in my_ask_dict.items() if p != p1_my}
                    snap_ask_rest = {p: vc for p, vc in snap_ask.items() if p != p1_snap}
                    
                    diff = abs(v1_my - v1_snap) 
                    match_rest = (my_ask_rest == snap_ask_rest)
                    match_p1 = (p1_my == p1_snap) and (c1_my == c1_snap) and (diff<=VOL_THRESHOLD)
                    match_levels = match_rest and match_p1
                    
            else:
                # ⚪ 正常状态：两边都有盘口，全量 10 档执行最严格、最高效的直接比对
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
            return True, -2
    
    return False, -1

def playback_and_rebuild(args):
    """
    从磁盘流式读取并构建订单薄
    :param order_file: 逐笔委托 parquet 文件路径
    :param deal_file: 逐笔成交 parquet 文件路径
    :param market: 市场标识，'SH' 代表沪市，'SZ' 代表深市
    :param batch_size: 磁盘缓存块大小
    """
    isSucess = True

    rday, stock_code=args
    stock_str = str(stock_code).zfill(6)
    checkyear = '2026'
    checkmonth = '06'
    msg = ''

    order_file = CONFIG.inferred_path['LEVEL2_BUFFER_PATH']/f"monthlystaging/order/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday}.parquet"
    deal_file = CONFIG.inferred_path['LEVEL2_BUFFER_PATH']/f"monthlystaging/deal/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday}.parquet"
    snapshot_file = CONFIG.inferred_path['LEVEL2_BUFFER_PATH']/f"monthlystaging/snapshot/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday}.parquet"
    
    assert order_file.exists(), f"Error: '{order_file}'"
    assert deal_file.exists(), f"Error: '{deal_file}'"
    assert snapshot_file.exists(), f"Error: '{snapshot_file}'"
    # 初始化上一轮构建好的 Level2OrderBook 实例 (Method 3 架构)
    need_checksnapshot = True

    ob = OrderBook() 


    if need_checksnapshot:
        snapshot_stream = stream_from_snapshot(snapshot_file)
        curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
        while curr_snapshot.TotalBidVolume==0 and curr_snapshot.TotalAskVolume==0:
            curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
        local_total_cumvolume = 0           #累积交易量，用于快速比较是否需要进行snapshot检验，不符的肯定不用检验
        
    # 直接消费磁盘流，内存开销恒定
    for event_type, row in stream_l2_timeline(order_file, deal_file):  
        # 提取当前事件的时间戳 (格式形如: 91501420)
        event_time = getattr(row, 'OrderTime', getattr(row, 'DealTime', 0))

        if event_type == 'Order':
            if row.OrderType == 0:    # 委买
                ob.insert_order(row.OrderID, row.Price, row.Volume, 'B', row.OrderTime)
            elif row.OrderType == 10: # 委卖
                ob.insert_order(row.OrderID, row.Price, row.Volume, 'S', row.OrderTime)
            # elif row.OrderType in (-1, -11): # 撤买/撤卖,竞价阶段的撤销无deal记录，不能删除
            #     ob.cancel_order(row.OrderID, row.Volume)

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
            if event_time >= 92500000:
                pass
            if local_total_cumvolume == curr_snapshot.TotalVolume:
                verified, _ = check_10_levels(ob=ob, snapshot=curr_snapshot, decimals=target_decimals)
                while verified:
                    curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
                    if  curr_snapshot != None and local_total_cumvolume == curr_snapshot.TotalVolume:
                        verified, _ = check_10_levels(ob=ob, snapshot=curr_snapshot, decimals=target_decimals)
                    else:
                        verified = False    #此分支不能删除，在while循环多次后需要退出
            else:
                if event_time >= curr_snapshot_timeout:
                    if isSucess:
                        msg = f"⚡ [snapshot:{curr_snapshot.TickTime}|{event_type}:{event_time}] snapshot检验超时 \n"
                    isSucess = False
                    verified = True
                    while verified:
                        curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
                        if curr_snapshot != None and local_total_cumvolume == curr_snapshot.TotalVolume:
                            verified, _ = check_10_levels(ob=ob, snapshot=curr_snapshot, decimals=target_decimals)
                        else:   
                            verified = False    #此分支不能删除，在while循环多次后需要退出

    # msg = "".join(msg_chunks)     
    return rday, stock_code, isSucess, msg

def check_snapshot():
    #stocklist = [2631, 688017, 688693] #(20260529, "002631"), (20260529, "688017"), (20260529, "688693"), 
    checkyear = '2026'
    pb_data = []

    for checkmonth in ['05']:
        directory = CONFIG.base_path['A_LEVEL2']/'order'/checkyear/f"{checkyear}{checkmonth}"
        # srcfile_list = [p.stem for p in directory.glob('*.parquet')]
        srcfile_list = ['000010', '000668', '002631', '000002','300776',  '688017', '688693']

        for stock_code in srcfile_list:
            order_file = CONFIG.base_path['A_LEVEL2']/f"order/{checkyear}/{checkyear}{checkmonth}/{stock_code}.parquet"
            deal_file = CONFIG.base_path['A_LEVEL2']/f"deal/{checkyear}/{checkyear}{checkmonth}/{stock_code}.parquet"
            snapshot_file = CONFIG.base_path['A_LEVEL2']/f"snapshot/{checkyear}/{checkyear}{checkmonth}/{stock_code}.parquet"

            assert order_file.exists(), f"Error: '{order_file}'"
            assert deal_file.exists(), f"Error: '{deal_file}'"
            assert snapshot_file.exists(), f"Error: '{snapshot_file}'"

            #check_deal(deal_file, stock_code=stock_code)
        
            tradingdays = cal_itemcounts(deal_file, collist=['TradingDay'])

            for date in tradingdays:
                # logging.info(f"{date}:{order_file}")
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


def Verify_level2(date_list=[], checkyear='', checkmonth=''):
    tasks = []
    srcfile_list = {}

    for rday in date_list:
        src_snapshot_file = CONFIG.inferred_path['LEVEL2_BUFFER_PATH']/f"snapshot/{checkyear}/{checkyear}{checkmonth}/snapshot_{rday}.parquet"
        assert src_snapshot_file.exists(), f"Error: '{src_snapshot_file}'"
        
        srcfile_list[rday] = cal_itemcounts(src_snapshot_file, collist=['SecuCode'])
        for stock_code in srcfile_list[rday]:
            tasks.append((rday, stock_code))
    
    physical_cores = psutil.cpu_count(logical=False)
    with Pool(physical_cores) as p:
        #starmap会自动将task_filelist解包传给函数
        
        results = p.imap_unordered(playback_and_rebuild, tasks, chunksize=100)
        for rday, stock_code, success, msg in results:
            if not success:
                logging.error(f"[X] 失败 | {rday} : {stock_code} | \n 错误信息: {msg}")

# =================================================================
# 1. Order & Deal 增量分流与对齐函数（单线程）
# =================================================================
def check_deal(order_table, deal_table):
    df_order = order_table.to_pandas()
    df_deal = deal_table.to_pandas()

    ismissing = False
    # --- 撤单比对逻辑 ---
    cancel_rules = [(-1, 'BuyID'), (-11, 'SellID')]
    for cancel_type, match_id_col in cancel_rules:
        ord_cancel = df_order[df_order['OrderType'] == cancel_type]
        deal_cancel = df_deal[df_deal['Side'] == cancel_type]
        
        if len(ord_cancel) > len(deal_cancel):
            missing_orders = ord_cancel[~ord_cancel['BizIndex'].isin(deal_cancel['DealID'])]
            
            if not missing_orders.empty:
                buy_ids = missing_orders['OrderID'] if cancel_type == -1 else 0
                sell_ids = missing_orders['OrderID'] if cancel_type == -11 else 0
                
                missing_deal_rows = pd.DataFrame({
                    'SecuCode': missing_orders['SecuCode'],
                    'TradingDay': missing_orders['TradingDay'],
                    'DealTime': missing_orders['OrderTime'],
                    'DealID': missing_orders['BizIndex'],
                    'BuyID': buy_ids,
                    'SellID': sell_ids,
                    'Price': missing_orders['Price'],
                    'Volume': missing_orders['Volume'],
                    'Side': missing_orders['OrderType'],
                    'Channel': missing_orders['Channel'],
                    'BizIndex': missing_orders['BizIndex']
                })
                ismissing = True
                df_deal = pd.concat([df_deal, missing_deal_rows], ignore_index=True)
    
    if ismissing:
        df_deal = df_deal.sort_values(by='BizIndex', ascending=True, ignore_index=True)
        deal_table = pa.Table.from_pandas(df_deal, schema=deal_schema)

    return deal_table

def generate_staging_order_and_deal_dateset(date_list=[], checkmonth='', checkyear='2026'):
    """
    每日或隔2~3日运行（单线程）：
    1. 读取 order, deal 每日全市场大文件并按股票分流。
    2. 对比补齐 deal 中缺失的 [-1, -11] 撤单数据。
    3. 增量追加进 Staging 暂存区对应的股票文件夹中。
    """
    print(f"🎬 开始执行 {checkyear}-{checkmonth} 批次 {date_list} 的 Order & Deal 增量分流与对齐...")
    
    base_buffer = CONFIG.inferred_path['LEVEL2_BUFFER_PATH']
    order_schema = schema_dict['order_schema']
    deal_schema = schema_dict['deal_schema']
    
    # 约定暂存区根目录
    dst_dir_order = base_buffer / f"monthlystaging/order/{checkyear}/{checkyear}{checkmonth}"
    dst_dir_deal = base_buffer / f"monthlystaging/deal/{checkyear}/{checkyear}{checkmonth}"
    dst_dir_order.mkdir(parents=True, exist_ok=True)
    dst_dir_deal.mkdir(parents=True, exist_ok=True)

    for rday in date_list:
        print(f"⏱️ 正在处理交易日(Order/Deal): {rday} ...")
       
        src_order = base_buffer / f"order/{checkyear}/{checkyear}{checkmonth}/order_{rday}.parquet"
        src_deal = base_buffer / f"deal/{checkyear}/{checkyear}{checkmonth}/deal_{rday}.parquet"
        
        if not src_order.exists() or not src_deal.exists():
            print(f"⚠️ 找不到 {rday} 的 order 或 deal 原始文件，自动跳过。")
            continue
            
        try:
            # 3.1 读取每日全市场大文件（全天只读一次）
            order_table = pq.read_table(src_order, schema=order_schema, memory_map=True)
            deal_table = pq.read_table(src_deal, schema=deal_schema, memory_map=True)

            stockcode_list = order_table['SecuCode'].unique().to_pylist()
            total_stock = len(stockcode_list)
            success_count = 0

            for stock_code in stockcode_list:
                stock_str = str(stock_code).zfill(6)

                order_stock_table = order_table.filter(pc.field('SecuCode') == stock_code)
                deal_stock_table = deal_table.filter(pc.field('SecuCode') == stock_code)
                        
                # 3.2 对比 deal, order 中 [-1, -11] 的撤单数据
                deal_stock_table = check_deal(order_stock_table, deal_stock_table)

                stock_order_dir = dst_dir_order / stock_str
                stock_deal_dir = dst_dir_deal / stock_str
                stock_order_dir.mkdir(parents=True, exist_ok=True)
                stock_deal_dir.mkdir(parents=True, exist_ok=True)

                # 5. 精准定制文件名 (例如: 000020_20260526.parquet)
                out_order_file = stock_order_dir / f"{stock_str}_{rday}.parquet"
                out_deal_file = stock_deal_dir / f"{stock_str}_{rday}.parquet"

                # 6. 使用 write_table 直接物理落盘（绕过 write_dataset 的强制命名限制）
                with pq.ParquetWriter(out_order_file, order_schema, compression='snappy') as writer:
                    writer.write_table(order_stock_table)
                with pq.ParquetWriter(out_deal_file, deal_schema, compression='snappy') as writer:
                    writer.write_table(deal_stock_table)

                del order_stock_table, deal_stock_table, out_order_file, out_deal_file
                success_count +=1
                print(f"已成功合并 {success_count}/{total_stock} 只股票 [{stock_str}]", end="\r")
            # 严格释放内存
            del order_table, deal_table
            gc.collect()
            
        except Exception as e:
            print(f"❌ 处理交易日 {rday} (Order/Deal) 失败: {e}\n{traceback.format_exc()}")


# =================================================================
# 2. Snapshot 增量分流函数（单线程）
# =================================================================
def generate_staging_snapshot_dateset(date_list=[], checkyear='2026', checkmonth=''):
    """
    每日或隔2~3日运行（单线程）：
    1. 读取 snapshot 每日全市场大文件。
    2. 极速拆分并分流存储进暂存区股票文件夹中。
    """
    print(f"🎬 开始执行 {checkyear}-{checkmonth} 批次 {date_list} 的 Snapshot 增量分流...")
    
    base_buffer = CONFIG.inferred_path['LEVEL2_BUFFER_PATH']
    snapshot_schema = schema_dict['snapshot_schema']
    order_raw_schema = schema_dict['order_raw_schema']
    
    # 约定暂存区根目录
    dst_dir_snapshot = base_buffer / f"monthlystaging/snapshot/{checkyear}/{checkyear}{checkmonth}"
    dst_dir_order_raw = base_buffer / f"monthlystaging/order_raw/{checkyear}/{checkyear}{checkmonth}"
    dst_dir_snapshot.mkdir(parents=True, exist_ok=True)
    dst_dir_order_raw.mkdir(parents=True, exist_ok=True)

    for rday in date_list:
        print(f"⏱️ 正在处理交易日(Snapshot): {rday} ...")
       
        src_snapshot = base_buffer / f"snapshot/{checkyear}/{checkyear}{checkmonth}/snapshot_{rday}.parquet"
        src_order_raw = base_buffer / f"order_raw/{checkyear}/{checkyear}{checkmonth}/order_raw_{rday}.parquet"
        
        if not src_snapshot.exists() or not src_order_raw.exists():
            print(f"⚠️ 找不到 {rday} 的 snapshot/order_raw 原始文件，自动跳过。")
            continue
            
        try:
            # 3.1 读取全市场大文件（Arrow 驻留内存极其轻量）
            snapshot_table = pq.read_table(src_snapshot, schema=snapshot_schema, memory_map=True)
            order_raw_table = pq.read_table(src_order_raw, schema=order_raw_schema, memory_map=True)

            # 提取全市场去重股票代码列表 (int 类型)
            stockcode_list = snapshot_table['SecuCode'].unique().to_pylist()
            total_stock = len(stockcode_list)
            success_count = 0

            for stock_code in stockcode_list:
                stock_str = str(stock_code).zfill(6)

                # 3.2 利用 pyarrow.compute 提取当前股票的切片（底层指针引用，零内存拷贝）
                snapshot_stock_table = snapshot_table.filter(pc.field('SecuCode') == stock_code)
                order_raw_stock_table = order_raw_table.filter(pc.field('SecuCode') == stock_code)
                
                # 3.3 精准定制该股票的物理目录
                stock_snapshot_dir = dst_dir_snapshot / stock_str
                stock_order_raw_dir = dst_dir_order_raw / stock_str
                stock_snapshot_dir.mkdir(parents=True, exist_ok=True)
                stock_order_raw_dir.mkdir(parents=True, exist_ok=True)

                # 3.4 精准定制纯净文件名 (例如: 000020_20260526.parquet)
                out_snapshot_file = stock_snapshot_dir / f"{stock_str}_{rday}.parquet"
                out_order_raw_file = stock_order_raw_dir / f"{stock_str}_{rday}.parquet"

                # 3.5 直接物理落盘
                with pq.ParquetWriter(out_snapshot_file, snapshot_schema, compression='snappy') as writer:
                    writer.write_table(snapshot_stock_table)
                with pq.ParquetWriter(out_order_raw_file, order_raw_schema, compression='snappy') as writer:
                    writer.write_table(order_raw_stock_table)

                del snapshot_stock_table, order_raw_stock_table, out_snapshot_file, out_order_raw_file
                success_count +=1
                print(f"已成功合并 {success_count}/{total_stock} 只股票 [{stock_str}]", end="\r")    
            # 严格释放全市场大表内存
            del snapshot_table, order_raw_table
            gc.collect()
            print(f"✅ 快照交易日 {rday} 完美分流完毕。")
            
        except Exception as e:
            print(f"❌ 处理快照交易日 {rday} 失败: {e}\n{traceback.format_exc()}")


# =================================================================
# 3. 月度合并与打扫战场函数（三线程并行）
# =================================================================
def _merge_single_data_type_worker(dtype='', checkmonth='', checkyear='2026'):
    """
    此函数将在独立的进程中运行，拥有独立的内存空间，彻底避免 GIL 锁限制
    """
    print(f"🚀 [进程启动] 正在并行处理数据类型: [{dtype}] ...")
     # 获取基础路径（传给子进程，避免子进程重新读取全局配置）
    base_buffer = CONFIG.inferred_path['LEVEL2_BUFFER_PATH']
    
    try:
        # 定位暂存区根目录与最终输出根目录
        staging_root = base_buffer / f"monthlystaging/{dtype}/{checkyear}/{checkyear}{checkmonth}"
        monthly_root = base_buffer / f"monthly/{dtype}/{checkyear}/{checkyear}{checkmonth}"
        monthly_root.mkdir(parents=True, exist_ok=True)
        
        if not staging_root.exists():
            print(f"⚠️ [进程退出] 未找到 [{dtype}] 的暂存区目录 {staging_root}。")
            return
            
        current_schema = schema_dict.get(f"{dtype}_schema")
        
        # 1. 扫描暂存区下所有的股票文件夹
        stock_dirs = [d for d in staging_root.iterdir() if d.is_dir() and len(d.name) == 6]
        print(f"📊 [{dtype} 进程] 扫描到 {len(stock_dirs)} 只股票，开始融合...")
        
        for sdir in stock_dirs:
            stock_str = sdir.name  # 完美的 "000020" 格式
            
            # 2. 搜集该股票当月所有的每日小文件（升序排列）
            daily_files = sorted(list(sdir.glob(f"{stock_str}_*.parquet")))
            if not daily_files:
                continue
                
            try:
                # 3. 高性能 C++ 层多文件无拷贝融合
                merged_table = pq.read_table(daily_files, schema=current_schema)
                
                # 5. 精准定制月度单文件路径
                out_monthly_file = monthly_root / f"{stock_str}.parquet"
                
                # 6. 物理落盘
                with pq.ParquetWriter(out_monthly_file, current_schema, compression='snappy') as writer:
                    writer.write_table(merged_table)
                del merged_table
                
            except Exception as e:
                print(f"❌ [{dtype} 进程] 融合股票 {stock_str} 失败: {e}")
                continue
                
        # 强制单进程内的垃圾回收
        gc.collect()
        print(f"✨ [进程完成] 数据类型 [{dtype}] 的全月大融合落盘成功。")
        
    except Exception as e:
        print(f"💥 [{dtype} 进程] 发生严重异常崩溃: {e}\n{traceback.format_exc()}")

def generate_monthly_dateset(checkyear=0, checkmonth=''):
    """
    月底运行一次：
    1. 开启 3 个线程分别并行处理 order, deal, snapshot 三张表。
    2. 将暂存区内的多日碎片文件合并成“直接放置于该目录下”的股票月度大文件。
    3. 自动删除各表的暂存区中间文件夹（打扫战场）。
    """
    data_types=['order', 'order_raw', 'deal', 'snapshot']
    print(f"🎬 开始调度 {checkyear}-{checkmonth} 批次的多进程月度数据大融合...")
    
    task_args = [(dtype, checkmonth, checkyear) for dtype in data_types ]
    pool_size = len(data_types)
    
    # 3. 使用 with 语句自动上下管理进程池
    print(f"🌀 正在拉起大小为 {pool_size} 的进程池...")
    with Pool(processes=pool_size) as pool:
        # starmap 会阻塞在这里，直到列表里的 3 个任务全部执行完毕
        pool.starmap(_merge_single_data_type_worker, task_args)
        
    print(f"🏁 【全功告成】Batch {checkyear}-{checkmonth} 进程池内所有流水线均已安全返回。")
        


def _process_single_stock_monthly(args):
    """
    单只股票的月度 ETL 任务（运行在独立的 CPU 核心中）
    """
    stock_code, date_list, filetype = args
    formatted_code = str(stock_code).zfill(6)
    
    checkyear = '2026'
    checkmonth = '06'
    dst_dir = CONFIG.inferred_path['LEVEL2_BUFFER_PATH'] / f"monthlystaging/{filetype}/{checkyear}/{checkyear}{checkmonth}"

    # 目标月度文件路径
    dst_file = dst_dir / f"{formatted_code}.parquet"


    fileschema = f"{filetype}_schema"

    
    # 检查断点续传，获取已处理的最后一天
    lastday = 0

    if dst_file.exists():
        try:
            existing_snap = pd.read_parquet(dst_file, schema=schema_dict[fileschema], columns=['TradingDay'])
            if not existing_snap.empty:
                lastday = int(existing_snap['TradingDay'].iloc[-1])
            del existing_snap
        except Exception  as e:
            print(f"read snapshot {dst_file}error:{e}") # 读取失败则从头处理

    # 初始化本月新数据的内存收集器（减少磁盘IO次数）
    src_write = None

    try:
        for rday in date_list:
            # 过滤掉已处理的日期，或者当天停牌的股票
            src_table = None
       
            if rday <= lastday:
                continue
                
            # 日源文件路径
            src_file = CONFIG.inferred_path['LEVEL2_BUFFER_PATH'] / f"{filetype}/{checkyear}/{checkyear}{checkmonth}/{filetype}_{rday}.parquet"
            
            if not src_file.exists():
                continue
            
           
            src_table = pq.read_table(src_file, schema=schema_dict[fileschema], filters=[('SecuCode', '==', stock_code)])
           
            if src_table.num_rows > 0:
                if src_write is None:
                    src_write = pq.ParquetWriter(dst_file, schema_dict[fileschema] , compression='snappy')
                src_write.write_table(src_table)
            del src_table
        
        if src_write is not None:
            src_write.close()
            
        gc.collect()
        return stock_code, True, "Success"
        
    except Exception as e:
        error_stack = traceback.format_exc()
        return stock_code, False, error_stack
    finally:
        if src_write is not None:
            src_write.close()


def generate_monthly_level2_dataset():
    checkyear = '2026'
    checkmonth = '06'
    
    date_list = [
        20260601, 20260602, 20260603, 20260604, 20260605,
        #20260608, 20260609, 20260610, 20260611, 20260612,
        #20260615, 20260616, 20260617, 20260618,
        #20260622, 20260623, 20260624, 20260625, 20260626,
        #20260629, 20260630
    ] 

    # 目标目录字典
    dst_paths = {
        'order': CONFIG.inferred_path['LEVEL2_BUFFER_PATH'] / f"monthlystaging/order/{checkyear}/{checkyear}{checkmonth}",
        'order_raw': CONFIG.inferred_path['LEVEL2_BUFFER_PATH'] / f"monthlystaging/order_raw/{checkyear}/{checkyear}{checkmonth}",
        'deal': CONFIG.inferred_path['LEVEL2_BUFFER_PATH'] / f"monthlystaging/deal/{checkyear}/{checkyear}{checkmonth}",
        'snapshot': CONFIG.inferred_path['LEVEL2_BUFFER_PATH'] / f"monthlystaging/snapshot/{checkyear}/{checkyear}{checkmonth}"
    }

    for p in dst_paths.values():
        p.mkdir(parents=True, exist_ok=True)

    # 提取全月股票全集
    all_stocks = set()
    tasks = []
    for rday in date_list:        
        for filetype in ['order']:  
            tasks.append((rday, filetype))
    

    # 获取物理核心数，全负荷运转
    physical_cores = 1 #psutil.cpu_count(logical=False)
    logging.info(f"🚀 启动多进程池，核心数: {physical_cores}，正在疯狂计算中...")

    # 执行并行任务

    with Pool(physical_cores) as pool:
        # 使用 imap_unordered 实时获取股票完成进度
        
        results = pool.imap_unordered(_process_single_stock_monthly, tasks, chunksize=1)
        
        success_count = 0
        for stock_code, success, msg in results:
            if success:
                success_count += 1
                # 使用 \r 实现单行刷新进度条效果，避免日志刷屏
                print(f"进程推进中... 已成功合并 {success_count}/{len(tasks)} 只股票 [{stock_code}]", end="\r")
            else:
                logging.error(f"\n[X] 股票 {stock_code} 合并失败! 错误原因: {msg}")

    print("\n")
    logging.info(f"🎉 {checkyear}{checkmonth} 月度 L2 数据多进程规范化归档全部完成！")

if __name__ == '__main__':
    min1 = Resample(timeframe=TimeFrame.Minutes, compression=1)
    min5 = Resample(timeframe=TimeFrame.Minutes, compression=5)
    day1 = Resample(timeframe=TimeFrame.Days, compression=1)

    date_list = [
        20260601, 
        #20260602, 20260603, 20260604, 20260605,
        #20260608, 20260609, 20260610, 20260611, 20260612,
        #20260615, 20260616, 20260617, 20260618,
        #20260622, 20260623, 20260624, 20260625, 20260626,
        #20260629, 20260630
    ] 

    # generate_staging_order_and_deal_dateset(date_list=date_list, checkmonth='06', checkyear='2026')
    # generate_staging_snapshot_dateset(date_list=date_list, checkmonth='06', checkyear='2026')
    Verify_level2(date_list=date_list, checkmonth='06', checkyear='2026')
    #generate_monthly_dateset(checkmonth='06', checkyear='2026')
    #generate_monthly_level2_dataset()
    tasks = [ (20260601, 600000), (20260601, 564), (20260601, 725), (20260601, 727)]

    
