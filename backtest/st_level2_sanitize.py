import logging
import datetime
import psutil
import shutil
import traceback
import gc
import polars as pl
import pandas as pd
import numpy as np
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from common import CONFIG, DATAFRAME
from dataclasses import dataclass
from enum import IntEnum
from multiprocessing import Pool
from api.timeprofile import TimeProfile
from .util.order import OrderBook


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
    df = pd.read_parquet(srcfile, schema=CONFIG.DEAL_SCHEMA, columns=deal_columns, filters=[('TradingDay', '==', date_str)]) 

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

def stream_from_snapshot(df_day_pl):    
    df_day_pl = (
        df_day_pl
        .sort("TickTime")
        .with_columns([
            (pl.col("Turnover") * 100).round(0).cast(pl.Int64),
            (pl.col("TotalTurnover") * 100).round(0).cast(pl.Int64),
            (pl.col("WeightBidPrice") * 10).round(0).cast(pl.Int32),
            (pl.col("WeightAskPrice") * 10).round(0).cast(pl.Int32),
        ])
    )
    
    df_snapshot = df_day_pl.to_pandas()
    
    for row in df_snapshot.itertuples(index=False):
        decimals = 0 if row.WeightBidPrice % 10 == 0 else 1
        yield row, decimals, row.TickTime + 2000



def stream_from_deal(df_day_pl):
    CLOSE_TIME = 153000000  
    
    df_day_pl = (
        df_day_pl
        .sort(["DealTime", "DealID"])
        .with_columns([
            pl.when(~pl.col("Side").is_in([-1, -11]))
            .then(pl.col("DealTime"))
            .otherwise(None)
            .shift(-1)                                # 向前位移一位 (相当于 pandas shift(-1))
            .fill_null(strategy="backward")          # 向后填充 (相当于 bfill)
            .fill_null(CLOSE_TIME)                    # 尾部彻底兜底 (相当于 fillna)
            .cast(pl.Int32)
            .alias("DealTimeNext")
        ])
    )
    
    df_deal = df_day_pl.to_pandas()
    for row in df_deal.itertuples(index=False):
        yield row

def stream_from_order(df_day_pl):
    biz_idx_10 = df_day_pl["BizIndex"][10]
    order_id_10 = df_day_pl["OrderID"][10]
    if biz_idx_10 > 0 and biz_idx_10 != order_id_10:
        order_cond = 'BizIndex'
    else:
        order_cond = 'OrderID'

        
    df_day_pl = df_day_pl.sort(["OrderTime", order_cond])
    
    df_order = df_day_pl.to_pandas()
    for row in df_order.itertuples(index=False):
        yield row
    


def stream_l2_timeline(order_pl, deal_pl):
    """
    流式双指针归并：同时拉取订单流与成交流，就地按照时间线对齐。
    """
    order_stream = stream_from_order(order_pl)
    deal_stream = stream_from_deal(deal_pl)
    
    curr_order = next(order_stream, None)
    curr_deal = next(deal_stream, None)
    
    while curr_order and curr_deal:
        # 如果DealID与Order的序号无法匹配，还可以采用下面的终极解法，回归deal的本质流程中
        # if curr_order:
        #     candidates.append((curr_order.OrderTime, curr_order.OrderID, 0, 'Order', curr_order))

        # if curr_deal:
        #     deal_cond = max(curr_deal.BuyID, curr_deal.SellID)
        #     candidates.append((curr_deal.DealTime, deal_cond, 1, 'Deal', curr_deal))

        # 按时间升序排序
        # 相同时间下按订单号升序排序
        # 不能只按订单号，有的撤单订单号很小。要结合时间一起双排序
        o_time = curr_order.OrderTime
        o_cond = curr_order.BizIndex if curr_order.BizIndex > 0 else curr_order.OrderID
        
        d_time = curr_deal.DealTime
        d_cond = curr_deal.DealID

        # 优先级比较规则映射：时间小优先 -> 业务索引小优先 -> 订单优先于成交
        if o_time < d_time:
            yield 'Order', o_time, curr_order
            curr_order = next(order_stream, None)
        elif d_time < o_time:
            yield 'Deal', d_time, curr_deal
            curr_deal = next(deal_stream, None)
        else:
            # 当时间绝对相等时，比对第二排序列字段
            if o_cond < d_cond:
                yield 'Order', o_time, curr_order
                curr_order = next(order_stream, None)
            elif d_cond < o_cond:
                yield 'Deal', d_time, curr_deal
                curr_deal = next(deal_stream, None)
            else:
                # 业务索引也完全一致，依靠元组第三位：Order(0) 优先于 Deal(1)
                yield 'Order', o_time, curr_order
                curr_order = next(order_stream, None)

    # 消费剩余的订单单流
    while curr_order:
        yield 'Order', curr_order.OrderTime, curr_order
        curr_order = next(order_stream, None)

    # 消费剩余的成交单流
    while curr_deal:
        yield 'Deal', curr_deal.DealTime, curr_deal
        curr_deal = next(deal_stream, None)

def check_10_levels(ob=None, snapshot=None, decimals=0):
    _, total_dealvolume, total_turnover = ob.get_deal_status()
    tick_time = snapshot.TickTime
    if tick_time<92500000 or (145700000<=tick_time<150000000):
        return total_dealvolume == snapshot.TotalVolume and total_turnover == snapshot.TotalTurnover
    
    if snapshot.BidPrice5==0 and snapshot.AskPrice5==0:
        return total_dealvolume == snapshot.TotalVolume and total_turnover == snapshot.TotalTurnover

    my_tot_bid_vol, my_tot_ask_vol, my_w_bid_p, my_w_ask_p = ob.get_orderbook_stats()
    my_w_bid_p = round(my_w_bid_p*10)
    my_w_ask_p = round(my_w_ask_p*10)

    # 对于整数价格：标准整数，实际为小数，最大相差0.5。放大10倍后应该用6做容差
    # 对于小数价格：一般相差0.1，原来用0.2做容差。放大10倍后应该用2做容差
    price_tolerance = 6 if decimals == 0 else 2
   

    if my_tot_bid_vol == snapshot.TotalBidVolume and \
        my_tot_ask_vol == snapshot.TotalAskVolume and \
        total_dealvolume == snapshot.TotalVolume and \
        total_turnover == snapshot.TotalTurnover and \
        abs(my_w_bid_p - snapshot.WeightBidPrice) < price_tolerance and \
        abs(my_w_ask_p - snapshot.WeightAskPrice) < price_tolerance:

        VOL_THRESHOLD = 20  # 允许十档委托量的绝对值偏差

        # 1. 提取本地数据与快照数据 (直接使用原始 List[Tuple])
        my_bids = ob.get_topN_snapshot('B', n_levels=10)
        my_asks = ob.get_topN_snapshot('S', n_levels=10)

        cached_snap_bids = [(getattr(snapshot, f'BidPrice{i}'), getattr(snapshot, f'BidVolume{i}'), getattr(snapshot, f'BidOrder{i}')) for i in range(1, 11)]
        cached_snap_asks = [(getattr(snapshot, f'AskPrice{i}'), getattr(snapshot, f'AskVolume{i}'), getattr(snapshot, f'AskOrder{i}')) for i in range(1, 11)]

        def check_side_alignment(my_levels, snap_levels, vol_thresh):
            for (p1, v1, c1), (p2, v2, c2) in zip(my_levels, snap_levels):
                # 价格(p)和总单数(c)必须绝对精确对齐；量(v)允许误差
                if p1 != p2 or c1 != c2 or abs(v1 - v2) > vol_thresh:
                    return False
            return True

        match_levels = check_side_alignment(my_bids, cached_snap_bids, VOL_THRESHOLD) and \
                        check_side_alignment(my_asks, cached_snap_asks, VOL_THRESHOLD)

        if match_levels:
            return True

    return False

def rebuild_and_verify_OB(orday_daily_pl=None, deal_daily_pl=None, snapshot_daily_pl=None, need_checksnapshot = True):
    isSucess = True
    msg = ''
    ob = OrderBook() 

    if need_checksnapshot:
        snapshot_stream = stream_from_snapshot(snapshot_daily_pl)
        curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
        while curr_snapshot.TotalBidVolume==0 and curr_snapshot.TotalAskVolume==0:
            curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
        local_total_cumvolume = 0           #累积交易量，用于快速比较是否需要进行snapshot检验，不符的肯定不用检验
        
    # 直接消费磁盘流，内存开销恒定
    for event_type, event_time, row in stream_l2_timeline(orday_daily_pl, deal_daily_pl):  
        if event_type == 'Order':
            order_type = row.OrderType
            if order_type in [0, 2]:    # 委买/限价委买
                ob.insert_order(row.OrderID, row.Price, row.Volume, 'B')
            elif order_type in [10,12]: # 委卖/限价委卖
                ob.insert_order(row.OrderID, row.Price, row.Volume, 'S')
            elif order_type == 1: #市价委买
                ob.insert_order(row.OrderID, row.LastPrice, row.Volume, 'B')                
            elif order_type == 3: #本方最优委买
                ob.insert_order(row.OrderID, ob.get_bbo_bid(), row.Volume, 'B')
            elif order_type == 11: # 市价委卖
                ob.insert_order(row.OrderID, row.LastPrice, row.Volume, 'S')       
            elif order_type == 13: # 本方最优委卖
                ob.insert_order(row.OrderID, ob.get_bbo_ask(), row.Volume, 'S')
        # ==========================================
        elif event_type == 'Deal':
            side = row.Side
            if side in [0, 1]:
                # 交易所 L2 机制：一笔成交双边扣减                            
                ob.execute_trade(ref_id=row.BuyID, exec_qty=row.Volume, exec_price=row.Price)
                ob.execute_trade(ref_id=row.SellID, exec_qty=row.Volume, exec_price=row.Price)

                if need_checksnapshot:
                    local_total_cumvolume += row.Volume
                    if local_total_cumvolume == curr_snapshot.TotalVolume:
                        curr_snapshot_timeout = row.DealTimeNext

            elif side == -1:  # 买单撤单
                ob.cancel_order(ref_id=row.BuyID, cancel_qty=row.Volume)
            elif side == -11: # 卖单撤单
                ob.cancel_order(ref_id=row.SellID, cancel_qty=row.Volume)

        if need_checksnapshot and curr_snapshot is not None:
            # 1. 判断snapshot是否与当前ob是否相同。
            # 2. 不相同: 则读入下一个order/deal数据
            # 3. 相同: 读入下一个snapshot,判断与当前ob是否相同，相同继续读入下一个snapshot，直到不相同。
            # if event_time >= 93035010:
            #     pass
            if local_total_cumvolume == curr_snapshot.TotalVolume:
                verified = check_10_levels(ob=ob, snapshot=curr_snapshot, decimals=target_decimals)
                while verified:
                    curr_snapshot, target_decimals, curr_snapshot_timeout = next(snapshot_stream, (None,None,180000000))
                    if  curr_snapshot != None and local_total_cumvolume == curr_snapshot.TotalVolume:
                        verified = check_10_levels(ob=ob, snapshot=curr_snapshot, decimals=target_decimals)
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
                            verified = check_10_levels(ob=ob, snapshot=curr_snapshot, decimals=target_decimals)
                        else:
                            verified = False    #此分支不能删除，在while循环多次后需要退出

    # msg = "".join(msg_chunks)     
    return isSucess, msg

def rebuild_and_verify_daily(args):
    """
    从磁盘流式读取并构建订单薄
    :param order_file: 逐笔委托 parquet 文件路径
    :param deal_file: 逐笔成交 parquet 文件路径
    :param market: 市场标识，'SH' 代表沪市，'SZ' 代表深市
    :param batch_size: 磁盘缓存块大小
    """
    date_list, stock_str, checkyear, checkmonth, validate=args
    isSucess_ret = True
    msg_ret = []

    base_path = CONFIG.base_path['LEVEL2_BUFFER_PATH'] / 'monthlystaging'
    # 初始化上一轮构建好的 Level2OrderBook 实例 (Method 3 架构)
    
    trading_days = date_list
    for rday in trading_days:
    
        order_file = base_path / f"order/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday}.parquet"
        deal_file = base_path / f"deal/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday}.parquet"
        snapshot_file = base_path / f"snapshot/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday}.parquet"
        
        if not order_file.exists() or not deal_file.exists() or not snapshot_file.exists():
            print(f"⚠️ 找不到股票{stock_str}在{rday}对应的原始文件，自动跳过。")
            continue

        order_columns = ['BizIndex', 'LastPrice', 'OrderID', 'OrderTime', 'OrderType', 'Price', 'Volume']
        orday_daily_pl = pl.read_parquet(order_file, columns=order_columns)  

        deal_columns = ['Price', 'DealTime', 'Volume', 'Side', 'BuyID', 'DealID', 'SellID']
        deal_daily_pl = pl.read_parquet(deal_file, columns=deal_columns)  
        
        snapshot_columns = ['DealNum', 'Price', 'TickTime', 'TotalAskVolume', 'TotalBidVolume', 'TotalDealNum', 'TotalTurnover', 'TotalVolume', 'Turnover', 'Volume', 'WeightAskPrice', 'WeightBidPrice']
        for i in range(1,11):
            snapshot_columns.extend([f'BidPrice{i}', f'BidVolume{i}', f'BidOrder{i}'])
            snapshot_columns.extend([f'AskPrice{i}', f'AskVolume{i}', f'AskOrder{i}'])
    
        snapshot_daily_pl = pl.read_parquet(snapshot_file, columns=snapshot_columns)

        isSucess, msg = rebuild_and_verify_OB(orday_daily_pl, deal_daily_pl, snapshot_daily_pl, validate)  
        if not isSucess:
            msg_ret.append(f"[X] 失败 | {rday} : {stock_str} | \n 错误信息: {msg}") 
            isSucess_ret = False
        # 清理单日内存占用
        del orday_daily_pl, deal_daily_pl, snapshot_daily_pl
        # gc.collect()

    return stock_str, isSucess_ret, msg_ret

def rebuild_and_verify_monthly(args):
    date_list, stock_str, checkyear, checkmonth, validate=args
    isSucess_ret = True
    msg_ret = []

    base_path = CONFIG.base_path['LEVEL2_BUFFER_PATH'] / 'monthly'
    order_file = base_path / f"order/{checkyear}/{checkyear}{checkmonth}/{stock_str}.parquet"
    deal_file = base_path / f"deal/{checkyear}/{checkyear}{checkmonth}/{stock_str}.parquet"
    snapshot_file = base_path / f"snapshot/{checkyear}/{checkyear}{checkmonth}/{stock_str}.parquet"

    if not order_file.exists() or not deal_file.exists() or not snapshot_file.exists():
        print(f"⚠️ 找不到股票{stock_str}本月对应的原始文件，自动跳过。")
        return stock_str, isSucess_ret, msg_ret

    order_columns = ['BizIndex', 'LastPrice', 'OrderID', 'OrderTime', 'OrderType', 'Price', 'Volume', 'TradingDay']
    order_monthly_pl = pl.read_parquet(order_file, columns=order_columns)  

    deal_columns = ['Price', 'DealTime', 'Volume', 'Side', 'BuyID', 'DealID', 'SellID', 'TradingDay']
    deal_monthly_pl = pl.read_parquet(deal_file, columns=deal_columns)  
    
    snapshot_columns = ['DealNum', 'Price', 'TickTime', 'TotalAskVolume', 'TotalBidVolume', 'TotalDealNum', 'TotalTurnover', 'TotalVolume', 'Turnover', 'Volume', 'WeightAskPrice', 'WeightBidPrice', 'TradingDay']
    for i in range(1,11):
        snapshot_columns.extend([f'BidPrice{i}', f'BidVolume{i}', f'BidOrder{i}'])
        snapshot_columns.extend([f'AskPrice{i}', f'AskVolume{i}', f'AskOrder{i}'])
   
    snapshot_monthly_pl = pl.read_parquet(snapshot_file, columns=snapshot_columns)
    
    order_dict = order_monthly_pl.partition_by("TradingDay", as_dict=True)
    deal_dict = deal_monthly_pl.partition_by("TradingDay", as_dict=True)
    snapshot_dict = snapshot_monthly_pl.partition_by("TradingDay", as_dict=True)

    trading_days = sorted(list(order_dict.keys()))

    for rday in trading_days:
        orday_daily_pl = order_dict[rday]
        deal_daily_pl = deal_dict.get(rday, pl.DataFrame(schema=deal_monthly_pl.schema))
        snapshot_daily_pl = snapshot_dict.get(rday, pl.DataFrame(schema=snapshot_monthly_pl.schema))

        isSucess, msg = rebuild_and_verify_OB(orday_daily_pl, deal_daily_pl, snapshot_daily_pl, validate)
        if not isSucess:
            msg_ret.append(f"[X] 失败 | {rday} : {stock_str} | \n 错误信息: {msg}") 
            isSucess_ret = False
        del orday_daily_pl, deal_daily_pl, snapshot_daily_pl
        # gc.collect()

    return stock_str, isSucess_ret, msg_ret

def Verify_level2(date_list=[], checkyear='', checkmonth='', validate=True, period='daily'):
    tasks = []
    staging_root = None
    stockstr_list = []
    physical_cores = 4
    subprocess = None

    if period == 'monthly':
        staging_root = CONFIG.base_path['LEVEL2_BUFFER_PATH'] / f"monthly/snapshot/{checkyear}/{checkyear}{checkmonth}"
        stockstr_list = [d.stem for d in staging_root.glob(f"*.parquet")]
        physical_cores = 4
        subprocess = rebuild_and_verify_monthly
    elif period == 'daily':
        staging_root = CONFIG.base_path['LEVEL2_BUFFER_PATH']/f"monthlystaging/snapshot/{checkyear}/{checkyear}{checkmonth}"
        assert staging_root.exists(), f"Error: '{staging_root}'"
        stockstr_list = [d.name for d in staging_root.iterdir() if d.is_dir() and len(d.name) == 6]
        physical_cores = psutil.cpu_count(logical=False)
        subprocess = rebuild_and_verify_daily
    
    for stock_str in stockstr_list:
        tasks.append((date_list, stock_str, checkyear, checkmonth, validate))
    
    with Pool(physical_cores) as p:
        print(f" 步骤 4：自建订单薄，与快照比对进行数据校验，")        
        results = p.imap_unordered(subprocess, tasks, chunksize=50)
        success_count = 0
        for stock_str, success, msg in results:
            if success:
                success_count += 1
                print(f"进程推进中.. 已成功合并 {success_count}/{len(tasks)} 只股票 [{stock_str}]", end="\r")
            else:
                logging.error(msg)

def fix_order_bizindex(order_df=None, deal_df=None):
    ret = True
    order_cond = ''

    if order_df['BizIndex'].iat[10] > 0 and order_df['BizIndex'].iat[10] != order_df['OrderID'].iat[10]:    
        buy_min = deal_df.loc[deal_df['BuyID'] > 0].groupby('BuyID')['DealID'].min()
        sell_min = deal_df.loc[deal_df['SellID'] > 0].groupby('SellID')['DealID'].min()
        
        sweep_map = pd.concat([buy_min, sell_min]).groupby(level=0).min()
        order_df['min_deal_id'] = order_df['OrderID'].map(sweep_map)
        mask = (order_df['BizIndex'] > order_df['min_deal_id']) & (~order_df['OrderType'].isin([-1, -11]))
        
        if mask.any():
            order_df.loc[mask, 'BizIndex'] = order_df.loc[mask, 'min_deal_id'].astype('int32')
        
        order_df.drop(columns=['min_deal_id'], inplace=True)
        order_cond = 'BizIndex'
        ret = False
    else:
        order_cond = 'OrderID'
        ret = True

    order_df = order_df.sort_values(by=['OrderTime', order_cond], ascending=True, kind='stable').reset_index(drop=True)
    return order_df, ret

# =================================================================
# 1. Order & Deal 增量分流与对齐函数（单线程）
# =================================================================
def check_deal(df_order, df_deal):
    ismissing = True
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
                ismissing = False
                df_deal = pd.concat([df_deal, missing_deal_rows], ignore_index=True)
    

    df_deal = df_deal.sort_values(by=['DealTime', 'DealID'], ascending=True, ignore_index=True)
    return df_deal, ismissing



def presplit(date_list=[], checkmonth='', checkyear='2026'):
    """
    每日或隔2~3日运行（单线程）：
    1. 读取 snapshot 每日全市场大文件。
    2. 极速拆分并分流存储进暂存区股票文件夹中。
    """
    print(f"🎬  步骤 1：对 {checkyear}-{checkmonth} 批次 {date_list} 的全市场大文件进行分盘...")
    
    base_buffer = CONFIG.base_path['LEVEL2_BUFFER_PATH']
    
    # 约定暂存区根目录

    dst_dir_order = base_buffer / f"monthlystaging/order/{checkyear}/{checkyear}{checkmonth}"
    dst_dir_deal = base_buffer / f"monthlystaging/deal/{checkyear}/{checkyear}{checkmonth}"
    dst_dir_snapshot = base_buffer / f"monthlystaging/snapshot/{checkyear}/{checkyear}{checkmonth}"
    dst_dir_order_raw = base_buffer / f"monthlystaging/order_raw/{checkyear}/{checkyear}{checkmonth}"

    for rday in date_list:
        src_order = base_buffer / f"order/{checkyear}/{checkyear}{checkmonth}/order_{rday}.parquet"
        src_deal = base_buffer / f"deal/{checkyear}/{checkyear}{checkmonth}/deal_{rday}.parquet"
        src_snapshot = base_buffer / f"snapshot/{checkyear}/{checkyear}{checkmonth}/snapshot_{rday}.parquet"
        src_order_raw = base_buffer / f"order_raw/{checkyear}/{checkyear}{checkmonth}/order_raw_{rday}.parquet"

        order_scanner = ds.dataset(src_order, schema=CONFIG.ORDER_SCHEMA).scanner(use_threads=False)
        deal_scanner = ds.dataset(src_deal, schema=CONFIG.DEAL_SCHEMA).scanner(use_threads=False)
        snapshot_scanner = ds.dataset(src_snapshot, schema=CONFIG.SNAPSHOT_SCHEMA).scanner(use_threads=False)
        order_raw_scanner = ds.dataset(src_order_raw, schema=CONFIG.ORDERRAW_SCHEMA).scanner(use_threads=False)

        tmp_order_split = dst_dir_order / "tmp_split"
        tmp_deal_split = dst_dir_deal / "tmp_split"
        tmp_snapshot_split = dst_dir_snapshot / "tmp_split"
        tmp_order_raw_split = dst_dir_order_raw / "tmp_split"

        tmp_order_split.mkdir(parents=True, exist_ok=True)
        tmp_deal_split.mkdir(parents=True, exist_ok=True)
        tmp_snapshot_split.mkdir(parents=True, exist_ok=True)
        tmp_order_raw_split.mkdir(parents=True, exist_ok=True)

        name_template = f"{rday}_{{i}}.parquet"

        print(f"⚡ 正在处理交易日{rday}全市场大文件")
        try:
            # 使用 Dataset API 流式读取并自动切分，规避将整个大表加载进内存
            print("1.1 处理order文件中...")
            ds.write_dataset(
                order_scanner,
                base_dir=tmp_order_split,
                format="parquet",
                partitioning=["SecuCode"],
                basename_template=name_template,
                max_open_files=10000,
                max_rows_per_file=0,
                existing_data_behavior="overwrite_or_ignore"
            )
            print("order文件处理完成")
            print("1.2 处理deal文件中...")
            ds.write_dataset(
                deal_scanner,
                base_dir=tmp_deal_split,
                format="parquet",
                partitioning=["SecuCode"],
                basename_template=name_template,
                max_open_files=10000,
                max_rows_per_file=0,
                existing_data_behavior="overwrite_or_ignore"
            )
            print("deal文件处理完成")
            print("1.3 处理snapshot文件中...")
            ds.write_dataset(
                snapshot_scanner,
                base_dir=tmp_snapshot_split,
                format="parquet",
                partitioning=["SecuCode"],
                basename_template=name_template,
                max_open_files=10000,
                max_rows_per_file=0,
                existing_data_behavior="overwrite_or_ignore"
            )
            print("snapshot文件处理完成")
            print("1.4 处理order_raw文件中...")
            ds.write_dataset(
                order_raw_scanner,
                base_dir=tmp_order_raw_split,
                format="parquet",
                partitioning=["SecuCode"],
                basename_template=name_template,
                max_open_files=10000,
                max_rows_per_file=0,
                existing_data_behavior="overwrite_or_ignore"
            )
            print("order_raw文件处理完成")
        except Exception as e:
            print(f"❌ 写入中途崩溃！触发安全机制，正在清理脏数据... 错误: {e}")
            if tmp_order_split.exists():
                shutil.rmtree(tmp_order_split) # 宁可玉碎，不可留残缺文件
            if tmp_deal_split.exists():
                shutil.rmtree(tmp_deal_split) # 宁可玉碎，不可留残缺文件
            if tmp_snapshot_split.exists():
                shutil.rmtree(tmp_snapshot_split) # 宁可玉碎，不可留残缺文件
            if tmp_order_raw_split.exists():
                shutil.rmtree(tmp_order_raw_split) # 宁可玉碎，不可留残缺文件
            raise e

def generate_staging_order_and_deal_dateset(date_list=[], checkmonth='', checkyear='2026'):
    """
    每日或隔2~3日运行（单线程）：
    1. 读取 order, deal 每日全市场大文件并按股票分流。
    2. 对比补齐 deal 中缺失的 [-1, -11] 撤单数据。
    3. 增量追加进 Staging 暂存区对应的股票文件夹中。
    """
    print(f"🎬 步骤 2：对{checkyear}-{checkmonth} 批次 {date_list} 的 Order & Deal 增量数据清洗...")
    
    base_buffer = CONFIG.base_path['LEVEL2_BUFFER_PATH']
    
    # 约定暂存区根目录
    dst_dir_order = base_buffer / f"monthlystaging/order/{checkyear}/{checkyear}{checkmonth}"
    dst_dir_deal = base_buffer / f"monthlystaging/deal/{checkyear}/{checkyear}{checkmonth}"
    dst_dir_order.mkdir(parents=True, exist_ok=True)
    dst_dir_deal.mkdir(parents=True, exist_ok=True)

    stage_order_root = dst_dir_order /'tmp_split'
    msg_chunks = []

    for rday in date_list:
        print(f"⏱️ 正在处理交易日{rday}的 Order/Deal 数据...")          
        try:
            # 3.1 读取每日全市场大文件（全天只读一次）
            stockcode_list = [d.name for d in stage_order_root.iterdir() if d.is_dir()]
            total_stocks = len(stockcode_list)

            for idx, stock_code in enumerate(stockcode_list):
                stock_str = str(stock_code).zfill(6)

                src_order = dst_dir_order / f"tmp_split/{stock_code}/{rday}_0.parquet"
                src_deal = dst_dir_deal / f"tmp_split/{stock_code}/{rday}_0.parquet"
                if not src_order.exists() or not src_deal.exists():
                    print(f"⚠️ 找不到{rday}股票{stock_str}的order或deal 原始文件，自动跳过。")
                    continue

                df_order = pq.read_table(src_order).to_pandas()
                df_deal = pq.read_table(src_deal).to_pandas()
                if df_order.empty or df_deal.empty:
                    continue
                #补齐ds.write_dataset(..., partitioning=["SecuCode"])时被系统删除的Secucode
                df_order['SecuCode'] = stock_code
                df_deal['SecuCode'] = stock_code
                # 修正在吞单情况下，order的BizIndex序号比deal大的错误，导致deal先执行Order还未生成
                df_order, ret = fix_order_bizindex(df_order, df_deal)
                if not ret:
                    msg_chunks.append(f"{rday} 股票{stock_str} BizIndex错误 \n")
                # 补齐 deal中缺失的 [-1, -11] 的撤单数据
                df_deal, ret = check_deal(df_order, df_deal)
                if not ret:
                    msg_chunks.append(f"{rday} 股票{stock_str} deal缺失撤单信号 \n")

                #对order和deal排序
                if df_order['BizIndex'].iat[10] > 0 and df_order['BizIndex'].iat[10] != df_order['OrderID'].iat[10]:
                    order_cond = 'BizIndex'
                else:
                    order_cond = 'OrderID'
                df_order = df_order.sort_values(by=['OrderTime', order_cond], ascending=True, kind='stable').reset_index(drop=True)
                df_deal = df_deal.sort_values(by=['DealTime', 'DealID'], ascending=True, kind='stable').reset_index(drop=True)
                stock_order_dir = dst_dir_order / stock_str
                stock_deal_dir = dst_dir_deal / stock_str
                stock_order_dir.mkdir(parents=True, exist_ok=True)
                stock_deal_dir.mkdir(parents=True, exist_ok=True)

                out_order_file = stock_order_dir / f"{stock_str}_{rday}.parquet"
                out_deal_file = stock_deal_dir / f"{stock_str}_{rday}.parquet"

                df_order.to_parquet(out_order_file, schema=CONFIG.ORDER_SCHEMA, engine='pyarrow', compression='zstd', index=False)
                df_deal.to_parquet(out_deal_file, schema=CONFIG.DEAL_SCHEMA, engine='pyarrow', compression='zstd', index=False)

                del df_order, df_deal
                # 如果清洗完想腾出磁盘空间，可以在这里直接把暂存区的输入文件删掉
                src_order.unlink()
                src_deal.unlink()
                print(f"已成功合并 {idx}/{total_stocks} 只股票 [{stock_str}]", end="\r")
            
            # 严格释放内存
            gc.collect()
            
        except Exception as e:
            msg_chunks.append(f"❌ 处理交易日 {rday} (Order/Deal) 失败, 代码异常")
            print(f"❌ 处理交易日 {rday} (Order/Deal) 失败: {e}\n{traceback.format_exc()}")
    
    ret_msg = "".join(msg_chunks)
    return ret_msg


# =================================================================
# 2. Snapshot 增量分流函数（单线程）
# =================================================================
def generate_staging_snapshot_dateset(date_list=[], checkyear='2026', checkmonth=''):
    """
    每日或隔2~3日运行（单线程）：
    1. 读取 snapshot 每日全市场大文件。
    2. 极速拆分并分流存储进暂存区股票文件夹中。
    """
    print(f"🎬 步骤 3：对{checkyear}-{checkmonth} 批次 {date_list} 的 Snapshot & Order_raw 增量分流...")
    
    base_buffer = CONFIG.base_path['LEVEL2_BUFFER_PATH']
    
    # 约定暂存区根目录
    dst_dir_snapshot = base_buffer / f"monthlystaging/snapshot/{checkyear}/{checkyear}{checkmonth}"
    dst_dir_order_raw = base_buffer / f"monthlystaging/order_raw/{checkyear}/{checkyear}{checkmonth}"
    dst_dir_snapshot.mkdir(parents=True, exist_ok=True)
    dst_dir_order_raw.mkdir(parents=True, exist_ok=True)

    stage_order_root = dst_dir_order_raw /'tmp_split' 

    for rday in date_list:
        print(f"⏱️ 正在处理交易日{rday}的 Snapshot/Order_raw 数据 ...")            
        try:
            # 提取全市场去重股票代码列表 (int 类型)
            stockcode_list = [d.name for d in stage_order_root.iterdir() if d.is_dir()]
            total_stocks = len(stockcode_list)

            for idx, stock_code in enumerate(stockcode_list):
                stock_str = str(stock_code).zfill(6)

                src_snapshot = dst_dir_snapshot / f"tmp_split/{stock_code}/{rday}_0.parquet"
                src_order_raw = dst_dir_order_raw / f"tmp_split/{stock_code}/{rday}_0.parquet"
                if not src_snapshot.exists() or not src_order_raw.exists():
                    print(f"⚠️ 找不到{rday}股票{stock_str}的snapshot或order_raw 原始文件，自动跳过。")
                    continue

                df_snapshot = pq.read_table(src_snapshot).to_pandas()
                df_order_raw = pq.read_table(src_order_raw).to_pandas()
                if df_snapshot.empty or df_order_raw.empty:
                    continue
                #补齐ds.write_dataset(..., partitioning=["SecuCode"])时被系统删除的Secucode
                df_snapshot['SecuCode'] = stock_code
                df_order_raw['SecuCode'] = stock_code

                stock_snapshot_dir = dst_dir_snapshot / stock_str
                stock_order_raw_dir = dst_dir_order_raw / stock_str
                stock_snapshot_dir.mkdir(parents=True, exist_ok=True)
                stock_order_raw_dir.mkdir(parents=True, exist_ok=True)

                out_snapshot_file = stock_snapshot_dir / f"{stock_str}_{rday}.parquet"
                out_order_raw_file = stock_order_raw_dir / f"{stock_str}_{rday}.parquet"

                # 直接物理落盘
                df_snapshot.to_parquet(out_snapshot_file, schema=CONFIG.SNAPSHOT_SCHEMA, engine='pyarrow', compression='zstd', index=False)
                df_order_raw.to_parquet(out_order_raw_file, schema=CONFIG.ORDERRAW_SCHEMA, engine='pyarrow', compression='zstd', index=False)


                del df_snapshot, df_order_raw  
                # 如果清洗完想腾出磁盘空间，可以在这里直接把暂存区的输入文件删掉
                src_snapshot.unlink()
                src_order_raw.unlink() 
                print(f"已成功合并 {idx}/{total_stocks} 只股票 [{stock_str}]", end="\r") 
            # 严格释放全市场大表内存

            gc.collect()
            
        except Exception as e:
            print(f"❌ 处理快照交易日 {rday} 失败: {e}\n{traceback.format_exc()}")


# =================================================================
# 3. 月度合并与打扫战场函数（三线程并行）
# =================================================================
def _merge_single_stock(dtype='', checkmonth='', checkyear='2026'):
    print(f"🚀 [进程启动] 正在并行处理数据类型: [{dtype}] ...")
     # 获取基础路径（传给子进程，避免子进程重新读取全局配置）
    base_buffer = CONFIG.base_path['LEVEL2_BUFFER_PATH']
    
    try:
        # 定位暂存区根目录与最终输出根目录
        staging_root = base_buffer / f"monthlystaging/{dtype}/{checkyear}/{checkyear}{checkmonth}"
        monthly_root = base_buffer / f"monthly/{dtype}/{checkyear}/{checkyear}{checkmonth}"
        monthly_root.mkdir(parents=True, exist_ok=True)
        
        if not staging_root.exists():
            print(f"⚠️ [进程退出] 未找到 [{dtype}] 的暂存区目录 {staging_root}。")
            return

        schema_dict = {
            'order_schema' : CONFIG.ORDER_SCHEMA,
            'order_raw_schema' : CONFIG.ORDERRAW_SCHEMA,
            'deal_schema' : CONFIG.DEAL_SCHEMA,
            'snapshot_schema' : CONFIG.SNAPSHOT_SCHEMA,
        }    
        current_schema = schema_dict.get(f"{dtype}_schema")

        sorted_dict = {
            'order' : ['TradingDay', 'OrderTime'],
            'order_raw' : ['TradingDay', 'OrderTime'],
            'deal' : ['TradingDay', 'DealID'],
            'snapshot' : ['TradingDay', 'TickTime'],
        }
        current_sorted = sorted_dict.get(dtype)
        
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
                merged_table = pq.read_table(daily_files, schema=current_schema) 
                sort_keys = [(col, "ascending") for col in current_sorted]
                sort_indices = pc.sort_indices(merged_table, sort_keys=sort_keys)
                merged_table = merged_table.take(sort_indices)

                out_monthly_file = monthly_root / f"{stock_str}.parquet"
                with pq.ParquetWriter(out_monthly_file, current_schema, compression='zstd') as writer:
                    writer.write_table(merged_table, row_group_size=200_000)
                del merged_table
            
                #待补充，代码及逻辑验证通过后，这里要补充暂存区文件删除的清理工作
                pass

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
    1. 开启 4 个线程分别并行处理 order, deal, snapshot, order_raw 四张表。
    2. 将暂存区内的多日碎片文件合并成“直接放置于该目录下”的股票月度大文件。
    3. 自动删除各表的暂存区中间文件夹（打扫战场）。
    """
    data_types=['order', 'order_raw', 'deal', 'snapshot']
    print(f"🎬 开始调度 {checkyear}-{checkmonth} 批次的多进程月度数据大融合...")
    
    task_args = [(dtype, checkmonth, checkyear) for dtype in data_types ]
    pool_size = len(data_types)
    
    print(f"🌀 正在拉起大小为 {pool_size} 的进程池...")
    with Pool(processes=pool_size) as pool:
        pool.starmap(_merge_single_stock, task_args)
        
    print(f"🏁 【全功告成】Batch {checkyear}-{checkmonth} 进程池内所有流水线均已安全返回。")


if __name__ == '__main__':
    min1 = Resample(timeframe=TimeFrame.Minutes, compression=1)
    min5 = Resample(timeframe=TimeFrame.Minutes, compression=5)
    day1 = Resample(timeframe=TimeFrame.Days, compression=1)

    # date_list = [
    #     20260601, 20260602, 20260603, 20260604, 20260605,        
    #     20260608, 20260609, 20260610, 20260611, 20260612,
    #     20260615, 20260616, 20260617, 20260618,
    #     20260622, 20260623, 20260624, 20260625, 20260626,
    #     20260629, 20260630
    # ] 
    date_list = [
        # 20260701, 20260702, 20260703,         
        20260706, 
        # 20260707, 20260708, 20260709, 20260710,
        # 20260713, 20260714, 20260715, 20260716, 20260717,
        # 20260720, 20260721, 20260722, 20260723, 20260724,
        # 20260727, 20260728, 20260729, 20260730, 20260721
    ]
    # ------------ 每日更新任务 --------------------
    # presplit(date_list=date_list, checkmonth='07', checkyear='2026')
    # msg = generate_staging_order_and_deal_dateset(date_list=date_list, checkmonth='07', checkyear='2026')
    # if msg: logging.warning(msg)
    # generate_staging_snapshot_dateset(date_list=date_list, checkmonth='07', checkyear='2026')
    Verify_level2(date_list=date_list, checkmonth='07', checkyear='2026', period='daily', validate=True)    #每日更新
    
    # ------------ 月底存档任务 --------------------
    # generate_monthly_dateset(checkmonth='06', checkyear='2026')
    # Verify_level2(checkmonth='06', checkyear='2026', period='monthly', validate=True)   #月底存档文件检查

    
