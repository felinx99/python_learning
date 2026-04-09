from abc import ABC, abstractmethod
from tqcenter import tq # type: ignore
import tushare as ts
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

class FeedBase(ABC):
    date_fmt = {
        'DAY': '%Y%m%d',
    }
    @abstractmethod
    def init_feed(self, **kwargs):
        pass

    @abstractmethod
    def get_sector_list(self, **kwargs):
        pass

    @abstractmethod
    def get_daily(self, **kwargs):
        pass

    @abstractmethod
    def get_stocklist_in_index(self, **kwargs):
        pass

    @abstractmethod
    def update_block(self, **kwargs):
        pass


class FeedProxy:
    def __init__(self, feedinstance):
        self._feedinstance = feedinstance

    def __getattr__(self, name):
        #代理访问Feed实例的方法
        return getattr(self._feedinstance, name)

class FeedManager:
    #数据管理器，管理不同来源的数据实例
    _feedcls = {}

    @classmethod
    def init(cls, name):
        def decorator(feed_cls):
            isinstance = feed_cls()  #实例化
            proxy = FeedProxy(isinstance)  #创建代理
            cls._feedcls[name] = proxy  #存储代理实例
            return proxy
        return decorator
    
    @classmethod
    def register(cls, name):
        """通过名称获取已经注册的代理对象"""
        return cls._feedcls.get(name)
    

@FeedManager.init('tdx')
class TdxFeed(FeedBase):
    @staticmethod
    def _data_to_df(data, type=None):
        """
        把tq.get_market_data返回的dict转换为DataFrame格式
        """
        CONFIG_MAP = {
            'market': {
                'rename': {'level_0': 'Date', 'level_1': 'Symbol'},
                'sort': ['Symbol', 'Date']
            },
            'sector': {
                'rename': {'level_0': 'sector_code', 'level_1': 'sector_name'},
                'sort': None
            }
        }
        combined = pd.concat(data.values(), keys=data.keys(), axis=0)
        df = combined.stack().unstack(level=0).reset_index()
        df.columns.name = None
        conf_type = CONFIG_MAP.get(type, None)
        if conf_type:           
                df.rename(columns=conf_type['rename'], inplace=True)
                if conf_type['sort']:
                    df = df.sort_values(by=conf_type['sort'], ascending=[True, True])  #按股票代码和日期排序
                    df = df.reset_index(drop=True)  #重置索引
                df.columns = df.columns.str.lower()
        return df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]

    @staticmethod
    def _L2Vol_to_df(data):
        """
        把tq.formula_process_mul_zb中'L2定单分析'公式返回的dict转换为DataFrame格式
        输出格式: date, symbol, huge_limit, huge_market, large_limit, large_market,
                 mid_limit, mid_market, small_limit, small_market
        """
        error_id = data.pop('ErrorId', '0')
        if error_id != '0':
            return None
        
        all_ret = []
        
        # 定义你需要提取的指标列名,tdx输入为全大写，不可更改
        cols = ['HUGE_LIMIT', 'HUGE_MARKET', 'LARGE_LIMIT', 'LARGE_MARKET', 
                'MID_LIMIT', 'MID_MARKET', 'SMALL_LIMIT', 'SMALL_MARKET']
        
        for symbol, fields in data.items():
            zipped_values = zip(
                [item['Date'] for item in fields['HUGE_LIMIT']],
                *[ [item['Value'] for item in fields[c]] for c in cols ]
            )
            for date, *values in zipped_values:
                all_ret.append((symbol, date, *values))

        # 4. 一次性合并（比循环 concat 快得多）
        final_df = pd.DataFrame.from_records(all_ret, columns=['symbol', 'date'] + cols).sort_values(['symbol', 'date']).reset_index(drop=True)
        final_df.columns = final_df.columns.str.lower()
        return final_df
        
    
    def init_feed(self, **kwargs):
        print("初始化通达信数据源")
        #初始化
        tq.initialize(__file__)

    def get_stocklist_in_index(self, **kwargs):
        sector = kwargs.get('sector', None)
        list_type = kwargs.get('list_type', 1)
        block_type = kwargs.get('block_type', 0) #1表示自定义板块
        
        stock_list = tq.get_stock_list_in_sector(block_code=sector, block_type=block_type, list_type=list_type)
        df = pd.DataFrame(stock_list)
        mapping = {0: 'stock_code', '1': 'stock_name'}
        df.rename(columns=mapping, inplace=True)
        return df
        
    def get_sector_list(self, **kwargs):
        #market默认为全部A股, 0:自选股 1:持仓股 5:所有A股 12:概念板块 15:缺省行业分类+概念板块 16:行业一级
        market = kwargs.get('sector_type', None)
        market_map = {
            'SECTOR_SELF': '0',  #自选股
            'SECTOR_HOLD': '1',  #持仓股
            'SECTOR_ALL': '5',  #所有A股
            'SECTOR_CONCEPT': '15',  #概念板块
            'SECTOR_L1': '16',  #行业一级
            'SECTOR_L2' : '17', #行业二级
            'SECTOR_L3' : '18' #行业三级

        }
        market = market_map.get(market, None)
        index_list = tq.get_stock_list(market=market, list_type=1)
        df = pd.DataFrame(index_list)
        mapping = {0: 'sector_code', '1': 'sector_name'}
        df.rename(columns=mapping, inplace=True)

        return df
    
    def get_daily(self, **kwargs):   
        symbol = kwargs.get('symbol', None)
        start_date = kwargs.get('start_date', None)
        end_date = kwargs.get('end_date', None)

        if type(symbol) is not list:
            symbol = [symbol]
        
        daily_dict = tq.get_market_data(
            field_list=[],
            stock_list=symbol,
            start_time=start_date,
            end_time=end_date,
            dividend_type='None', #不复权
            period='1d',
            fill_data=False      #不填充缺失数据
        )
        df = self._data_to_df(daily_dict, type='market')
        return df
    
    def update_block(self, **kwargs):
        print("更新通达信板块数据")
        block_name = kwargs.get('block_code', None)
        stock_list = kwargs.get('stock_list', None)
        #此处可添加更新板块数据的逻辑，如调用Tushare接口获取最新板块信息等
        tq.clear_sector(block_name)
        tq.send_user_block(block_name, stock_list, show=True)

    def get_L2Vol(self, **kwargs):
        today = datetime.now().strftime(FeedBase.date_fmt['DAY'])
        symbol = kwargs.get('symbol', None)
        start_date = kwargs.get('start_date', '20260101')
        end_date = kwargs.get('end_date', '20260301')

        if type(symbol) is not list:
            symbol = [symbol]

        mul_zb_res = tq.formula_process_mul_zb(
            formula_name='L2定单分析',
            formula_arg='',
            return_count=0,
            return_date=True,
            stock_list=symbol,
            stock_period='1d',
            start_time = start_date,
            end_time = end_date,
            count=0,
            dividend_type=1)
        df = self._L2Vol_to_df(mul_zb_res)
        return df
        
@FeedManager.init('tushare')
class TushareFeed(FeedBase):
    def init_feed(self, *args, **kwargs):
        print("初始化Tushare数据源")
        ts.set_token('665ecc81b76150c0ae793d389dbf298f5545abe110d5e862211430df')

    def get_sector_list(self, *args, **kwargs):
        print("获取Tushar板块列表")
        market = kwargs.get('sector_type', None)
        market_map = {
            'SECTOR_SELF': '0',  #自选股
            'SECTOR_HOLD': '1',  #持仓股
            'SECTOR_ALL': '5',  #所有A股
            'SECTOR_CONCEPT': '12',  #概念板块
            'SECTOR_L1': 'L1' #行业一级
        }
        market = market_map.get(market, None)
        pro = ts.pro_api()
        return pro.index_classify(level=market, src='SW2021')

    def get_stocklist_in_index(self, **kwargs):
        print(f"获取Tushare的成分股列表")
        #此处可添加根据指数名称获取对应成分股列表的逻辑

    def get_daily(self, **kwargs):
        symbol = kwargs.get('symbol', None)
        start_date = kwargs.get('start_date', None)
        end_date = kwargs.get('end_date', None)
        pro = ts.pro_api()
        return pro.index_daily(ts_code=symbol, start_date=start_date, end_date=end_date)
    
    def update_block(self, **kwargs):
        print("更新Tushare板块数据")
        block_name = kwargs.get('block_name', None)
        stock_list = kwargs.get('stock_list', None)
        #此处可添加更新板块数据的逻辑，如调用Tushare接口获取最新板块信息等

        
        
@FeedManager.init('akshare')
class AkshareFeed(FeedBase):
    def init_feed(self, **kwargs):
        pass

    def get_stocklist_in_index(self, **kwargs):
        sector_name = kwargs.get('sector_name', None)
        print(f"获取Akshare指数 '{sector_name}' 的成分股列表")
        #此处可添加根据指数名称获取对应成分股列表的逻辑

    def get_sector_list(self, *args, **kwargs):
        pass

    def get_daily(self, **kwargs):
        symbol = kwargs.get('symbol', None)
        start_date = kwargs.get('start_date', None)
        end_date = kwargs.get('end_date', None)
        pass

    def update_block(self, **kwargs):
        print("更新Tushare板块数据")
        block_name = kwargs.get('block_name', None)
        stock_list = kwargs.get('stock_list', None)
        #此处可添加更新板块数据的逻辑，如调用Tushare接口获取最新板块信息等
        
@FeedManager.init('csvFile')
class CsvFileFeed(FeedBase):
    def init_feed(self, **kwargs):
        print("初始化CSV文件数据源")
        #此处可添加CSV文件数据源的初始化逻辑，如设置文件路径等

    def get_stocklist_in_index(self, **kwargs):
        sector_name = kwargs.get('sector_name', None)
        print(f"获取CSV文件指数 '{sector_name}' 的成分股列表")
        #此处可添加根据指数名称获取对应成分股列表的逻辑
    
    def get_sector_list(self, **kwargs):
        pass

    def get_daily(self, **kwargs):
        symbol = kwargs.get('symbol', None)
        start_date = kwargs.get('start_date', None)
        end_date = kwargs.get('end_date', None)
        pass

    def update_block(self, **kwargs):
        print("更新Tushare板块数据")
        block_name = kwargs.get('block_name', None)
        stock_list = kwargs.get('stock_list', None)
        #此处可添加更新板块数据的逻辑，如调用Tushare接口获取最新板块信息等