import pandas as pd
from datetime import datetime
from common import CONFIG,DATAFRAME

# 配置路径
OUTPUT_PATH = CONFIG.base_path['STOCK_OUTPUT_PATH']

class StockPoolManager:
    def __init__(self):
        self.OUTPUT_PATH = OUTPUT_PATH
        self.POOL_FILE = self.OUTPUT_PATH/"selection_pool.csv"
        self.DELETED_FILE = self.OUTPUT_PATH/"deleted_pool.csv"

        self.selection_df = self._load_csv(self.POOL_FILE)
        self.deleted_df = self._load_csv(self.DELETED_FILE)

    def _load_csv(self, path):
        if path.exists():
            return pd.read_csv(path, dtype={'ts_code': str})
        return pd.DataFrame()
        
    
    def _save_csv(self):
        self.selection_df.to_csv(self.POOL_FILE, sep=',', encoding='utf-8-sig', index=False, date_format=CONFIG.date_fmt[DATAFRAME['DAY']], float_format='%.2f')
        self.deleted_df.to_csv(self.DELETED_FILE, encoding='utf-8-sig', index=False, date_format=CONFIG.date_fmt[DATAFRAME['DAY']], float_format='%.2f')

    # --- 调整 1：批量入池接口 ---
    def add_to_pool(self, newstock_df=None, strategy_name=''):
        """
        每执行完一个策略后调用一次
        """
        if newstock_df.empty:
            return

        now = datetime.now().strftime('%Y-%m-%d')
        
        for _, row in newstock_df.iterrows():
            code = row['Code']
            name = row['Name']
            
            # 1. 查重处理
            if not self.selection_df.empty and code in self.selection_df['ts_code'].values:
                idx = self.selection_df[self.selection_df['ts_code'] == code].index
                existing_reason = str(self.selection_df.loc[idx, 'in_type'].values[0])
                if strategy_name not in existing_reason:
                    self.selection_df.loc[idx, 'in_type'] = f"{existing_reason} | {strategy_name}"
                continue

            # 2. 如果在删除池，则移出（准备重新入池）
            if not self.deleted_df.empty and code in self.deleted_df['ts_code'].values:
                self.deleted_df = self.deleted_df[self.deleted_df['ts_code'] != code]

            # 3. 新增入池记录（此时不计算价格指标，只记录基础信息）
            new_record = {
                'ts_code': code, 'name': name, 'indate': now, 'in_type': strategy_name,
                'lowest': None, 'lowest_date': None, 'highest': None, 'highest_date': None,
                'maxdd': 0.0, 'maxdd_duration': None, 'highest_duration': 0
            }
            self.selection_df = pd.concat([self.selection_df, pd.DataFrame([new_record])], ignore_index=True)

    # --- 调整 2：流程重组的同步与更新函数 ---
    def sync_and_update(self, price_provider_func):
        """
        执行顺序：
        1. 先根据通达信 .blk 文件同步手动增删情况，完成“名单更新”
        2. 对更新后的名单统一进行价格特征指标计算
        3. 判定出池条件（时间、回撤）
        4. 保存并同步回通达信
        """
        # --- 第一步：同步通达信手动变更 (名单对齐) ---
        self._sync_with_tdx_manual()

        # --- 第二步：更新池内所有股票的价格特征 (数据补全) ---
        self._update_all_price_metrics(price_provider_func)

        # --- 第三步：出池逻辑判定 ---
        self._check_exit_conditions()

        # --- 第四步：最终持久化与反馈 ---
        self._write_tdx_blk("股票待选池.blk", self.selection_df['ts_code'].tolist())
        self._write_tdx_blk("已删股票池.blk", self.deleted_df['ts_code'].tolist())
        self._save_csv()

    def _sync_with_tdx_manual(self):
        """同步通达信板块文件的手工操作"""
        tdx_codes = self._read_tdx_blk("股票待选池.blk")
        csv_codes = self.selection_df['ts_code'].tolist() if not self.selection_df.empty else []

        # 1. 手工添加：通达信有，CSV没有
        for c in tdx_codes:
            if c not in csv_codes:
                # 注意：手动添加时由于没有 strategy_name，入池原因为“手工添加”
                # 名称暂时留空或通过后续 price_provider 获取
                self.add_to_pool(pd.DataFrame([{'code': c, 'name': '股票名称'}]), "manual")

        # 2. 手工删除：CSV有，通达信没有
        for c in csv_codes:
            if c not in tdx_codes:
                self._do_remove(c, "手工删除")

    def _update_all_price_metrics(self, price_provider_func):
        """统一更新待选池中所有股票的指标"""
        if self.selection_df.empty: return
        
        today = datetime.now()
        for idx, row in self.selection_df.iterrows():
            code = row['ts_code']
            entry_date = row['indate']
            
            # 调用你之前运行的数据模块获取的最新数据
            # 这里的 price_provider_func 应返回从入池日至今的 DataFrame
            hist = price_provider_func(code, entry_date)
            
            if hist is not None and not hist.empty:
                # 如果手动添加的股票没有名称，此处进行补全
                if row['name'] == 'manual':
                    self.selection_df.at[idx, 'name'] = hist['name'].iloc[-1] if 'name' in hist.columns else '未知'

                # 计算价格极值
                max_price = hist['close'].max()
                max_date = hist['close'].idxmax()
                min_price = hist['close'].min()
                min_date = hist['close'].idxmin()
                
                # 最大回撤
                post_max_series = hist.loc[max_date:]['close']
                max_drawdown = (post_max_series.min() - max_price) / max_price
                
                # 赋值
                self.selection_df.at[idx, 'highest'] = max_price
                self.selection_df.at[idx, 'highest_date'] = max_date
                self.selection_df.at[idx, 'lowest'] = min_price
                self.selection_df.at[idx, 'lowest_date'] = min_date
                self.selection_df.at[idx, 'maxdd'] = max_drawdown
                self.selection_df.at[idx, 'highest_duration'] = (today - pd.to_datetime(max_date)).days

    def _check_exit_conditions(self):
        """执行出池判定逻辑"""
        if self.selection_df.empty: return
        
        # 复制一份以防迭代时删除出错
        temp_df = self.selection_df.copy()
        for _, row in temp_df.iterrows():
            code = row['ts_code']
            # 条件 1：最高点超过 60 天
            if row['highest_duration'] > 60:
                self._do_remove(code, "最高点间隔超过60天")
            # 条件 2：回撤超过 40%
            elif row['maxdd'] <= -0.4:
                self._do_remove(code, "从最高点回撤超过40%")

    def _do_remove(self, code, reason):
        """执行删除的具体动作"""
        target_row = self.selection_df[self.selection_df['ts_code'] == code].iloc[0].to_dict()
        target_row['出池原因'] = reason
        # 移除不需要保存到已删池的列
        target_row.pop('highest_duration', None)
        
        self.deleted_df = pd.concat([self.deleted_df, pd.DataFrame([target_row])], ignore_index=True)
        self.selection_df = self.selection_df[self.selection_df['ts_code'] != code]

    # --- 通达信读写底层 ---
    def _read_tdx_blk(self, filename):
        path = self.TDX_BLOCK_PATH/filename
        if not path.exists(): return []
        with open(path, 'r') as f:
            return [line.strip()[-6:] for line in f.readlines() if line.strip()]

    def _write_tdx_blk(self, filename, code_list):
        path = self.TDX_BLOCK_PATH/filename
        with open(path, 'w') as f:
            for code in code_list:
                prefix = '1' if code.startswith(('6', '9')) else '0'
                f.write(f"{prefix}{code}\n")