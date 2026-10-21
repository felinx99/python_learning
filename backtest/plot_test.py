
import numpy as np
import talib as ta
import pandas as pd
import finplot as fpt
from common import CONFIG, DATAFRAME


if __name__ == '__main__':
    checkyear = '2026'
    checkmonth = '05'
    stock_str = '301080'
    rday = '20260506'
    window = 10

    file_path = CONFIG.tdx_data_path[DATAFRAME['DAY']]/f'{stock_str}.SZ.csv'
    if file_path.exists():
        dailybar_df = pd.read_csv(file_path, index_col='date', parse_dates=True)

    cancel_file = CONFIG.l2_path['L2_STAGE'] / f"cancel/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday}.parquet"
    #cancelorder_file = CONFIG.l2_path['L2_MATRICS']/'cancelorder'
    cancelorder_file = CONFIG.l2_path['L2_MATRICS'] / 'cancelorder'/f"cancelorder_{stock_str}.parquet"
    ordertier_m1_file = CONFIG.l2_path['L2_MATRICS'] / f"min1_dealtier/{checkyear}/min1_dealtier_{stock_str}.parquet"

    df_cancel = pd.read_parquet(cancel_file, schema=CONFIG.CANCEL_SCHEMA)           
    df_cancel['CancelTime'] = df_cancel['DealTime']
    df_cancelorder = pd.read_parquet(cancelorder_file)
    df_ordertier_m1 = pd.read_parquet(ordertier_m1_file)
    sum_cols = [col for col in df_ordertier_m1.columns if col not in ['TradingDay', 'Minute']]

    df_ordertier_daily = df_ordertier_m1.groupby('TradingDay', as_index=False)[sum_cols].sum()
   
    # ------------------ 数据预处理与时间截取 ------------------ #
    start_date = '2026-04-01'
    end_date = '2026-09-30'
    startdate_int = int(start_date.replace('-', ''))
    enddate_int = int(end_date.replace('-', ''))

    dailybar_df.index = pd.to_datetime(dailybar_df.index).normalize()
    dailybar_df = dailybar_df.sort_index()

    # 截取主图时间段
    dailybar_df_sub = dailybar_df.loc[start_date:end_date]

    # ------------------ 2. 副图 ordertier 数据对齐 ------------------ #
    df_cancelorder_sub = df_cancelorder[
        (df_cancelorder['TradingDay'] >= startdate_int) & 
        (df_cancelorder['TradingDay'] <= enddate_int)
    ].copy()

    df_cancelorder_sub['date'] = pd.to_datetime(df_cancelorder_sub['TradingDay'].astype(str), format='%Y%m%d').dt.normalize()
    df_cancelorder_sub.set_index('date', inplace=True)
    df_cancelorder_sub = df_cancelorder_sub.reindex(dailybar_df_sub.index).fillna(0)

    df_ordertier_sub = df_ordertier_daily[
        (df_ordertier_daily['TradingDay'] >= startdate_int) & 
        (df_ordertier_daily['TradingDay'] <= enddate_int)
    ].copy()

    df_ordertier_sub['date'] = pd.to_datetime(df_ordertier_sub['TradingDay'].astype(str), format='%Y%m%d').dt.normalize()
    df_ordertier_sub.set_index('date', inplace=True)
    df_ordertier_sub = df_ordertier_sub.reindex(dailybar_df_sub.index).fillna(0)

    # ------------------ 3. finplot 绘图 ------------------ #
    # 创建 2 行子图
    ax1, ax2, ax3, ax4, ax5 = fpt.create_plot(f'{stock_str} Daily Bar & Buy Cancel Tier', rows=5)

    # 设置主副图高度比例 3:1
    fpt.windows[-1].ci.layout.setRowStretchFactor(0, 2)
    fpt.windows[-1].ci.layout.setRowStretchFactor(1, 1)
    fpt.windows[-1].ci.layout.setRowStretchFactor(2, 1)
    fpt.windows[-1].ci.layout.setRowStretchFactor(3, 1)
    fpt.windows[-1].ci.layout.setRowStretchFactor(4, 1)

    # 主图：画 K 线图
    fpt.candle_bull_color = '#FF3333'        # 红色边框与影线
    fpt.candle_bull_body_color = '#FF333300' # #FF3333 + 00(全透明)

    fpt.candle_bear_color = '#00B050'        # 绿色边框与影线
    fpt.candle_bear_body_color = '#00B050'   # 实心绿
    fpt.candlestick_ochl(dailybar_df_sub[['open', 'close', 'high', 'low']], ax=ax1)

    # 【关键修正】：使用 volume_interval 画副图柱状图
    df_mega = df_ordertier_sub['deal_act_buy_vol_Mega']-df_ordertier_sub['deal_act_sell_vol_Mega']
    df_mega_ema = ta.EMA(df_mega, window)
    fpt.bar(
        df_mega_ema, 
        ax=ax2, 
        color='#4682b4',
        title='超大单主买'
    )

    df_large = df_ordertier_sub['deal_act_buy_vol_Large']-df_ordertier_sub['deal_act_sell_vol_Large']
    df_large_ema = ta.EMA(df_large, window)

    fpt.bar(
        df_large_ema, 
        ax=ax3, 
        color='#4682b4',
        title='大单主买'
    )

    df_medium = df_ordertier_sub['deal_act_buy_vol_Medium']-df_ordertier_sub['deal_act_sell_vol_Medium']
    df_medium_ema = ta.EMA(df_medium, window)
    fpt.bar(
        df_medium_ema, 
        ax=ax4, 
        color='#4682b4',
        title='中单主买'
    )

    df_small = df_ordertier_sub['deal_act_buy_vol_Small']-df_ordertier_sub['deal_act_sell_vol_Small']
    df_small_ema = ta.EMA(df_small, window)
    fpt.bar(
        df_small_ema, 
        ax=ax5, 
        color='#4682b4',
        title='小单主买'
    )

    # 显示图表
    fpt.show()
    pass
    