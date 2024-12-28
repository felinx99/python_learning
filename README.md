# Aglo Trader

## Intro

This is my repo for backtesting algorithmic trading strategies.

Implemented with Backtrader in Python.

'''
## 调用说明
AIzaSyAArtu952QWf37FrAp4qygPA8sTmuwQJCU

支持回测和交易两种使用模式。

回测模式：针对有限数据集，进行策略回测，输出回测结果。支持单策略测试，多策略测试和策略调优。

            -有限数据集一般指数据文件，有限长度在线数据如历史数据。

            -回测结果包含指标数据，图表展示，结果文件

            -多策略测试一般用于策略对比，一个为基准策略，其他为对比策略。

            -策略调优一般用于策略参数优化，指定参数范围，步长，输出最优参数。

交易模式：针对实时交易数据，执行策略进行实盘交易。支持实盘交易，策略报告输出。

            -实时交易数据一般指在线数据，如实时行情和实时历史数据。数据无限长,且由交易平台提供。

            -实盘交易包含下单，撤单，成交，回报等。

            -策略报告输出包含策略表现，成交记录，回报记录等。
'''

## 参数说明

runmode: 回测模式：'backtest', 交易模式：'living_trading', 'paper_trading'

datatype: 回测模式下两种: file,historical_limit,

          交易模式下两种: realtime, hitsorical_update


### Arguments:

| Arg          | Flag           | Possible Values             | Description                                                                                 |
| ------------ | -------------- | --------------------------- | ------------------------------------------------------------------------------------------- |
| running mode | runmode        | backtest,living_trading,etc | running mode
| strategy     |                | BuyAndHold, CrossOver, etc. | Choose from the list of algorithms in the ./backtest/algos/. The arg value is the filename. |
| tickers      | -t, --tickers  | SPY, AAPL, etc.             | A list of tickers to use.                                                                   |
| universe     | -u, --universe | sp500, faang, etc.          | Find the list of uniuverses in ./backtest/utils/universe.py    
| data type    | -d, --datatype | file, historical_limit, etc.| Choose for description data source type
| start        | -s, --start    | 2010, 2010-01-01            | Starting date of the                              |
| start        | -s, --start    | 2010, 2010-01-01            | Starting date of the backtest                                                               |
| end          | -e, --end      | 2022, 2021-12-31            | End date for backtest                                                                       |
| cash         | --cash         | 100000                      | Starting cash balance                                                                       |
| verbose      | -v, --verbose  |                             | Show verbose details of all trades                                                          |
| plot         | -p, --plot     |                             | Show the full plot                                                                          |
| plot returns | --plotreturns  |                             | Only plot the returns                                                                       |
| kwargs       | -k, --kwargs   |                             | Additional arguments to pass through to the strategy                                        |


## Run a backtest

```
python -m backtest.run BuyAndHold -t SPY -s 2010
```

### Syntax:

```
backtest.run <strategy> -t <tickers list> ...
```


## Tools

```
python -m tools.download_prices -t SPY
```

| Tool            | Description                                                                                                            |
| --------------- | ---------------------------------------------------------------------------------------------------------------------- |
| download_info   | Download fundamental data                                                                                              |
| download_prices | Download price history for specified tickers. If no tickers given, defaults to download all tickers in SP500           |
| update_prices   | Updates newest price data and appends to the end of the downloaded file (Use this once you've already downloaded data) |
| plot            | Plot price for specified tickers                                                                                       |
| validate_data   | Cleans up and validates price data                                                                                     |
| stats           | Get statistical data of ticker                                                                                         |
| etc.            | You can follow this format and try out the other tools as well. They can all be imported too.                          |

## Current Implemented Strategies

-   Buy and Hold (`BuyAndHold.py`)
-   Simple Moving Average Cross-Over (`CrossOver.py`)
-   Leveraged ETF Pairs (`LeveragedEtfPair.py`)
-   Pair Switching (`PairSwitching.py`)
-   Mean reversion (`MeanReversion.py`)

### Notes:

#### Pair Switching

This strategy has been successful for the ETF pairs MDY and TLT.

Backtest results:

##### 2003 - 2013

| Method        | Value   | SPY     |
| ------------- | ------- | ------- |
| Total Returns | 525.71% | 89.86%  |
| Max Drawdown  | 16.28%  | 54.83%  |
| CAGR          | 20.15%  | 6.63%   |
| Sharpe        | 1.03988 | 0.24775 |
| Sortino       | 1.52483 | 0.34871 |

##### 2013 - 2018

| Method        | Value   | SPY     |
| ------------- | ------- | ------- |
| Total Returns | 55.83%  | 100.92% |
| Max Drawdown  | 9.76%   | 12.93%  |
| CAGR          | 9.29%   | 14.99%  |
| Sharpe        | 0.51831 | 0.95824 |
| Sortino       | 0.72603 | 1.35337 |

##### 2018 - YTD (09/04/2019)

| Method        | Value   | SPY     |
| ------------- | ------- | ------- |
| Total Returns | 14.64%  | 12.29%  |
| Max Drawdown  | 12.05%  | 19.15%  |
| CAGR          | 8.50%   | 7.19%   |
| Sharpe        | 0.43412 | 0.30127 |
| Sortino       | 0.58252 | 0.40374 |

#### MeanReversion

This strategy has been successful for the S&P 100 stocks.

##### Possible Enhancements:

[Quantopian: Enhancing short term mean reversion strategies](https://www.quantopian.com/posts/enhancing-short-term-mean-reversion-strategies-1)

-   Filter out large 1-day news-realted moves
    -   (Sort by 5d standard-deviation of returns)

Backtest results:

##### 2013 - 2018 (60d lookback, 5d rebalance)

| Method        | Value   | SPY     |
| ------------- | ------- | ------- |
| Total Returns | 133.90% | 96.88%  |
| Max Drawdown  | 18.10%  | 13.04%  |
| CAGR          | 17.54%  | 14.52%  |
| Sharpe        | 0.97543 | 0.93255 |
| Sortino       | 1.43594 | 1.32703 |

##### 2018 - YTD (12/16/2019) (60d lookback, 5d rebalance)

| Method        | Value   | OEF     |
| ------------- | ------- | ------- |
| Total Returns | 33.29%  | 22.65%  |
| Max Drawdown  | 20.20%  | 19.41%  |
| CAGR          | 13.88%  | 11.03%  |
| Sharpe        | 0.66737 | 0.53051 |
| Sortino       | 0.94469 | 0.71488 |

