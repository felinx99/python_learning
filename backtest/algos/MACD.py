import backtrader as bt
import itertools
import datetime
from . import BaseStrategy as base


class MACD(base.Strategy):
    params = {
        # Standard MACD Parameters
        'macd1': 12,
        'macd2': 26,
        'macdsig': 9,
        'atrperiod': 14,  # ATR Period (standard)
        'atrdist': 3.0,   # ATR distance for stop price
        'smaperiod': 30,  # SMA Period (pretty standard)
        'dirperiod': 10,  # Lookback period to consider SMA trend direction
    }

    def __init__(self):
        base.Strategy.__init__(self)
        self.target_percent = 0.90
        self.weights = [float(1/len(self.datas)) for d in self.datas]        

        # Define Indicators
        for i, d in enumerate(self.datas):
            d.order = None
            d.order_rejected = False

            # To alternate amongst different tradeids
            d.tradeid = itertools.cycle([0, 1, 2])
            d.macd = bt.indicators.MACD(d,
                                       period_me1=self.p.macd1,
                                       period_me2=self.p.macd2,
                                       period_signal=self.p.macdsig)

            # Cross of macd.macd and macd.signal
            d.mcross = bt.indicators.CrossOver(d.macd.macd, d.macd.signal)

            # To set the stop price
            d.atr = bt.indicators.ATR(d, period=self.p.atrperiod)

            # Control market trend
            d.sma = bt.indicators.SMA(d, period=self.p.smaperiod)
            d.smadir = d.sma - d.sma(-self.p.dirperiod)
     
    def next(self):

        for i, d in enumerate(self.datas):
            if d.order or not d.islive():
                # Skip if order is pending
                return
            split_target = self.target_percent * self.weights[i]
            comminfo = self.broker.getcommissioninfo(d)
            price = d.close[0]
            possize = self.getposition(d).size if self.getposition(d) else 0
            d.curtradeid = next(d.tradeid)
            if possize == 0:  # not in the market
                if d.mcross[0] > 0.0 and d.smadir < 0.0:
                    targetvalue = split_target * self.broker.getcash()                 
                    size = comminfo.getsize(price, targetvalue)
                    #self.log(f'Buy-{d._name} size:{size}')
                    self.buy(data=d,
                             size=size,
                             price=price,
                             #takeProfitPrice=0,
                             #stopLossPrice=stopLossPrice,
                             orderType='LMT',
                             tif='GTC',
                             tradeid=d.curtradeid)
                    pdist = d.atr[0] * self.p.atrdist
                    d.pstop = d.close[0] - pdist

            else:  # in the market
                pclose = d.close[0]
                pstop = d.pstop

                if pclose < pstop: # stop met - get out
                    #self.log(f'Close-{d._name} size:{abs(possize)}')
                    self.close(data=d,
                               size=abs(possize),
                               orderType='MKT',
                               tif='GTC',
                               tradeid=d.curtradeid)  
                else:
                    pdist = d.atr[0] * self.p.atrdist
                    # Update only if greater than
                    d.pstop = max(pstop, pclose - pdist)
