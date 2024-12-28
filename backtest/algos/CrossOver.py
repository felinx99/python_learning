import backtrader as bt
import itertools

from . import BaseStrategy as base


class CrossOver(base.Strategy):
    def __init__(self):
        base.Strategy.__init__(self)

        self.target_percent = 0.90
        self.weights = [float(1/len(self.datas)) for d in self.datas]

        # Define Indicators
        for i, d in enumerate(self.datas):
            d.order = None
            d.order_rejected = False
            d.ema5 = bt.indicators.EMA(d, period=5)
            d.ema20 = bt.indicators.EMA(d, period=20)
            d.buysell = bt.indicators.CrossOver(d.ema5, d.ema20, plot=True)
            # To alternate amongst different tradeids
            d.tradeid = itertools.cycle([0, 1, 2])

    def next(self):       
        for i, d in enumerate(self.datas):
            if d.order:
                # Skip if order is pending
                return
            
            split_target = self.target_percent * self.weights[i]
            comminfo = self.broker.getcommissioninfo(d)
            price = d.close[0]
            possize = self.getposition(d).size
            
            if d.buysell > 0:
                takeProfitPrice = round(price*1.5, 4)
                stopLossPrice = round(price*0.85,4)
                if possize:
                    self.close(data=d,
                               size=possize,
                               lmtPrice=price,
                               orderType='LMT',
                               tif='DAY',
                               tradeid=d.curtradeid)
                    break
                
                d.curtradeid = next(d.tradeid)
                targetvalue = split_target * self.broker.getcash()                 
                size = comminfo.getsize(price, targetvalue)
                #self.buy(data=d,
                #         size=size,
                #         lmtPrice=price,
                #         takeProfitPrice=takeProfitPrice,
                #         stopLossPrice=stopLossPrice,
                #         orderType='BKT',
                #         tif='GTC',
                #         tradeid=d.curtradeid)
                self.buy_bracket(data=d,
                         size=size,
                         price=price,
                         limitprice=takeProfitPrice,
                         stopprice=stopLossPrice,
                         tradeid=d.curtradeid)
  
            if d.buysell < 0:
                takeProfitPrice = round(price*0.85,4)
                stopLossPrice = round(price*1.5,4)
                if possize:
                    
                    self.close(data=d,
                               size=possize,
                               lmtPrice=price,
                               orderType='LMT',
                               tif='DAY',
                               tradeid=d.curtradeid,
                               pseudosubmit=True)
                    break

                
                d.curtradeid = next(d.tradeid)
                targetvalue = -split_target * self.broker.getcash()
                size = comminfo.getsize(price, targetvalue)

                #self.sell(data=d,
                #          size=size,
                #          lmtPrice=price,
                #          takeProfitPrice=takeProfitPrice,
                #          stopLossPrice=stopLossPrice,
                #          orderType='BKT',
                #          tif='GTC',
                #          tradeid=d.curtradeid)
                self.sell_bracket(data=d,
                         size=size,
                         price=price,
                         limitprice=takeProfitPrice,
                         stopprice=stopLossPrice,
                         tradeid=d.curtradeid)

