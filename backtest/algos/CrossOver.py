import backtrader as bt
import itertools

from . import BaseStrategy as base


class CrossOver(base.Strategy):
    params = {
        'target_percent': 0.80
    }

    def __init__(self):
        base.Strategy.__init__(self)

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
        if self.order:
            # Skip if order is pending
            return
        
        for i, d in enumerate(self.datas):
            split_target = self.params.target_percent * self.weights[i]
            comminfo = self.broker.getcommissioninfo(d)
            price = d.close[0]

            if d.buysell > 0:
                closevalue = 0
                if self.getposition(d):
                    self.log('CLOSE SHORT(SELL) , %.2f' % price)
                    #self.close(data=d, tradeid=d.curtradeid)
                    closevalue = d.order.size * price

                self.log('BUY CREATE, {:.2f}'.format(price))
                d.curtradeid = next(d.tradeid)
                targetvalue = split_target * (self.broker.getcash()+closevalue) 
                
                size = comminfo.getsize(price, targetvalue)
                #d.order = self.buy(data=d, size=size, price=price)
                d.order_rejected = False

            if d.buysell < 0:
                closevalue = 0
                if self.getposition(d):
                    self.log('CLOSE LONG(BUY) , %.2f' % price)
                    #self.close(data=d, tradeid=d.curtradeid)
                    closevalue = d.order.size * price

                self.log('SELL CREATE , %.2f' % price)
                d.curtradeid = next(d.tradeid)
                targetvalue = -split_target * (self.broker.getcash()+closevalue) 
                size = comminfo.getsize(price, targetvalue)
                #d.order = self.sell(data=d, size=size, price=price)
                d.order_rejected = False

            '''
            if not self.getposition(d):
                if d.buysell > 0 or d.order_rejected:
                    # Buy the up crossover
                    self.log('BUY CREATE, {:.2f}'.format(self.data.close[0]))
                    d.order = self.order_target_percent(d, target=split_target)
                    d.order_rejected = False
                
            else:
                if d.buysell < 0:
                    # Sell the down crossover
                    self.log('SELL CREATE, {:.2f}'.format(self.data.close[0]))
                    d.order = self.close(data=d)
            '''

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enought cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY {}\tprice {:.2f}\tsize {:.2f}\tCost: {:.2f}\tComm: {:.2f}'.format(
                    order.data._name,
                    order.executed.price,
                    order.executed.size,
                    order.executed.value,
                    order.executed.comm))
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            if order.issell():
                self.log('SELL {}\tprice {:.2f}\tsize{:.2f}\t Cost: {:.2f}\tComm: {:.2f}'.format(
                    order.data._name,
                    order.executed.price,
                    order.executed.size,
                    order.executed.value,
                    order.executed.comm))

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            status_reason = {
                order.Canceled: 'Canceled',
                order.Margin: 'Margin Called',
                order.Rejected: 'Rejected'
            }
            self.log('Order {}: {} {}'.format(
                status_reason[order.status],
                'BUY' if order.isbuy() else 'SELL',
                order.data._name
            ))
            self.log('Cash: {:.2f}, Order: {:.2f}'.format(self.broker.get_cash(),
                                                          (order.price or 0) * (order.size or 0)))
            order.data.order_rejected = True

        # Write down: no pending order
        self.order = None
