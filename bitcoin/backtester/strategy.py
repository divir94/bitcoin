import bitcoin.util as util
from bitcoin.backtester.orders import LimitOrder


class Strategy(util.BaseObject):
    def __init__(self):
        self.num = 0

    def rebalance(self, msg, book, outstanding_orders, balance):
        if self.num == 0:
            order = LimitOrder(quote='BTC',
                               base='USD',
                               side='buy',
                               price=3910.05,
                               size=1)
            new_orders = [order]
        else:
            new_orders = []
        self.num += 1
        return new_orders
