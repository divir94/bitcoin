import pandas as pd

import bitcoin.util as util
from bitcoin.backtester.orders import Order


class Strategy(util.BaseObject):
    def __init__(self):
        self.num = 0

    def rebalance(self, msg, book, outstanding_orders, balance):
        time = pd.to_datetime(msg['time'])
        if self.num == 0:
            order = Order(order_id=123,
                          quote='BTC',
                          base='USD',
                          side='buy',
                          price=3910.05,
                          size=1,
                          time=time)
            new_orders = [order]
        else:
            new_orders = []
        self.num += 1
        return new_orders
