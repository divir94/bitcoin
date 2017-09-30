import bitcoin.util as util
from bitcoin.backtester.orders import *


class Strategy(util.BaseObject):
    def __init__(self):
        pass

    @staticmethod
    def rebalance(msg, book, outstanding_orders, balance):
        best_bid = book.bids[-1].price
        best_ask = book.asks[0].price
        sides = {order.side for order in outstanding_orders.values()}
        new_orders = []

        if OrderSide.BUY not in sides:
            order = LimitOrder(quote='BTC',
                               base='USD',
                               side=OrderSide.BUY,
                               price=best_bid,
                               size=1)
            new_orders.append(order)

        if OrderSide.SELL not in sides:
            order = LimitOrder(quote='BTC',
                               base='USD',
                               side=OrderSide.SELL,
                               price=best_ask,
                               size=1)
            new_orders.append(order)

        return new_orders
