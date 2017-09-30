import bitcoin.util as util
from bitcoin.backtester.orders import *


class Strategy(util.BaseObject):
    def __init__(self):
        pass

    @staticmethod
    def rebalance(msg, book, outstanding_orders, balance):
        best_bid = book.bids[-1].price
        best_ask = book.asks[0].price
        orders = {order.side: order for order in outstanding_orders.values()}
        new_orders = []
        tolerance = 1
        size = 1

        side_dict = [
            {'side': OrderSide.BUY, 'currency': 'USD', 'price': best_bid, 'amount': best_bid * size},
            {'side': OrderSide.SELL, 'currency': 'BTC', 'price': best_ask, 'amount': size},
        ]

        for item in side_dict:
            side = item['side']
            currency = item['currency']
            price = item['price']
            amount = item['amount']

            if side in orders:
                outstanding_order = orders[side]
                # cancel if changed significantly
                if abs(outstanding_order.price - best_bid) > tolerance:
                    order = CancelOrder(outstanding_order.id)
                    new_orders.append(order)
            else:
                # place a new order
                if balance[currency] >= amount:
                    order = LimitOrder(quote='BTC',
                                       base='USD',
                                       side=side,
                                       price=price,
                                       size=size)
                    new_orders.append(order)

        return new_orders
