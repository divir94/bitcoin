import pandas as pd
import time
import bitcoin.backtester.strategy as strat
import bitcoin.storage.api as st
import bitcoin.logs.logger as lc
from bitcoin.backtester.orders import OutstandingOrder
import random


logger = lc.config_logger('backtester', level='DEBUG', file_handler=False)


def compute_order_impact_on_available_balance(order):
    """
    Returns the amount by which this order decreases the available balance,
    and the currency which is affected
    """
    if order.side == 'buy':
        amount = order.price * order.size
        currency = order.base
    elif order.side == 'sell':
        amount = order.size
        currency = order.quote
    else:
        raise ValueError
    return currency, amount


class BackTester(object):
    def __init__(self, exchange, product_id, balance):
        self.exchange = exchange
        self.product_id = product_id
        self.balance = balance
        self.outstanding_orders = {}
        self.trades = {}

    @staticmethod
    def get_fills(msg, orders, order_book):
        # fills is dict[order id, fill qty]
        assert len(orders) <= 2, 'More than a single buy/sell order is not currently supported'
        fills = {}
        if not msg['type'] == 'match':
            return fills

        # the message side field indicates the maker order side
        maker_side = msg['side']
        match_price = float(msg['price'])
        match_size = float(msg['size'])
        #TODO Lol we need the time when the maker order was put on the book.
        # Not the time when this match happened
        maker_time_string = None

        for _, order in orders.iteritems():
            # our order is competitive if it has a better price than the match price
            competitive_price = order.price > match_price if order.side == 'buy' else order.price < match_price
            # match at same price if our order came before
            early_at_same_price = (order.price == match_price) and (order.time_string < maker_time_string)
            # order has to be same side as maker i.e. opposite side of taker
            same_side_as_maker = (maker_side == order.side)
            if same_side_as_maker and (competitive_price or early_at_same_price):
                fills[order.id] = min(match_size, order.size)
                logger.info('Filled ({}) of order: {}'.format(fills[order.id], order))
        return fills

    def handle_fills(self, fills):
        """
        Called after orders have been filled. Updates balance and outstanding orders.
        """
        for order_id, fill_qty in fills.iteritems():
            order = self.outstanding_orders[order_id]

            # update balance
            if order.side == 'buy':
                amount = fill_qty
                currency = order.quote
            elif order.side == 'sell':
                amount = order.price * fill_qty
                currency = order.base
            else:
                raise ValueError
            self.balance[currency] += amount

            # remove if filled, otherwise update order
            if self.outstanding_orders[order.id].size == fill_qty:
                del self.outstanding_orders[order.id]
            else:
                self.outstanding_orders[order.id] = order._replace(size=order.size - fill_qty)
        return

    def place_orders(self, orders):
        """
        Called to place orders. Updates balance and outstanding orders.
        """
        for order in orders:
            if type(order).__name__ == 'LimitOrder':
                outstanding_order = OutstandingOrder(
                    id=str(random.random()),
                    side=order.side,
                    quote=order.quote,
                    price=order.price,
                    base=order.base,
                    size=order.size,
                    #TODO(vidurj) pd.to_datetime('now') is not in gdax time format
                    time_string=str(pd.to_datetime('now')),
                )
                self.outstanding_orders[outstanding_order.id] = outstanding_order
                currency, amount = compute_order_impact_on_available_balance(outstanding_order)
                self.balance[currency] -= amount
                assert self.balance[currency] >= 0, \
                    'Insufficient balance {} for order {}'.format(self.balance[currency], order)

            elif type(order).__name__ == 'CancelOrder':
                cancelled_order = self.outstanding_orders[order.id]
                currency, amount = compute_order_impact_on_available_balance(cancelled_order)
                self.balance[currency] += amount
                del cancelled_order
            else:
                raise ValueError('Invalid order type {}'.format(order.type))

        return

    def run(self, strategy, start, end=None):
        book = st.get_book(self.exchange, self.product_id, timestamp=start)
        msgs = st.get_messages_by_time(self.exchange, self.product_id, start=start, end=end)
        for msg in msgs:
            book.process_message(msg)
            fills = self.get_fills(msg, self.outstanding_orders)
            self.handle_fills(fills)
            new_orders = strategy.rebalance(msg=msg,
                                            book=book,
                                            outstanding_orders=self.outstanding_orders,
                                            balance=self.balance)
            self.place_orders(new_orders)
        return book


if __name__ == '__main__':
    exchange = 'GDAX'
    product_id = 'BTC-USD'
    start = pd.datetime(2017, 9, 26, 4, 31)
    end = pd.datetime(2017, 9, 26, 4, 41)
    strategy = strat.Strategy()

    back_tester = BackTester(exchange, product_id, {'USD': 100000, 'BTC': 1000})
    back_tester.run(strategy, start, end)
