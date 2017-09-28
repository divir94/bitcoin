import pandas as pd
import uuid

import bitcoin.storage.api as st
import bitcoin.logs.logger as lc
import bitcoin.params as params

import bitcoin.backtester.strategy as strat
import bitcoin.backtester.util as butil
from bitcoin.backtester.orders import OutstandingOrder


logger = lc.config_logger('backtester', level='DEBUG', file_handler=False)
SUPPORTED_ORDER_TYPES = ['LimitOrder', 'CancelOrder']
SUPPORTED_ORDER_ACTIONS = ['fill', 'place']
SUPPORTED_ORDER_SIDES = ['buy', 'sell']


def get_new_balance(balance, order, action, fill_qty=None):
    """
    Decreases the available balance after an order is placed.
    """
    assert order.side in SUPPORTED_ORDER_SIDES
    assert action in SUPPORTED_ORDER_ACTIONS
    new_balance = balance.copy()

    if action == 'place':
        if order.side == 'buy':
            amount = -order.price * order.size
            currency = order.base
            assert balance[currency] >= amount, 'Insufficient balance {} for order {}'.format(balance[currency], amount)
        else:
            amount = order.size
            currency = order.quote
    else:
        # filled orders always increase balance
        if order.side == 'buy':
            amount = fill_qty
            currency = order.quote
        else:
            amount = order.price * fill_qty
            currency = order.base

    new_balance[currency] += amount
    return new_balance


class BackTester(object):
    def __init__(self, exchange, product_id, balance):
        self.exchange = exchange
        self.product_id = product_id
        self.balance = butil.Balance(balance)
        self.outstanding_orders = {}
        self.trades = {}

    @staticmethod
    def get_fills(msg, orders, book):
        """
        fills is dict[order id, fill qty]
        """
        assert len(orders) <= 2, 'More than a single buy/sell order is not currently supported'
        fills = {}
        if not msg['type'] == 'match':
            return fills

        # the message side field indicates the maker order side
        maker_side = msg['side']
        match_price = float(msg['price'])
        match_size = float(msg['size'])
        # TODO(divir): use the time when the maker order was put on the book
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
                logger.info('Filled size {} of order: {}'.format(fills[order.id], order))
        return fills

    @staticmethod
    def handle_fills(fills, orders, balance):
        """
        Called after orders have been filled. Updates balance and outstanding orders.
        """
        new_orders = orders.copy()
        new_balance = balance

        for order_id, fill_qty in fills.iteritems():
            order = orders[order_id]
            new_balance = get_new_balance(balance, order, action='fill', fill_qty=fill_qty)

            # remove if filled, otherwise update order
            if new_orders[order.id].size == fill_qty:
                del new_orders[order.id]
            else:
                new_orders[order.id] = order._replace(size=order.size - fill_qty)
        return new_balance, new_orders

    @staticmethod
    def place_orders(orders, balance, exchange):
        """
        Called to place orders. Updates balance and outstanding orders.
        """
        new_orders = orders.copy()
        new_balance = balance

        for order in orders:
            order_type = type(order).__name__
            assert order_type in SUPPORTED_ORDER_TYPES

            if order_type == 'LimitOrder':
                now = pd.datetime.now().strftime(params.DATE_FORMAT[exchange])
                new_balance = get_new_balance(balance, order, action='place')
                outstanding_order = OutstandingOrder(
                    id=uuid.uuid4(),
                    side=order.side,
                    quote=order.quote,
                    price=order.price,
                    base=order.base,
                    size=order.size,
                    time_string=now,
                )
                new_orders[outstanding_order.id] = outstanding_order

            elif order_type == 'CancelOrder':
                new_balance = get_new_balance(balance, order, action='cancel')
                del new_orders[order.id]
        return new_balance, new_orders

    def run(self, strategy, start, end=None):
        book = st.get_book(self.exchange, self.product_id, timestamp=start)
        msgs = st.get_messages_by_time(self.exchange, self.product_id, start=start, end=end)

        for msg in msgs:
            book.process_message(msg)
            orders, balance = self.outstanding_orders, self.balance

            # get and handle fills
            fills = self.get_fills(msg=msg,
                                   orders=orders,
                                   book=book)
            new_balance, new_orders = self.handle_fills(fills=fills,
                                                        orders=orders,
                                                        balance=balance)

            # get and place new orders
            new_orders = strategy.rebalance(msg=msg,
                                            book=book,
                                            outstanding_orders=new_orders,
                                            balance=new_balance)
            new_balance, new_orders = self.place_orders(orders=new_orders,
                                                        balance=new_balance,
                                                        exchange=self.exchange)

            # update
            self.balance = new_balance
            self.outstanding_orders = new_orders
        return book


if __name__ == '__main__':
    exchange = 'GDAX'
    product_id = 'BTC-USD'
    start = pd.datetime(2017, 9, 26, 4, 31)
    end = pd.datetime(2017, 9, 26, 4, 41)
    strategy = strat.Strategy()

    back_tester = BackTester(exchange, product_id, {'USD': 100000, 'BTC': 1000})
    back_tester.run(strategy, start, end)
