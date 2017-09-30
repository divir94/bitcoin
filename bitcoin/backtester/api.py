import pandas as pd
import uuid
from collections import namedtuple

import bitcoin.storage.api as st
import bitcoin.logs.logger as lc
import bitcoin.params as params
import bitcoin.util as util

import bitcoin.backtester.strategy as strat
import bitcoin.backtester.util as butil
from bitcoin.backtester.orders import OutstandingOrder


logger = lc.config_logger('backtester', level='DEBUG', file_handler=False)

SUPPORTED_ORDER_TYPES = ['LimitOrder', 'cancel']
SUPPORTED_ORDER_ACTIONS = ['fill', 'place', 'cancel']
SUPPORTED_ORDER_SIDES = ['buy', 'sell']

OrderType = namedtuple('OrderType', ['LIMIT', 'CANCEL'])(*SUPPORTED_ORDER_TYPES)
OrderAction = namedtuple('OrderAction', ['FILL', 'PLACE', 'CANCEL'])(*SUPPORTED_ORDER_ACTIONS)
OrderSide = namedtuple('OrderSide', ['BUY', 'SELL'])(*SUPPORTED_ORDER_SIDES)


class BackTester(object):
    def __init__(self, exchange, product_id, balance):
        self.exchange = exchange
        self.product_id = product_id
        self.balance = butil.Balance(balance)
        self.outstanding_orders = {}
        self.trades = {}
        self.book = None

    @staticmethod
    def _get_place_order_balance(order):
        if order.side == OrderSide.BUY:
            amount = -order.price * order.size
            currency = order.base
        elif order.side == OrderSide.SELL:
            amount = -order.size
            currency = order.quote
        else:
            raise ValueError
        return amount, currency

    @staticmethod
    def _get_fill_order_balance(order, fill_qty):
        if order.side == OrderSide.BUY:
            amount = fill_qty
            currency = order.quote
        elif order.side == OrderSide.SELL:
            amount = order.price * fill_qty
            currency = order.base
        else:
            raise ValueError
        return amount, currency

    def update_balance(self, order, action, fill_qty=None):
        """
        Decreases the available balance after an order is placed.
        """
        if action == OrderAction.PLACE:
            amount, currency = self._get_place_order_balance(order)
        elif action == OrderAction.FILL:
            amount, currency = self._get_fill_order_balance(order, fill_qty)
        elif action == OrderAction.CANCEL:
            amount, currency = self._get_place_order_balance(order)
            amount *= -1
        else:
            raise ValueError
        self.balance[currency] += amount
        return

    def get_fills(self, msg):
        """
        fills is dict[order id, fill qty]
        """
        assert len(self.outstanding_orders) <= 2, 'More than a single buy/sell order is not currently supported'
        fills = {}
        if not msg['type'] == 'match':
            return fills

        # the message side field indicates the maker order side
        maker_side = msg['side']
        match_price = msg['price']
        match_size = msg['size']
        # if maker_time_string is None, it means that the maker order existed before we got a snapshot and
        # thus we don't have a timestamp. In this case, our order was after the maker order
        maker_time_string = self.book.order_to_time.get(msg['maker_order_id'])

        for _, order in self.outstanding_orders.iteritems():
            # our order is competitive if it has a better price than the match price
            competitive_price = order.price > match_price if order.side == 'buy' else order.price < match_price
            # match at same price if our order came before. Note that order.time_string < None is False and implies
            # that maker order is created before we got the snapshot
            early_at_same_price = (order.price == match_price) and (order.time_string < maker_time_string)
            # order has to be same side as maker i.e. opposite side of taker
            same_side_as_maker = (maker_side == order.side)
            if same_side_as_maker and (competitive_price or early_at_same_price):
                fills[order.id] = min(match_size, order.size)
                logger.info('Filled size {} of order: {}'.format(fills[order.id], order))
        return fills

    def handle_fills(self, fills):
        """
        Called after orders have been filled. Updates balance and outstanding orders.
        """
        for order_id, fill_qty in fills.iteritems():
            order = self.outstanding_orders[order_id]
            self.update_balance(order=order, action=OrderAction.FILL, fill_qty=fill_qty)

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
            order_type = type(order).__name__
            assert order_type in SUPPORTED_ORDER_TYPES

            if order_type == OrderType.LIMIT:
                self.update_balance(order=order, action=OrderAction.PLACE)
                now = pd.datetime.now().strftime(params.DATE_FORMAT[self.exchange])
                outstanding_order = OutstandingOrder(id=uuid.uuid4(), side=order.side, quote=order.quote,
                                                     price=order.price, base=order.base, size=order.size,
                                                     time_string=now)
                self.outstanding_orders[outstanding_order.id] = outstanding_order

            elif order_type == OrderType.CANCEL:
                self.update_balance(order=order, action=OrderAction.CANCEL)
                del self.outstanding_orders[order.id]
            else:
                raise ValueError
        return

    def run(self, strategy, start, end=None):
        logger.info('Backtest start')
        self.book = st.get_book(self.exchange, self.product_id, timestamp=start)
        msgs = st.get_messages_by_time(self.exchange, self.product_id, start=start, end=end)

        for msg in msgs:
            # update book
            msg = util.to_decimal(msg, params.MSG_NUMERIC_FIELD[self.exchange])
            self.book.process_message(msg)

            # get and handle fills
            fills = self.get_fills(msg=msg)
            self.handle_fills(fills=fills)

            # get and place new orders
            orders = strategy.rebalance(msg=msg,
                                        book=self.book,
                                        outstanding_orders=self.outstanding_orders,
                                        balance=self.balance)
            self.place_orders(orders)

        logger.info('Backtest end')
        return


if __name__ == '__main__':
    exchange = 'GDAX'
    product_id = 'BTC-USD'
    start = pd.datetime(2017, 9, 26, 4, 31)
    end = pd.datetime(2017, 9, 26, 4, 41)
    strategy = strat.Strategy()

    backtest = BackTester(exchange, product_id, {'USD': 100000, 'BTC': 1000})
    backtest.run(strategy, start, end)
    print backtest.balance
    print backtest.outstanding_orders
