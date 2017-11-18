import pandas as pd
import json
import cPickle as pickle

import bitcoin.storage.api as st
import bitcoin.logs.logger as lc
import bitcoin.params as params
import bitcoin.util as util

import bitcoin.backtester.util as butil
from bitcoin.backtester.orders import *


logger = lc.config_logger('backtester', level='DEBUG', file_handler=False)


class BackTester(object):
    def __init__(self, exchange, product_id, balance):
        self.exchange = exchange
        self.product_id = product_id
        self.init_balance = butil.Balance(balance)
        self.balance = butil.Balance(balance)
        self.exposure = 0
        self.current_orders = {}  # dict of side to order
        self.trades = pd.DataFrame()
        self.book = None
        self.quote = 'USD'
        self.base = 'BTC'
        self.view = 0

    @property
    def all_current_orders(self):
        current_orders = self.current_orders.values()
        current_orders = [x for x in current_orders if x]
        return current_orders

    def _get_place_order_balance(self, order):
        if order.side == OrderSide.BUY:
            amount = -order.price * order.size
            currency = self.quote
        elif order.side == OrderSide.SELL:
            amount = -order.size
            currency = self.base
        else:
            raise ValueError
        return amount, currency

    def _get_fill_order_balance(self, order, fill_qty):
        if order.side == OrderSide.BUY:
            amount = fill_qty
            currency = self.base
        elif order.side == OrderSide.SELL:
            amount = order.price * fill_qty
            currency = self.quote
        else:
            raise ValueError
        return amount, currency

    def get_fills(self, msg):
        """
        fills is dict[order, fill qty]
        """
        fills = {}
        if not msg['type'] == 'match':
            return fills

        # the message side field indicates the maker order side
        maker_side = msg['side']
        match_price = msg['price']
        match_size = msg['size']
        # if maker_time_string is None, it means that the maker order existed before we got a snapshot and
        # thus we don't have a timestamp. In this case, our order was after the maker order
        maker_time = self.book.order_to_time.get(msg['maker_order_id'])

        for side, order in self.current_orders.iteritems():
            if not order:
                continue

            # our order is competitive if it has a better price than the match price
            competitive_price = order.price > match_price if order.side == 'buy' else order.price < match_price
            # match at same price if our order came before. Note that order.time_string < None is False and implies
            # that maker order is created before we got the snapshot
            early_at_same_price = (order.price == match_price) and (order.order_time < maker_time)
            # order has to be same side as maker i.e. opposite side of taker
            same_side_as_maker = (maker_side == order.side)
            if same_side_as_maker and (competitive_price or early_at_same_price):
                fill_size = min(match_size, order.size)
                fills[order] = fill_size
                self._add_trade(order, msg, fill_size)
        return fills

    def _add_trade(self, order, msg, fill_size):
        trade = dict(order._asdict())
        # add current balance to trade log
        trade.update(self.balance)
        # add best bid/ask to trade log
        best_bid, best_ask = self.book.get_best_bid_ask()
        balance = self._liquidate_positions()
        trade.update({
            'trade_time': msg['time'],
            'best_bid': best_bid,
            'best_ask': best_ask,
            'fill_size': fill_size,
            'remaining_size': order.size - fill_size,
            'exposure': self.exposure,
            'view': self.view,
            'balance': balance,
        })

        num_rows = self.trades.shape[0]
        if fill_size > 1e-4:
            logger.info('Order filled {} at {}. balance: {}'.format(fill_size, self.book.time_str, balance))
        trade = pd.DataFrame([trade], index=[num_rows])
        self.trades = self.trades.append(trade)
        return

    def handle_fills(self, fills):
        """
        Called after orders have been filled. Updates balance and current orders.
        """
        for order, fill_qty in fills.iteritems():
            self.update_balance(order=order, action=OrderAction.FILL, fill_qty=fill_qty)
            self.exposure += fill_qty if order.side == OrderSide.BUY else -fill_qty

            # remove if filled, otherwise update order
            if order.size == fill_qty:
                self.current_orders[order.side] = None
            else:
                self.current_orders[order.side] = order._replace(size=order.size - fill_qty)
        return

    def update_balance(self, order, action, fill_qty=None):
        """
        Decreases the available balance after an order is placed.
        """
        if action == OrderAction.PLACE:
            amount, currency = self._get_place_order_balance(order)
        elif action == OrderAction.FILL:
            amount, currency = self._get_fill_order_balance(order, fill_qty)
        elif action == OrderAction.CANCEL:
            original_order = self._get_order(order.id)
            amount, currency = self._get_place_order_balance(original_order)
            amount *= -1
        else:
            raise ValueError
        self.balance[currency] += amount
        return

    def _get_order(self, order_id):
        for order in self.all_current_orders:
            if order.id == order_id:
                return order
        raise Exception('Did not find order {}'.format(order_id))

    def place_orders(self, orders):
        """
        Called to place orders. Updates balance and current orders.
        """
        for order in orders:
            order_type = type(order).__name__
            assert order_type in SUPPORTED_ORDER_TYPES

            if order_type == OrderType.LIMIT:
                self.update_balance(order=order, action=OrderAction.PLACE)
                current_order = CurrentOrder(id=order.id,
                                             side=order.side,
                                             price=order.price,
                                             size=order.size,
                                             order_time=self.book.time_str)
                self.current_orders[current_order.side] = current_order

            elif order_type == OrderType.CANCEL:
                self.update_balance(order=order, action=OrderAction.CANCEL)
                original_order = self._get_order(order.id)
                self.current_orders[original_order.side] = None
            else:
                raise ValueError
        return

    def _run_with_data(self, strategy, book, msgs):
        self.book = book
        logger.info('Backtest start: {}. USD: {}. BTC: {}'.format(self.book.time_str,
                                                                  self.balance['USD'],
                                                                  self.balance['BTC']))
        for msg in msgs:
            # update book
            msg = util.to_numeric(msg, params.MSG_NUMERIC_FIELD[self.exchange])
            self.book.process_message(msg)

            # get and handle fills
            fills = self.get_fills(msg=msg)
            self.handle_fills(fills=fills)

            # get and place new orders
            instructions, view = strategy.rebalance(msg=msg,
                                                    book=self.book,
                                                    current_orders=self.current_orders,
                                                    balance=self.balance,
                                                    exposure=self.exposure)
            self.view = view or self.view
            self.place_orders(instructions)

        cancel_all = [CancelOrder(order.id) for order in self.all_current_orders]
        self.place_orders(cancel_all)
        final_usd = self._liquidate_positions()
        if not self.trades.empty:
            self.trades = self.trades.set_index(['order_time', 'id', 'side'])
        logger.info('Backtest end: {}. Final USD: {}'.format(self.book.time_str, final_usd))

    def _liquidate_positions(self):
        balance = self.balance.copy()

        # get balance for current orders
        for order in self.all_current_orders:
            order_type = type(order).__name__
            if order_type == 'CurrentOrder':
                amount, currency = self._get_place_order_balance(order)
                amount *= -1
                balance[currency] += amount

        excess_coins = balance['BTC'] - self.init_balance['BTC']
        best_bid, best_ask = self.book.get_best_bid_ask()
        mid_price = (best_bid + best_ask) / 2
        coin_value = excess_coins * mid_price
        final_usd = balance['USD'] + coin_value
        return final_usd

    def run_with_saved_data(self, strategy, file_name):
        snapshot_df = pickle.load(open('{}.pickle'.format(file_name), 'rb'))
        msgs = json.load(open('{}.json'.format(file_name), 'rb'))['data']
        book = st.get_book_from_df(snapshot_df)
        self._run_with_data(strategy, book, msgs)

    def save_data(self, start, num_msgs, file_name):
        _, msgs, snapshot_df = self._get_data(start, num_msgs)
        msgs = {'data': list(msgs)}
        pickle.dump(snapshot_df, open('{}.pickle'.format(file_name), 'wb'))
        json.dump(msgs, open('{}.json'.format(file_name), 'wb'))

    def _get_data(self, start, num_msgs):
        snapshot_df = st.get_closest_snapshot(self.exchange, self.product_id, timestamp=start, sequence=None)
        book = st.get_book_from_df(snapshot_df)
        msgs = st.get_messages_by_sequence(self.exchange,
                                           self.product_id,
                                           start=book.sequence,
                                           end=book.sequence + num_msgs)
        return book, msgs, snapshot_df

    def run(self, strategy, start, num_msgs):
        book, msgs, _ = self._get_data(start, num_msgs)
        self._run_with_data(strategy, book, msgs)


def main():
    from bitcoin.strategies.mom import TSMomStrategy
    strategy = TSMomStrategy()

    root = util.get_project_root()
    file_name = '{}/data/test_data.pickle'.format(root)
    backtest = BackTester(exchange='GDAX', product_id='BTC-USD', balance={'USD': 100000, 'BTC': 1000})

    # backtest.save_data(start=pd.datetime(2017, 9, 26, 4, 31), num_msgs=100000, file_name=file_name)
    backtest.run_with_saved_data(strategy, file_name)
    return backtest


if __name__ == '__main__':
    main()
