import json
import time
import requests
import logging
from Queue import Queue
from threading import Thread
from multiprocessing import Pool
from copy import deepcopy

import bitcoin.order_book as ob
import bitcoin.util as util
from bitcoin.websocket.core import WebSocket


GX_WS_URL = 'wss://ws-feed.gdax.com'
GX_HTTP_URL = 'https://api.gdax.com/products/BTC-USD/book'
POOL = Pool()


def get_gdax_book():
    request = requests.get(GX_HTTP_URL, params={'level': 3})
    data = json.loads(request.content)
    return data


class GdaxOrderBook(WebSocket):
    def __init__(self, on_change=None):
        channel = {'type': 'subscribe', 'product_ids': ['BTC-USD']}
        super(GdaxOrderBook, self).__init__(GX_WS_URL, channel)
        self.exchange = 'GDAX'
        self.book = ob.OrderBook(-1)
        self.queue = Queue()
        self.restart = True
        self.syncing = False
        self.on_change = on_change

        self.checking = False
        self.check_queue = Queue()
        self.logger.setLevel(logging.INFO)

    def reset_book(self):
        """get level 3 order book and apply pending messages from queue"""
        self.logger.debug('Loading book')
        self.syncing = True

        # get book
        data = get_gdax_book()
        self.book = ob.OrderBook(sequence=data['sequence'], bids=data['bids'], asks=data['asks'])
        self.logger.debug('Got book: {}'.format(self.book.sequence))

        # apply queue
        self.apply_queue(self.queue, self.book)
        self.queue = Queue()
        self.syncing = False

    def apply_queue(self, queue, book, end=None):
        """apply queued messages to book till end sequence"""
        end = end or float('inf')

        while not self.restart and not queue.empty():
            msg = queue.get()
            sequence = msg['sequence']
            if sequence >= end:
                break
            self.logger.debug('Applying queued msg: {}'.format(sequence))
            self.process_message(msg, book)
        self.logger.info('Book ready: {}'.format(book.sequence))
        return

    def on_message(self, ws, message):
        msg = self.parse_message(message)
        sequence = msg['sequence']
        self.logger.debug('Msg receieved: {}'.format(msg['sequence']))

        if self.restart:
            # reset order book and clear queue
            self.logger.info('Restarting sync')
            self.queue = Queue()
            Thread(target=self.reset_book).start()
            self.restart = False
        elif self.checking:
            # check queue
            self.logger.debug('Queuing msg for checking: {}'.format(sequence))
            self.check_queue.put(msg)
        elif self.syncing:
            # sync in process, queue msgs
            self.logger.debug('Queuing msg: {}'.format(sequence))
            self.queue.put(msg)
        else:
            self.process_message(msg)

    def process_message(self, msg, book=None):
        book = book or self.book
        sequence = msg['sequence']

        if sequence <= book.sequence:
            # ignore older messages (e.g. before order book initialization from getProductOrderBook)
            self.logger.debug('Ignoring older msg: {}'.format(sequence))
            return
        elif sequence != book.sequence + 1:
            # resync
            self.logger.info('Out of synch: book({}), message({})'.format(book.sequence, sequence))
            self.restart = True
            return

        _type = msg['type']
        if _type == 'open':
            self.open_order(msg, book)

        elif _type == 'done':
            self.done_order(msg, book)

        elif _type == 'match':
            self.match_order(msg, book)

        elif _type == 'change':
            self.change_order(msg, book)
        elif _type == 'received' or _type == 'heartbeat':
            pass
        else:
            assert _type == 'error'
            self.logger.error('Error message: {}'.format(msg))

        book.sequence = msg['sequence']
        self.logger.debug('Book: {}'.format(book.sequence))

    def parse_message(self, msg):
        """convert fields to number"""
        msg = json.loads(msg)
        msg = {k: util.to_decimal(v) for k, v in msg.iteritems()}
        return msg

    def _get_levels(self, side, book):
        book = book or self.book
        return book.bids if side == 'buy' else book.asks

    def open_order(self, msg, book):
        """
        The order is now open on the order book. This message will only be sent for orders which are not fully filled
        immediately. remaining_size will indicate how much of the order is unfilled and going on the book.

        Parameters
        ----------
        msg: dict
         e.g.
         {
            "type": "open",
            "time": "2014-11-07T08:19:27.028459Z",
            "product_id": "BTC-USD",
            "sequence": 10,
            "order_id": "d50ec984-77a8-460a-b958-66f114b0de9b",
            "price": "200.2",
            "remaining_size": "1.00",
            "side": "sell"
        }
        book: ob.OrderBook
        """
        side = msg['side']
        price = msg['price']
        size = msg['remaining_size']
        order_id = msg['order_id']
        book.add(side, price, size, order_id)

    def done_order(self, msg, book):
        """
        The order is no longer on the order book. Sent for all orders for which there was a received message.
        This message can result from an order being canceled or filled. There will be no more messages for this
        order_id after a done message. remaining_size indicates how much of the order went unfilled; this will be 0
        for filled orders.

        market orders will not have a remaining_size or price field as they are never on the open order book at a given
        price.

        Notes
        -----
        A done message will be sent for received orders which are fully filled or canceled due to self-trade prevention.
        There will be no open message for such orders. done messages for orders which are not on the book should be
        ignored when maintaining a real-time order book.

        Parameters
        ----------
        msg: dict
        e.g.
        {
            "type": "done",
            "time": "2014-11-07T08:19:27.028459Z",
            "product_id": "BTC-USD",
            "sequence": 10,
            "price": "200.2",
            "order_id": "d50ec984-77a8-460a-b958-66f114b0de9b",
            "reason": "filled", // canceled
            "side": "sell",
            "remaining_size": "0.2"
        }
        book: ob.OrderBook
        """
        price = msg.get('price')
        order_id = msg['order_id']

        if order_id not in book.orders:
            return

        result = book.update(order_id, 0)
        assert price == result[0]

    def match_order(self, msg, book):
        """
        A trade occurred between two orders. The aggressor or taker order is the one executing immediately after being
        received and the maker order is a resting order on the book. The side field indicates the maker order side.
        If the side is sell this indicates the maker was a sell order and the match is considered an up-tick.
        A buy side match is a down-tick.

        Parameters
        ----------
        msg: dict
        e.g.
        {
            "type": "match",
            "trade_id": 10,
            "sequence": 50,
            "maker_order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
            "taker_order_id": "132fb6ae-456b-4654-b4e0-d681ac05cea1",
            "time": "2014-11-07T08:19:27.028459Z",
            "product_id": "BTC-USD",
            "size": "5.23512",
            "price": "400.23",
            "side": "sell"
        }
        book: ob.OrderBook
        """
        price = msg['price']
        trade_size = msg['size']
        order_id = msg['maker_order_id']

        # get original order size
        old_price, old_size, order_id = book.get(order_id)
        assert price == old_price
        assert trade_size <= old_size

        # update order to new size
        new_size = old_size - trade_size
        book.update(order_id, new_size)

    def change_order(self, msg, book):
        """
        An order has changed. This is the result of self-trade prevention adjusting the order size or available funds.
        Orders can only decrease in size or funds. change messages are sent anytime an order changes in size;
        this includes resting orders (open) as well as received but not yet open. change messages are also sent when a
        new market order goes through self trade prevention and the funds for the market order have changed.

        Notes
        -----
        change messages for received but not yet open orders can be ignored when building a real-time order book.
        The side field of a change message and price can be used as indicators for whether the change message is
        relevant if building from a level 2 book.

        Any change message where the price is null indicates that the change message is for a market order.
        Change messages for limit orders will always have a price specified.

        Parameters
        ----------
        msg: dict
        e.g.
        {
            "type": "change",
            "time": "2014-11-07T08:19:27.028459Z",
            "sequence": 80,
            "order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
            "product_id": "BTC-USD",
            "new_size": "5.23512",
            "old_size": "12.234412",
            "price": "400.23",
            "side": "sell"
        }
        book: ob.OrderBook
        """
        price = msg.get('price')
        new_size = msg['new_size']
        order_id = msg['order_id']

        if order_id not in book.orders or not price:
            return

        result = book.update(order_id, new_size)
        assert price == result[0]

    def check_book(self):
        self.logger.info('Checking book is in sync')
        self.checking = True

        # same current book
        current_book = deepcopy(self.book)
        self.logger.info('Current book: {}'.format(current_book.sequence))

        # expected book
        data = get_gdax_book()
        expected_book = ob.OrderBook(sequence=data['sequence'], bids=data['bids'], asks=data['asks'])

        # apply queue to current book
        self.apply_queue(self.check_queue, current_book, end=expected_book.sequence)

        # compare diffs
        num_diff = ob.compare_books(current_book, expected_book)
        self.logger.info('Book differences: {}'.format(num_diff))

        # reset book
        self.apply_queue(self.check_queue, expected_book)
        self.book = expected_book
        self.logger.info('Current book after checking: {}'.format(self.book.sequence))
        self.check_queue = Queue()
        self.checking = False
        return


if __name__ == '__main__':
    ws = GdaxOrderBook()
    ws.run()

    time.sleep(5)
    ws.check_book()
    time.sleep(10)
    ws.close()
