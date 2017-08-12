import json
import requests
import logging
from collections import deque
from threading import Thread
from copy import deepcopy

import bitcoin.logs.logger
import bitcoin.order_book as ob
import bitcoin.util as util
from bitcoin.websocket.core import WebSocket


GX_WS_URL = 'wss://ws-feed.gdax.com'
GX_CHANNEL = {'type': 'subscribe', 'product_ids': ['BTC-USD']}
GX_HTTP_URL = 'https://api.gdax.com/products/BTC-USD/book'
logger = logging.getLogger('gdax_websocket')
#logger.setLevel(logging.DEBUG)


def get_gdax_book():
    request = requests.get(GX_HTTP_URL, params={'level': 3})
    data = json.loads(request.content)
    return data


class GdaxOrderBook(WebSocket):
    def __init__(self, on_change=None):
        super(GdaxOrderBook, self).__init__(GX_WS_URL, GX_CHANNEL)
        self.exchange = 'GDAX'
        self.book = ob.OrderBook(-1)
        self.queue = deque()
        self.on_change = on_change

        self.restart = True  # load the order book
        self.syncing = False  # sync in process i.e. loading order book or applying messages
        self.check_freq = 10  # check every x seconds

    def _get_levels(self, side, book):
        book = book or self.book
        return book.bids if side == 'buy' else book.asks

    @property
    def _log_extra(self):
        return {'sequence': self.book.sequence}

    def reset_book(self):
        """get level 3 order book and apply pending messages from queue"""
        logger.info('='*30, extra=self._log_extra)
        logger.info('Loading book', extra=self._log_extra)
        self.syncing = True

        # get book
        data = get_gdax_book()
        self.book = ob.OrderBook(sequence=data['sequence'], bids=data['bids'], asks=data['asks'])
        logger.info('Got book', extra=self._log_extra)

        # apply queue
        self.apply_queue(self.book)
        self.queue = deque()
        self.syncing = False
        logger.info('=' * 30, extra=self._log_extra)

    def apply_queue(self, book, end=None):
        """apply queued messages to book till end sequence"""
        logger.info('Applying queued msgs', extra=self._log_extra)
        end = end or float('inf')

        while not self.restart and len(self.queue):
            msg = self.queue.popleft()
            sequence = msg['sequence']

            if sequence > end:
                self.queue.appendleft(msg)
                break

            result = self.process_message(msg, book)
            if result != -1:
                logger.debug('Processed queued msg', extra=self._log_extra)
        logger.info('Book ready', extra=self._log_extra)
        return

    def on_message(self, msg):
        msg = util.to_decimal(msg)
        sequence = msg['sequence']

        if self.restart:
            # reset order book and clear queue
            logger.info('Restarting sync', extra=self._log_extra)
            self.queue = deque()
            Thread(target=self.reset_book).start()
            self.restart = False
        elif self.syncing:
            # sync in process, queue msgs
            logger.debug('Queuing msg', extra=self._log_extra)
            self.queue.append(msg)
        else:
            logger.debug('Processing msg', extra=self._log_extra)
            self.process_message(msg)

    def process_message(self, msg, book=None):
        book = book or self.book
        sequence = msg['sequence']

        if sequence <= book.sequence:
            # ignore older messages (e.g. before order book initialization from getProductOrderBook)
            logger.debug('Ignoring older msg', extra=self._log_extra)
            return -1
        elif sequence != book.sequence + 1:
            # resync
            logger.info('Out of sync: message({})'.format(sequence), extra=self._log_extra)
            self.restart = True
            return -1

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
            logger.error('Error message: {}'.format(msg), extra=self._log_extra)

        book.sequence = int(msg['sequence'])

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
        logger.info('=' * 30, extra=self._log_extra)
        self.syncing = True

        # save current book
        current_book = deepcopy(self.book)
        logger.info('Checking book start', extra={'sequence': current_book.sequence})

        # get expected book
        data = get_gdax_book()
        expected_book = ob.OrderBook(sequence=data['sequence'], bids=data['bids'], asks=data['asks'])
        logger.info('Expected book sequence', extra={'sequence': expected_book.sequence})

        # apply queue to current book
        self.apply_queue(current_book, end=expected_book.sequence)

        # compare diffs
        num_diff = ob.compare_books(current_book, expected_book)
        msg = 'Book differences: {}'.format(num_diff)
        log_extra = {'sequence': current_book.sequence}
        logger.error(msg, extra=log_extra) if num_diff > 0 else logger.info(msg, extra=log_extra)

        # reset book
        self.apply_queue(expected_book)
        self.book = expected_book
        self.queue = deque()
        self.syncing = False
        logger.info('Checking book end', extra=self._log_extra)
        logger.info('=' * 30, extra=self._log_extra)
        return


if __name__ == '__main__':
    ws = GdaxOrderBook()
    ws.start()
