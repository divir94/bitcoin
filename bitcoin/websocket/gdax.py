import logging
from collections import deque
from threading import Thread
from copy import deepcopy


import bitcoin.logs.logger as lc
import bitcoin.order_book as ob
import bitcoin.util as util
import bitcoin.gdax.public_client as gdax
import bitcoin.gdax.params as params
from bitcoin.websocket.core import WebSocket


logger = lc.config_logger('gdax_websocket')
# logger.setLevel(logging.DEBUG)


class GdaxOrderBook(WebSocket):
    def __init__(self, on_change=None):
        super(GdaxOrderBook, self).__init__(params.WS_URL, params.CHANNEL)
        self.exchange = 'GDAX'
        self.book = ob.OrderBook(-1)
        self.queue = deque()
        self.on_change = on_change
        self.gdax_client = gdax.PublicClient()
        self.product_id = 'BTC-USD'

        self.restart = True  # load the order book
        self.syncing = False  # sync in process i.e. loading order book or applying messages
        self.check_freq = 3600  # check every x seconds
        self.sleep_time = 2

    def _get_levels(self, side, book):
        book = book or self.book
        return book.bids if side == 'buy' else book.asks

    def reset_book(self):
        """get level 3 order book and apply pending messages from queue"""
        self.syncing = True
        logger.info('='*30)
        logger.info('Loading book', extra={'sequence': self.book.sequence})

        # get book
        data = self.gdax_client.get_product_order_book(self.product_id, level=3)
        self.book = ob.OrderBook(sequence=data['sequence'], bids=data['bids'], asks=data['asks'])
        logger.info('Got book: {}'.format(self.book.sequence))

        # apply queue
        self.apply_queue(self.book)
        self.queue = deque()
        self.syncing = False
        logger.info('=' * 30)

    def apply_queue(self, book, end=None):
        """apply queued messages to book till end sequence"""
        logger.info('Applying queued msgs: {}'.format(book.sequence))
        end = end or float('inf')

        while not self.restart and len(self.queue):
            msg = self.queue.popleft()
            sequence = msg['sequence']

            if sequence > end:
                self.queue.appendleft(msg)
                break

            result = self.process_message(msg, book)
            if result != -1:
                logger.debug('Processed queued msg: {}'.format(sequence))
        logger.info('Book ready: {}'.format(book.sequence))
        return

    def on_message(self, msg):
        msg = util.to_decimal(msg)
        sequence = msg['sequence']

        if self.restart:
            # reset order book and clear queue
            logger.info('Restarting sync', extra={'sequence': self.book.sequence})
            self.queue = deque()
            Thread(target=self.reset_book).start()
            self.restart = False
        elif self.syncing:
            # sync in process, queue msgs
            logger.debug('Queuing msg: {}'.format(sequence))
            self.queue.append(msg)
        else:
            logger.debug('Processing msg: {}'.format(sequence))
            self.process_message(msg)

    def process_message(self, msg, book=None):
        book = book or self.book
        sequence = msg['sequence']

        if sequence <= book.sequence:
            # ignore older messages (e.g. before order book initialization from getProductOrderBook)
            logger.debug('Ignoring older msg: {}'.format(sequence))
            return -1
        elif sequence != book.sequence + 1:
            # resync
            logger.warning('Out of sync: book({}), message({})'.format(book.sequence, sequence))
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
            logger.error('Error message: {}'.format(msg))

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
        new_size = msg.get('new_size')
        order_id = msg['order_id']

        if order_id not in book.orders or not price or not new_size:
            return

        result = book.update(order_id, new_size)
        assert price == result[0]

    def check_book(self):
        self.syncing = True
        logger.info('^' * 30)

        # save current book
        current_book = deepcopy(self.book)
        logger.info('Checking book start: {}'.format(current_book.sequence))
        logger.setLevel(logging.DEBUG)

        # get expected book
        data = self.gdax_client.get_product_order_book(self.product_id, level=3)
        expected_book = ob.OrderBook(sequence=data['sequence'], bids=data['bids'], asks=data['asks'])
        logger.info('Expected book: {}'.format(expected_book.sequence))

        # apply queue to current book
        self.apply_queue(current_book, end=expected_book.sequence)

        # compare diffs
        num_diff = ob.compare_books(current_book, expected_book)
        msg = 'Book differences: {}'.format(num_diff)
        logger.error(msg) if num_diff > 0 else logger.info(msg)

        # reset book
        self.apply_queue(expected_book)
        self.book = expected_book
        self.queue = deque()
        self.syncing = False
        logger.setLevel(logging.INFO)
        logger.info('Checking book end: {}'.format(self.book.sequence))
        logger.info('^' * 30)
        return


if __name__ == '__main__':
    ws = GdaxOrderBook()
    ws.start()
