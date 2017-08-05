import json
import time
import requests
from Queue import Queue
from threading import Thread

import bitcoin.order_book as ob
import bitcoin.util as util
from bitcoin.websocket.core import WebSocket


GX_WS_URL = 'wss://ws-feed.gdax.com'
GX_HTTP_URL = 'https://api.gdax.com/products/BTC-USD/book'


class GdaxOrderBook(WebSocket):
    def __init__(self, on_change=None):
        channel = {'type': 'subscribe', 'product_ids': ['BTC-USD']}
        super(GdaxOrderBook, self).__init__(GX_WS_URL, channel)
        self.exchange = 'GDAX'
        self.book = ob.OrderBook(seq=-1)
        self.queue = Queue()
        self.on_change = on_change

        self.logger.debug('Initializing {}'.format(self.book.seq))

    def load_book(self):
        self.logger.debug('Loading book')
        level = 3
        request = requests.get(GX_HTTP_URL, params={'level': level})
        data = json.loads(request.content)
        book = ob.get_order_book(data, level, seq='sequence')
        self.logger.debug('Got book: {}'.format(book.seq))
        return book

    def reset_book(self):
        """
        Get level 3 order book and apply pending messages from queue
        """
        self.logger.info('Resetting book')
        self.book = self.load_book()

        while not self.queue.empty():
            msg = self.queue.get()
            self.logger.info('Applying queued msg: {}'.format(msg['sequence']))
            result = self.process_message(msg)
            if result == -1:
                # skipped message in queue
                return

    def on_message(self, ws, message):
        msg = self.parse_message(message)
        sequence = msg['sequence']
        self.logger.debug('Msg receieved: {}'.format(msg['sequence']))

        if not self.queue.empty():
            # sync in process
            self.queue.put(msg)
            self.logger.info('Queuing msg: {}'.format(sequence))
        elif self.book.seq == -1:
            # start first time sync
            self.logger.info('Starting first sync')
            self.queue = Queue()
            self.queue.put(msg)
            Thread(target=self.reset_book).start()
        else:
            self.process_message(msg)

    def process_message(self, msg):
        sequence = msg['sequence']

        if sequence <= self.book.seq:
            # ignore older messages (e.g. before order book initialization from getProductOrderBook)
            self.logger.info('Ignoring older msg: {}'.format(sequence))
            return
        elif sequence != self.book.seq + 1:
            # resync
            self.logger.info('Out of synch: {}'.format(sequence))
            self.book.seq = -1
            return -1

        _type = msg['type']
        if _type == 'open':
            self.open_order(msg)

        elif _type == 'done':
            self.done_order(msg)

        elif _type == 'match':
            self.match_order(msg)

        # price is null for market orders in a change msg
        elif type == 'change' and msg.get('price') and msg.get('new_size'):
            self.change_order(msg)

        self.book.seq = msg['sequence']
        self.logger.debug('Book: {}'.format(self.book.seq))

    def parse_message(self, msg):
        """
        Convert fields to float
        """
        msg = json.loads(msg)
        msg = {k: util.to_float(v) for k, v in msg.iteritems()}
        return msg

    def _get_levels(self, side):
        return self.book.bids if side == 'buy' else self.book.asks

    def open_order(self, msg):
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
        """
        side = msg['side']
        price = msg['price']
        size = msg['remaining_size']
        order_id = msg['order_id']

        levels = self._get_levels(side)
        ob.add_order(levels, price, size, order_id)

    def done_order(self, msg):
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
        """
        side = msg['side']
        price = msg['price']
        size = msg['remaining_size']
        order_id = msg['order_id']

        levels = self._get_levels(side)
        ob.remove_order(levels, price, size, order_id)

    def match_order(self, msg):
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
        """
        side = msg['side']
        price = msg['price']
        size = msg['size']
        order_id = msg['maker_order_id']
        levels = self._get_levels(side)

        ob.remove_order(levels, price, size, order_id)

    def change_order(self, msg):
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
        """
        price = msg.get('price')
        if not price:
            # ignore market order
            return

        side = msg['side']
        old_size = msg['old_size']
        new_size = msg['new_size']
        order_id = msg['order_id']
        levels = self._get_levels(side)

        ob.update_order(levels, price, old_size, new_size, order_id)


if __name__ == '__main__':
    gx_ws = GdaxOrderBook()
    gx_ws.run()

    time.sleep(10)
    gx_ws.close()
