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
        self.book = ob.OrderBook(seq=-2)
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
            self.process_message(msg)

    def on_message(self, ws, message):
        msg = self.parse_message(message)
        sequence = msg['sequence']
        self.logger.debug('Msg receieved: {}'.format(msg['sequence']))

        if self.book.seq == -2:
            # start first time sync
            self.logger.info('Starting first sync')
            self.book.seq = -1
            self.book.queue = Queue()
            Thread(target=self.reset_book).start()
        elif self.book.seq == -1:
            # sync in process
            self.queue.put(msg)
            self.logger.info('Queuing msg: {}'.format(sequence))
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
            self.book.seq = -2
            return

        _type = msg['type']
        if _type == 'open':
            self.open_order(msg)

        # market orders will not have a price field in a done msg
        elif _type == 'done' and msg.get('price'):
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
        side = msg['side']
        price = msg['price']
        size = msg['remaining_size']
        order_id = msg['order_id']

        levels = self._get_levels(side)
        ob.add_order(levels, price, size, order_id)

    def done_order(self, msg):
        side = msg['side']
        price = msg['price']
        size = msg['remaining_size']
        order_id = msg['order_id']

        levels = self._get_levels(side)
        ob.remove_order(levels, price, size, order_id)

    def match_order(self, msg):
        # the side field indicates the maker order side
        side = msg['side']
        price = msg['price']
        size = msg['size']
        order_id = msg['maker_order_id']
        levels = self._get_levels(side)

        ob.update_order(levels, price, size, order_id)

    def change_order(self, msg):
        side = msg['side']
        price = msg['price']
        size = msg['new_size']
        order_id = msg['order_id']
        levels = self._get_levels(side)

        ob.update_order(levels, price, size, order_id)


if __name__ == '__main__':
    gx_ws = GdaxOrderBook()
    gx_ws.run()

    time.sleep(10)
    gx_ws.close()
