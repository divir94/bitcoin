import json
import time
import requests
from Queue import Queue
from threading import Thread

from bitcoin.websocket.core import WebSocket
import bitcoin.bot.order_book as ob


GX_WS_URL = 'wss://ws-feed.gdax.com'
GX_HTTP_URL = 'https://api.gdax.com/products/BTC-USD/book'


class GdaxOrderBook(WebSocket):
    def __init__(self, on_change):
        channel = {'type': 'subscribe', 'product_ids': ['BTC-USD']}
        super(GdaxOrderBook, self).__init__(GX_WS_URL, channel)
        self.exchange = 'GDAX'
        self.book = ob.OrderBook(seq=-2)
        self.on_change = on_change
        self.queue = Queue()

        print 'init {}'.format(self.book.seq)

    def reset_book(self):
        print 'resetting book'
        level = 3
        request = requests.get(GX_HTTP_URL, params={'level': level})
        data = json.loads(request.content)
        book = ob.get_order_book(data, level, seq='sequence')
        self.book = book
        print 'got book: {}'.format(self.book.seq)

        if not self.queue.empty():
            print 'Applying queued msgs'

        while not self.queue.empty():
            msg = self.queue.get()
            self.process_message(msg)

    def on_message(self, ws, message):
        msg = self.parse_message(message)
        print 'Msg: {}'.format(msg['sequence'])

        if self.book.seq < 0:
            print 'Queuing msg'
            # orderbook snapshot not loaded yet
            self.queue.put(msg)

            if self.book.seq == -2:
                # start first resync
                self.book.seq = -1
                t = Thread(target=self.reset_book)
                t.start()
        else:
            self.process_message(msg)

    def process_message(self, msg):
        sequence = msg['sequence']

        if sequence <= self.book.seq:
            # ignore older messages (e.g. before order book initialization from getProductOrderBook)
            return
        elif sequence != self.book.seq + 1:
            # resync
            print 'resynching'
            self.book.seq = -1
            self.queue = Queue()

            t = Thread(target=self.reset_book)
            t.start()

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
        print 'Book: {}'.format(self.book.seq)

    def parse_message(self, msg):
        numeric_fields = ['price', 'size', 'old_size', 'new_size', 'remaining_size']
        msg = json.loads(msg)
        msg = {k: float(v) if k in numeric_fields else v
               for k, v in msg.iteritems()}
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
    gx_ws = GdaxOrderBook(None)
    gx_ws.run()

    time.sleep(5)
    gx_ws.close()
