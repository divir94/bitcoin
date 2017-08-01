import json
import time
import requests

from bitcoin.websocket.core import WebSocket
import bitcoin.bot.order_book as ob


GX_WS_URL = 'wss://ws-feed.gdax.com'
GX_HTTP_URL = 'https://api.gdax.com/products/BTC-USD/book'


class GdaxOrderBook(WebSocket):
    def __init__(self, on_change):
        channel = {'type': 'subscribe', 'product_ids': ['BTC-USD']}
        super(GdaxOrderBook, self).__init__(GX_WS_URL, channel)
        self.exchange = 'GDAX'
        self.book = self.reset_book()
        print 'init'
        print self.book.id
        self.on_change = on_change

    #TODO(divir): make this aync
    def reset_book(self):
        level = 3
        request = requests.get(GX_HTTP_URL, params={'level': level})
        data = json.loads(request.content)
        book = ob.get_order_book(data, level, _id='sequence')
        return book

    #TODO(divir): queue msgs and apply
    def on_message(self, ws, message):
        msg = self.parse_msg(message)
        _type = msg['type']
        sequence = msg['sequence']
        print sequence

        if sequence <= self.book.id:
            # ignore older messages (e.g. before order book initialization from getProductOrderBook)
            return
        elif sequence > self.book.id + 1:
            # missing packet restart
            print 'restarting'
            print self.book.id, sequence
            #self.close()
            # self.run()
            return
        self.book.id = sequence

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

    def parse_msg(self, message):
        numeric_fields = ['price', 'size', 'old_size', 'new_size', 'remaining_size']
        msg = json.loads(message)
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

        ob.remove_order(levels, price, size, order_id)

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
