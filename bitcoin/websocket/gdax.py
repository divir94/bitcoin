import json
import time
from sortedcontainers import SortedListWithKey

from bitcoin.websocket.core import WebSocket
from bitcoin.bot.order_book import OrderBook, PriceLevel


GX_URL = 'wss://ws-feed.gdax.com'


class GdaxOrderBook(WebSocket):
    def __init__(self, on_change):
        channel = {'type': 'subscribe', 'product_ids': ['BTC-USD']}
        super(GdaxOrderBook, self).__init__(GX_URL, channel)
        self.exchange = 'GDAX'
        self.book = None
        self.on_change = on_change

    def on_message(self, ws, message):
        msg = json.loads(message)
        _type = msg['type']

        if _type == 'open':
            pass
        elif _type == 'match':
            pass
        elif type == 'change':
            pass

    def add_order(self):
        pass

    def remove_order(self):
        pass

    def change_order(self):
        pass



if __name__ == '__main__':
    gx_ws = GdaxOrderBook(None)
    gx_ws.run()

    time.sleep(5)
    gx_ws.close()
