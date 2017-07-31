import json
import time

from bitcoin.websocket.core import WebSocket


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

        # handler change
        print msg



if __name__ == '__main__':
    gx_ws = GdaxOrderBook(None)
    gx_ws.run()

    time.sleep(5)
    gx_ws.close()
