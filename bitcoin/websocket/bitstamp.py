import json
import time

import bitcoin.order_book as ob
from bitcoin.websocket.core import WebSocket


BS_URL = 'ws://ws.pusherapp.com/app/de504dc5763aeef9ff52?protocol=7'


class BitstampOrderBook(WebSocket):
    def __init__(self, on_change=None, channel='order_book'):
        channel = {'event': 'pusher:subscribe', 'data': {'channel': channel}}
        super(BitstampOrderBook, self).__init__(BS_URL, channel)
        self.exchange = 'BITSTAMP'
        self.book = None
        self.on_change = on_change

    def on_message(self, ws, message):
        msg = json.loads(message)
        self.logger.debug(msg)

        # handler change
        if msg['event'] == 'data':
            data = json.loads(msg['data'])
            # update local book
            self.book = ob.get_order_book(data, level=2)
            # callback
            # self.on_change(self.book, self.exchange)


if __name__ == '__main__':
    ws = BitstampOrderBook()
    ws.run()

    time.sleep(10)
    ws.close()
