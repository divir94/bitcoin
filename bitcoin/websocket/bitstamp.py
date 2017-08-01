import json
import time
from sortedcontainers import SortedListWithKey

import bitcoin.bot.order_book as ob
from bitcoin.websocket.core import WebSocket


BS_URL = 'ws://ws.pusherapp.com/app/de504dc5763aeef9ff52?protocol=7'


class BitstampOrderBook(WebSocket):
    def __init__(self, on_change, channel='order_book'):
        channel = {'event': 'pusher:subscribe', 'data': {'channel': channel}}
        super(BitstampOrderBook, self).__init__(BS_URL, channel)
        self.exchange = 'BITSTAMP'
        self.book = None
        self.on_change = on_change

    def on_message(self, ws, message):
        msg = json.loads(message)

        # handler change
        if msg['event'] == 'data':
            data = json.loads(msg['data'])
            # update local book
            self.book = ob.get_order_book_from_level_2(data)
            # callback
            self.on_change(self.book, self.exchange)


if __name__ == '__main__':
    def echo(book, exchange):
        print book

    ws = BitstampOrderBook(echo)
    ws.run()

    time.sleep(10)
    ws.close()
