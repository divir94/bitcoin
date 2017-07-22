import json
import pandas as pd
import logging
import sys

from pprint import pprint
from time import time

# websocket
import websocket
from threading import Thread

# logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

fh = logging.FileHandler('bot.log', 'w')
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

# websocket chanels
GDAX_FEED = 'wss://ws-feed.gdax.com'
BS_FEED = 'ws://ws.pusherapp.com/app/de504dc5763aeef9ff52?protocol=7'

GDAX_CHANNEL = json.dumps({'type': 'subscribe', 'product_ids': ['BTC-USD']})
BS_CHANNEL = '{"event":"pusher:subscribe", "data":{"channel":"order_book"}}'


class WebSocket(object):
    def __init__(self, feed, channel):
        self.feed = feed
        self.channel = channel
        self.ws = websocket.WebSocketApp(feed,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

    def on_open(self, ws):
        def run():
            logger.info('Running websocket')
            ws.send(self.channel)
            logger.info('Thread terminating...')

        t = Thread(target=run, args=())
        t.start()

    def on_message(self, ws, message):
        msg = json.loads(message)
        if msg['type'] == 'match':
            logger.info(msg)

    def on_error(self, ws, error):
        logger.error('Message error')
        logger.error(error)

    def on_close(self, ws):
        logger.info('Closing websocket')

    def run(self):
        self.ws.run_forever()


if __name__ == '__main__':
    gdax_ws = WebSocket(GDAX_FEED, GDAX_CHANNEL)
    gdax_ws.run()
