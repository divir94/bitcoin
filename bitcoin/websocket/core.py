import json

# websocket
from websocket import WebSocketApp
from threading import Thread

# logging
import logging


# logging
logger = logging.getLogger('websocket')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fh = logging.FileHandler('../logs/bot.log', 'w')
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


class WebSocket(WebSocketApp):
    def __init__(self, url, channel):
        self.channel = channel
        super(WebSocket, self).__init__(url,
                                        on_open=self.on_open,
                                        on_message=self.on_message,
                                        on_error=self.on_error,
                                        on_close=self.on_close)

    def on_open(self, ws):
        logger.debug('Connecting to channel: {}'.format(self.channel))
        channel_str = json.dumps(self.channel)
        ws.send(channel_str)

    def on_message(self, ws, message):
        msg = json.loads(message)
        logger.debug(msg)

    def on_error(self, ws, error):
        logger.error('Message error')
        logger.error(error)

    def on_close(self, ws):
        logger.debug('Closing websocket')

    def run(self):
        t = Thread(target=self.run_forever)
        t.start()
