import json
import logging
import websocket
from threading import Thread

import bitcoin.util as util


default_logger = util.get_logger(name='websocket', fname='websocket.log', level=logging.DEBUG)
websocket.enableTrace(True)


class WebSocket(websocket.WebSocketApp):
    def __init__(self, url, channel, logger=None):
        super(WebSocket, self).__init__(url,
                                        on_open=self.on_open,
                                        on_message=self.on_message,
                                        on_error=self.on_error,
                                        on_close=self.on_close)
        self.channel = channel
        self.logger = logger or default_logger

    def on_open(self, ws):
        self.logger.debug('Connecting to channel: {}'.format(self.channel))
        channel_str = json.dumps(self.channel)
        ws.send(channel_str)

    def on_message(self, ws, message):
        msg = json.loads(message)
        self.logger.debug(msg)

    def on_error(self, ws, error):
        self.logger.error('Message error')
        self.logger.error(error)

    def on_close(self, ws):
        self.logger.debug('Closing websocket')

    def run(self):
        Thread(target=self.run_forever).start()
