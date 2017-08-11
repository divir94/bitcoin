import json
import logging
import websocket
from threading import Thread
from time import time

import bitcoin.util as util


logger = logging.getLogger('core_websocket')


class WebSocket(websocket.WebSocketApp):
    def __init__(self, url, channel, heartbeat=None):
        super(WebSocket, self).__init__(url,
                                        on_open=self.on_open,
                                        on_message=self.on_message,
                                        on_error=self.on_error,
                                        on_close=self.on_close)
        self.heartbeat_freq = 30
        self.channel = channel
        self.heartbeat = heartbeat

    def on_open(self, ws):
        logger.info('Connecting to channel: {}'.format(self.channel))
        ws.send(json.dumps(self.channel))
        if self.heartbeat:
            ws.send(json.dumps(self.heartbeat))

    def on_message(self, ws, message):
        # heartbeat
        if int(time() % self.heartbeat_freq) == 0:
            ws.ping('keepalive')

        # handle msg
        msg = self.parse_message(message)
        try:
            self.handle_message(msg)
        except Exception as e:
            logger.exception(e)

    def handle_message(self, message):
        raise NotImplementedError

    def parse_message(self, msg):
        """convert fields to number"""
        msg = json.loads(msg)
        msg = {k: util.to_decimal(v) for k, v in msg.iteritems()}
        return msg

    def on_error(self, ws, error):
        logger.error('Message error')
        logger.error(error)

    def on_close(self, ws):
        logger.info('Closing websocket')

    def run(self):
        Thread(target=self.run_forever).start()
