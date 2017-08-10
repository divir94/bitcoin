import json
import logging
import websocket
from threading import Thread


logger = logging.getLogger('core_websocket')


class WebSocket(websocket.WebSocketApp):
    def __init__(self, url, channel):
        super(WebSocket, self).__init__(url,
                                        on_open=self.on_open,
                                        on_message=self.on_message,
                                        on_error=self.on_error,
                                        on_close=self.on_close)
        self.channel = channel

    def on_open(self, ws):
        logger.info('Connecting to channel: {}'.format(self.channel))
        channel_str = json.dumps(self.channel)
        ws.send(channel_str)

    def on_message(self, ws, message):
        msg = json.loads(message)
        logger.debug(msg)

    def on_error(self, ws, error):
        logger.error('Message error')
        logger.error(error)

    def on_close(self, ws):
        logger.info('Closing websocket')

    def run(self):
        Thread(target=self.run_forever).start()
