import json
import yaml
import pandas as pd
from datetime import timedelta
from websocket import create_connection, WebSocketConnectionClosedException

import bitcoin.logs.logger as lc
import bitcoin.util as util


logger = lc.config_logger('core_ws')


class WebSocket(object):
    """
    The core websocket connects and listens for messages.
    """
    def __init__(self, url, products, channels):
        self.url = url
        self.channels = channels
        self.products = products

        self.ws = None
        self.thread = None
        self.stop = False

        self.ping_freq = timedelta(seconds=30)
        self.last_ping_time = pd.datetime.utcnow()
        
    def start(self):
        """
        Connect and listen to messages.
        """
        self.stop = False
        self._connect()
        self._listen()
        self._disconnect()

    def _connect(self):
        """
        Create websocket object and send initial message.
        """
        params = dict(type='subscribe', product_ids=self.products, channels=self.channels)
        logger.info('Subscribing to {}'.format(params))

        connected = False

        while not connected:
            try:
                self.ws = create_connection(self.url)
                self.ws.send(json.dumps(params))
                connected = True
            except Exception as e:
                logger.error('Failed to connect: {}'.format(e))

    def _listen(self):
        """
        Listen forever and call message handler. Also, send frequent pings and check for missing heartbeat.
        """
        logger.info('Listening for messages')

        while not self.stop:
            # ping
            try:
                time_elapsed = util.time_elapsed(self.last_ping_time, self.ping_freq)
                if time_elapsed:
                    self.ws.ping('keepalive')
                    self.last_ping_time = pd.datetime.utcnow()
            except Exception as e:
                logger.error('Failed to ping: {}'.format(e))

            # parse data
            try:
                data = self.ws.recv()
                msg = yaml.safe_load(data)
            except Exception as e:
                self.on_error(e)
            else:
                self.on_message(msg)

    def _disconnect(self):
        logger.info('Disconnecting websocket')
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            logger.error(e)

    def close(self):
        logger.info('Closing websocket')
        self.stop = True

    def on_message(self, msg):
        logger.debug(msg)

    def on_error(self, e, data=None):
        logger.error('{} - data: {}'.format(e, data))
        self.close()
        self.start()


if __name__ == '__main__':
    import bitcoin.params as pms

    ws = WebSocket(url=pms.WS_URL[pms.DEFAULT_EXCHANGE],
                   products=[pms.DEFAULT_PRODUCT],
                   channels=['full', 'heartbeat'])
    ws.start()
