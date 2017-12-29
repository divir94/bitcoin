import json
import time
import yaml
import pandas as pd
from datetime import timedelta
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException

import bitcoin.logs.logger as lc
import bitcoin.util as util


logger = lc.config_logger('core_ws')


class WebSocket(object):
    """
    The core websocket connects and listens for messages. It is independent of any crypto specific stuff i.e.
    it does not maintain any order book information.
    """
    def __init__(self, url, products=None, channels=None):
        self.url = url
        self.channels = channels
        self.products = products

        self.ws = None
        self.thread = None
        self.stop = False

        self.ping_freq = timedelta(seconds=30)
        self.store_freq = timedelta(seconds=60)
        self.last_ping_time = pd.datetime.utcnow()
        self.last_store_time = pd.datetime.utcnow()

        self.store_path = '../data/trades.hdf5'
        self.trades = pd.DataFrame()
        
    def start(self):
        """
        Connect and listen to messages.
        """
        def _go():
            self._connect()
            self._listen()
            self._disconnect()

        self.stop = False
        self.thread = Thread(target=_go)
        self.thread.start()

    def _connect(self):
        """
        Create websocket object and send initial message.
        """
        params = dict(type='subscribe', product_ids=self.products, channels=self.channels)

        self.ws = create_connection(self.url)
        self.ws.send(json.dumps(params))

        logger.info('Subscribed to {}'.format(params))

    def _listen(self):
        """
        Listen forever and call message handler. Also, send frequent pings and check for missing heartbeat.
        """
        logger.info('Listening for messages')

        while not self.stop:
            try:
                # ping
                time_elapsed = util.time_elapsed(self.last_ping_time, self.ping_freq)
                if time_elapsed:
                    self.ws.ping('keepalive')
                    self.last_ping_time = pd.datetime.utcnow()

                # store
                time_elapsed = util.time_elapsed(self.last_store_time, self.store_freq)
                if time_elapsed:
                    self.trades.to_hdf(self.store_path, 'trades', append=True)
                    logger.info('Wrote {} records data to disk'.format(self.trades.shape[0]))
                    self.trades = pd.DataFrame()
                    self.last_store_time = pd.datetime.utcnow()

                # parse data
                data = self.ws.recv()
                msg = yaml.safe_load(data)

            except ValueError as e:
                self.on_error(e)
            except Exception as e:
                self.on_error(e)
            else:
                self.on_message(msg)

    def _disconnect(self):
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            logger.error(e)
        logger.info('Disconnected websocket')

    def close(self):
        logger.info('Closing websocket thread')
        self.stop = True
        self.thread.join()

    def on_message(self, msg):
        if msg['type'] == 'ticker':
            self.trades = self.trades.append(msg, ignore_index=True)

    def on_error(self, e, data=None):
        # self.stop = True
        logger.error('{} - data: {}'.format(e, data))


if __name__ == '__main__':
    import bitcoin.params as pms

    ws = WebSocket(url=pms.WS_URL['GDAX'], products=['BTC-USD'], channels=['ticker', 'heartbeat'])
    ws.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        ws.close()
