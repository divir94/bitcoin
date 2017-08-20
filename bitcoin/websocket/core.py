import json
import logging
import time
from threading import Thread
from websocket import create_connection


logger = logging.getLogger('core_websocket')


# TODO(divir): add an optional dict[name, func and freq] are to call func periodically
class WebSocket(object):
    def __init__(self, url, channel, error_callback=None):
        self.url = url
        self.channel = channel
        self.ws = None

        self.ping_freq = 30
        self.heartbeat = True
        self.last_heartbeat = time.time()
        self.heartbeat_tol = 2
        self.check_freq = None
        self.last_check = time.time()
        self.error_callback = error_callback

        self.stop = False
        
    def start(self):
        """
        Connect and listen to messages
        """
        def _go():
            self._connect()
            self._listen()

        Thread(target=_go).start()
            
    def _connect(self):
        """
        Create websocket object and send initial message.
        """
        logger.info('Connecting to url: {} channel: {}'.format(self.url, self.channel))
        self.ws = create_connection(self.url)
        self.ws.send(json.dumps(self.channel))

        # heatbeat
        if self.heartbeat:
            logger.info('Turning on heartbeat')
            self.ws.send(json.dumps({'type': 'heartbeat', 'on': True}))
            self.last_heartbeat = time.time()

    def _listen(self):
        """
        Listen forever and call message handler. Also, send frequent pings and check for missing heartbeat.
        """
        logger.info('Listening for messages')
        self.stop = False

        while not self.stop:
            # ping every few seconds to keep connection alive
            if int(time.time() % self.ping_freq) == 0:
                self.ws.ping('keepalive')

            # restart if we missed a heartbeat
            if self.heartbeat and (time.time() - self.last_heartbeat) > self.heartbeat_tol:
                logger.error('Missed a heartbeat!')
                self.close()
                self.start()
                return

            # check book
            if self.check_freq and (time.time() - self.last_check) > self.check_freq:
                Thread(target=self.check_book).start()
                self.last_check = time.time()

            # save heartbeat and handle message
            msg = None
            try:
                msg = self.ws.recv()
                msg = json.loads(msg)
            except Exception as e:
                self.on_error(e, msg)
            else:
                if msg['type'] == 'heartbeat':
                    logger.debug('Got heartbeat: {}'.format(msg))
                else:
                    self.on_message(msg)
                self.last_heartbeat = time.time()

    def close(self):
        """
        Close the connection and turn off heartbeat.
        """
        logger.info('Closing websocket')
        self.stop = True

        # turn off heartbeat
        if self.heartbeat:
            self.ws.send(json.dumps({'type': 'heartbeat', 'on': False}))

        # stop listening and close the websocket
        time.sleep(1)
        self.ws.close()
        time.sleep(1)

    def on_message(self, msg):
        # logger.debug(msg)
        pass

    def check_book(self):
        pass

    def on_error(self, error, msg):
        logger.exception('Message error: {}\nMessage: {}'.format(error, msg))
        self.close()
        self.start()
        if self.error_callback is not None:
            Thread(target=self.error_callback)
