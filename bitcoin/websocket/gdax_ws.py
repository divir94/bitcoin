from collections import deque
from threading import Thread
from datetime import datetime, timedelta

import bitcoin.gdax.public_client as gdax
import bitcoin.logs.logger as lc
import bitcoin.order_book.gdax_order_book as ob
import bitcoin.params as pms
import bitcoin.util as util
import bitcoin.websocket.core_ws as cws


logger = lc.config_logger('gdax_ws')


class GdaxWebSocket(cws.WebSocket):
    """
    Maintains an up to date instance of GdaxOrderBook.
    Is responsible for queuing and applying messages and restarting the book if needed.
    """
    def __init__(self):
        url = pms.WS_URL['GDAX']
        channels = ['full', 'heartbeat']
        product = pms.DEFAULT_PRODUCT
        super(GdaxWebSocket, self).__init__(url=url, products=[product], channels=channels)

        self.book = ob.GdaxOrderBook(sequence=-1)
        self.queue = deque()
        self.gdax_client = gdax.PublicClient()
        self.product_id = product
        self.ignore_msgs = ['subscriptions', 'heartbeat']
        self.last_heartbeat = None
        self.heartbeat_freq = timedelta(seconds=2)

        self.restart = True  # load the order book
        self.syncing = False  # sync in process i.e. loading order book or applying messages

    def on_message(self, msg):
        self.check_heartbeat(msg)

        if msg['type'] in self.ignore_msgs:
            return

        msg = util.parse_message(msg)
        sequence = msg['sequence']

        if self.restart:
            # reset order book and clear queue
            logger.info('Restarting sync', extra={'sequence': self.book.sequence})
            self.queue = deque()
            Thread(target=self.reset_book).start()
            self.restart = False
        elif self.syncing:
            # sync in process, queue msgs
            logger.debug('Queuing msg: {}'.format(sequence))
            self.queue.append(msg)
        else:
            logger.debug('Processing msg: {}'.format(sequence))
            self.process_message(msg)

    def process_message(self, msg, book=None):
        book = book or self.book
        sequence = msg['sequence']

        if sequence <= book.sequence:
            # ignore older messages (e.g. before order book initialization from getProductOrderBook)
            logger.debug('Ignoring older msg: {}'.format(sequence))
        elif sequence != book.sequence + 1:
            # resync
            logger.error('Out of sync: book({}), message({})'.format(book.sequence, sequence))
            self.restart = True
        else:
            book.process_message(msg, book)

    def reset_book(self):
        """get level 3 order book and apply pending messages from queue"""
        self.syncing = True
        logger.info('='*30)
        logger.info('Loading book', extra={'sequence': self.book.sequence})

        # get book
        data = self.gdax_client.get_product_order_book(self.product_id, level=3)
        self.book = ob.GdaxOrderBook(sequence=data['sequence'], bids=data['bids'], asks=data['asks'])
        logger.info('Got book: {}'.format(self.book.sequence))

        # apply queue
        self.apply_queue(self.book)
        self.queue = deque()
        self.syncing = False
        logger.info('=' * 30)

    def apply_queue(self, book):
        logger.info('Applying queued msgs: {}'.format(book.sequence))

        while not self.restart and len(self.queue):
            msg = self.queue.popleft()
            self.process_message(msg, book)

        logger.info('Book ready: {}'.format(book.sequence))

    def check_heartbeat(self, msg):
        """
        Restart websocket if heartbeat is missed.

        Parameters
        ----------
        msg: dict
        """
        missed_hb = util.time_elapsed(self.last_heartbeat, self.heartbeat_freq)
        # restart websocket
        if missed_hb:
            last_hb = (datetime.utcnow() - self.last_heartbeat).seconds if self.last_heartbeat else None
            logger.error('Missed heartbeat! Last heartbeat {}s'.format(last_hb))
            self.last_heartbeat = None
            self.close()
            self.start()

        # reset time
        if msg['type'] == 'heartbeat':
            logger.info('Got heartbeat')
            self.last_heartbeat = datetime.utcnow()


if __name__ == '__main__':
    ws = GdaxWebSocket()
    ws.start()
