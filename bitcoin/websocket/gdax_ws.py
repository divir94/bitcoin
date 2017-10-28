import logging
from collections import deque
from copy import deepcopy
from threading import Thread

import bitcoin.gdax.public_client as gdax
import bitcoin.logs.logger as lc
import bitcoin.order_book.gdax_order_book as ob
import bitcoin.order_book.util as ob_util
import bitcoin.params as params
import bitcoin.util as util
from bitcoin.websocket.core_ws import WebSocket

logger = lc.config_logger('gdax_websocket')


class GdaxWebSocket(WebSocket):
    """
    Maintains an up to date instance of GdaxOrderBook. Is responsible for queuing and applying messages and
    restarting the book if needed.
    """
    def __init__(self, product_id, on_change=None):
        self.exchange = 'GDAX'
        url = params.WS_URL[self.exchange]
        channel = params.CHANNEL[self.exchange][product_id]
        super(GdaxWebSocket, self).__init__(url, channel)

        self.book = ob.GdaxOrderBook(sequence=-1)
        self.queue = deque()
        self.on_change = on_change
        self.gdax_client = gdax.PublicClient()
        self.product_id = product_id

        self.restart = True  # load the order book
        self.syncing = False  # sync in process i.e. loading order book or applying messages
        self.check_freq = 3600  # check every x seconds

    def on_message(self, msg):
        msg = util.to_numeric(msg, params.MSG_NUMERIC_FIELD[self.exchange])
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
            return -1
        elif sequence != book.sequence + 1:
            # resync
            logger.warning('Out of sync: book({}), message({})'.format(book.sequence, sequence))
            self.restart = True
            return -1

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

    def apply_queue(self, book, end=None):
        """apply queued messages to book till end sequence"""
        logger.info('Applying queued msgs: {}'.format(book.sequence))
        end = end or float('inf')

        while not self.restart and len(self.queue):
            msg = self.queue.popleft()
            sequence = msg['sequence']

            if sequence > end:
                self.queue.appendleft(msg)
                break

            result = self.process_message(msg, book)
            if result != -1:
                logger.debug('Processed queued msg: {}'.format(sequence))
        logger.info('Book ready: {}'.format(book.sequence))
        return

    def check_book(self):
        self.syncing = True
        logger.info('^' * 30)

        # save current book
        current_book = deepcopy(self.book)
        logger.info('Checking book start: {}'.format(current_book.sequence))
        logger.setLevel(logging.DEBUG)

        # get expected book
        data = self.gdax_client.get_product_order_book(self.product_id, level=3)
        expected_book = ob.GdaxOrderBook(sequence=data['sequence'], bids=data['bids'], asks=data['asks'])
        logger.info('Expected book: {}'.format(expected_book.sequence))

        # apply queue to current book
        self.apply_queue(current_book, end=expected_book.sequence)

        # compare diffs
        num_diff = ob_util.compare_books(current_book, expected_book)
        msg = 'Book differences: {}'.format(num_diff)
        logger.error(msg) if num_diff > 0 else logger.info(msg)

        # reset book
        self.apply_queue(expected_book)
        self.book = expected_book
        self.queue = deque()
        self.syncing = False
        logger.setLevel(logging.INFO)
        logger.info('Checking book end: {}'.format(self.book.sequence))
        logger.info('^' * 30)
        return


if __name__ == '__main__':
    ws = GdaxWebSocket(product_id='BTC-USD')
    ws.start()
