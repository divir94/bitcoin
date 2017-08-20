import pandas as pd
import multiprocessing as mp
from threading import Thread
from datetime import datetime, timedelta

import bitcoin.logs.logger as lc
import bitcoin.storage.util as st_util
import bitcoin.gdax.public_client as gdax
import bitcoin.gdax.params as params
import bitcoin.util as util
from bitcoin.websocket.core import WebSocket


logger = lc.config_logger('gdax_msgs')
# logger.setLevel(logging.DEBUG)
GDAX_CLIENT = gdax.PublicClient()


def store_order_book(product_id):
    logger.info('=' * 30)
    logger.info('Storing order book')

    # get data
    data = GDAX_CLIENT.get_product_order_book(params.BTC_PRODUCT_ID, level=3)
    # to df
    timestamp = pd.datetime.utcnow()
    df = gdax_book_to_df(data, timestamp)
    # store
    table_name = params.SNAPSHOT_TBLS[product_id]
    st_util.store_df(df, table_name)

    logger.info('=' * 30)
    return


def gdax_book_to_df(data, timestamp):
    # combine bids and asks
    columns = ['price', 'size', 'order_id']
    bids = pd.DataFrame(data['bids'], columns=columns)
    asks = pd.DataFrame(data['asks'], columns=columns)
    bids['side'] = 'bid'
    asks['side'] = 'ask'
    df = pd.concat([bids, asks])

    # add sequence and timestamp
    df['sequence'] = data['sequence']
    df['received_time'] = timestamp
    return df


def store_msgs(msgs, product_id):
    """
    Store list of messages to db
    """
    logger.info('~' * 30)
    logger.debug('Storing messages')
    start = datetime.utcnow()

    table_name = params.MSG_TBLS[product_id]
    columns = params.MSG_COLS_TBL
    # msgs to df
    df = pd.DataFrame(msgs, columns=columns)
    df['time'] = pd.to_datetime(df['time'])

    # store
    st_util.store_df(df, table_name)

    time_elapsed = (datetime.utcnow() - start).seconds
    logger.info('Took {:.2f}s to store {} messages'.format(time_elapsed, len(msgs)))
    logger.info('~' * 30)
    return


class GdaxMsgStorage(WebSocket):
    def __init__(self, url, channel, product_id):
        super(GdaxMsgStorage, self).__init__(url, channel, error_callback=self.store_book_wrapper)
        self.msgs = []
        self.last_sequence = -1
        self.pool = mp.Pool()
        self.product_id = product_id

        self.msg_store_freq = timedelta(seconds=60)  # frequency of storing messages
        self.book_store_freq = timedelta(minutes=60)  # frequency of storing order book
        self.last_msg_store_time = datetime.utcnow()
        self.last_book_store_time = datetime.utcnow() - self.book_store_freq

    def on_message(self, msg):
        """
        Check if message is out of order and store messages at regular intervals
        """
        msg['received_time'] = datetime.utcnow()

        if self.last_sequence != -1:
            self.check_msg(msg)

        self.msgs.append(msg)

        # store messages
        time_elapsed = util.time_elapsed(self.last_msg_store_time, self.msg_store_freq)
        if time_elapsed:
            self.store_msgs_wrapper()

        # store order book
        time_elapsed = util.time_elapsed(self.last_book_store_time, self.book_store_freq)
        if time_elapsed:
            self.store_book_wrapper()

    def check_msg(self, msg):
        """
        Log if message is out of order
        """
        sequence = msg['sequence']
        if sequence <= self.last_sequence:
            logger.error('Ignoring older msg: {}'.format(sequence))
        elif sequence != self.last_sequence + 1:
            logger.error('Out of sync: last sequence({}), message({})'.format(self.last_sequence, sequence))
            self.close()
            self.start()
            self.store_book_wrapper()
        self.last_sequence = sequence

    def store_book_wrapper(self):
        self.pool.apply_async(store_order_book, args=(self.product_id,))
        self.last_book_store_time = datetime.utcnow()

    def store_msgs_wrapper(self):
        args = (list(self.msgs), self.product_id)
        # self.pool.apply_async(store_msgs, args=args)
        Thread(target=store_msgs, args=args).start()
        self.msgs = []
        self.last_msg_store_time = datetime.utcnow()


if __name__ == '__main__':
    btc_ws = GdaxMsgStorage(params.WS_URL, params.BTC_CHANNEL, params.BTC_PRODUCT_ID)
    btc_ws.start()
