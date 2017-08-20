import pandas as pd
import time
from threading import Thread
from datetime import datetime, timedelta

import bitcoin.logs.logger as lc
import bitcoin.gdax.public_client as gdax
import bitcoin.gdax.params as params
import bitcoin.util as util
import bitcoin.storage.util as sutil
from bitcoin.websocket.core import WebSocket


GDAX_CLIENT = gdax.PublicClient()
logger = lc.config_logger('gdax_msgs')


class GdaxMsgStorage(WebSocket):
    def __init__(self, url, channel, product_id):
        super(GdaxMsgStorage, self).__init__(url, channel, error_callback=self.store_order_book)
        self.msgs = []
        self.last_sequence = -1
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
            Thread(target=self.store_msgs).start()

        # store order book
        time_elapsed = util.time_elapsed(self.last_book_store_time, self.book_store_freq)
        if time_elapsed:
            Thread(target=self.store_order_book).start()

    def check_msg(self, msg):
        """
        Log if message is out of order
        """
        sequence = msg['sequence']
        if sequence <= self.last_sequence:
            logger.error('Ignoring older msg in {}: {}'.format(self.product_id, sequence))
        elif sequence != self.last_sequence + 1:
            logger.error('{} out of sync: last sequence({}), message({})'.format(self.product_id,
                                                                                 self.last_sequence,
                                                                                 sequence))
            self.close()
            self.start()
            Thread(target=self.store_order_book).start()
        self.last_sequence = sequence

    def store_order_book(self):
        """
        Store order book to db
        """
        self.last_book_store_time = datetime.utcnow()
        logger.info('=' * 30)
        logger.info('Storing {} order book'.format(self.product_id))

        # get data
        data = GDAX_CLIENT.get_product_order_book(params.BTC_PRODUCT_ID, level=3)
        # to df
        timestamp = pd.datetime.utcnow()
        df = sutil.gdax_book_to_df(data, timestamp)
        # store
        table_name = params.SNAPSHOT_TBLS[self.product_id]
        sutil.store_df(df, table_name)

        logger.info('=' * 30)
        return

    def store_msgs(self):
        """
        Store list of messages to db
        """
        self.last_msg_store_time = datetime.utcnow()
        msgs = list(self.msgs)
        self.msgs = []
        start = datetime.utcnow()

        table_name = params.MSG_TBLS[self.product_id]
        columns = params.MSG_COLS_TBL
        # msgs to df
        df = pd.DataFrame(msgs, columns=columns)
        df['time'] = pd.to_datetime(df['time'])

        # store
        sutil.store_df(df, table_name)

        time_elapsed = (datetime.utcnow() - start).seconds
        logger.info('Stored {} messages in {} in {:.2f}s'.format(len(msgs), table_name, time_elapsed))
        return


if __name__ == '__main__':
    for product_id, channel in params.CHANNELS.iteritems():
        ws = GdaxMsgStorage(params.WS_URL, channel, product_id)
        ws.start()
        time.sleep(30)
