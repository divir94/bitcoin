import time
import sys
from datetime import datetime, timedelta
from threading import Thread
import pandas as pd

import bitcoin.gdax.public_client as gdax
import bitcoin.logs.logger as lc
import bitcoin.params as params
import bitcoin.storage.util as sutil
import bitcoin.util as util
from bitcoin.websocket.core import WebSocket

GDAX_CLIENT = gdax.PublicClient()


class GdaxMsgStorage(WebSocket):
    def __init__(self, url, channel, product_id):
        super(GdaxMsgStorage, self).__init__(url, channel)
        self.msgs = []
        self.last_sequence = -1
        self.product_id = product_id
        self.date_format = "%Y-%m-%dT%H:%M:%S.%fZ"

        self.msg_store_freq = timedelta(minutes=1)  # frequency of storing messages
        self.book_store_freq = timedelta(minutes=60)  # frequency of storing order book
        self.last_msg_store_time = datetime.utcnow()
        self.last_book_store_time = datetime.utcnow() - self.book_store_freq

    def on_message(self, msg):
        """
        Check if message is out of order and store messages at regular intervals
        """
        msg['received_time'] = datetime.utcnow().strftime(self.date_format)

        if self.last_sequence != -1:
            self.check_msg(msg)

        self.msgs.append(msg)
        if msg['type'] == 'error':
            logger.error(msg)

        # store messages
        time_elapsed = util.time_elapsed(self.last_msg_store_time, self.msg_store_freq)
        if time_elapsed:
            Thread(target=self.store_msgs).start()
            self.last_msg_store_time = datetime.utcnow()

        # store order book
        time_elapsed = util.time_elapsed(self.last_book_store_time, self.book_store_freq)
        if time_elapsed:
            Thread(target=self.store_order_book).start()
            self.last_book_store_time = datetime.utcnow()

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
        logger.info('=' * 30)
        logger.info('Storing {} order book'.format(self.product_id))

        # get data
        data = GDAX_CLIENT.get_product_order_book(self.product_id, level=3)
        # to df
        timestamp = pd.datetime.utcnow().strftime(self.date_format)
        df = sutil.gdax_book_to_df(data, timestamp)
        # store
        table_name = params.GX_SNAPSHOT_TBLS[self.product_id]
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

        table_name = params.GX_MSG_TBLS[self.product_id]
        columns = params.GX_MSG_COLS_TBL
        # msgs to df
        df = pd.DataFrame(msgs, columns=columns)

        # store
        sutil.store_df(df, table_name)

        time_elapsed = (datetime.utcnow() - start).total_seconds()
        logger.info('Stored {} messages in {} in {:.2f}s'.format(len(msgs), table_name, time_elapsed))
        return


if __name__ == '__main__':
    product_id = sys.argv[1]
    channel = params.GX_CHANNELS[product_id]
    logger = lc.config_logger('gdax_msgs', fsuffix=product_id)

    ws = GdaxMsgStorage(params.GX_WS_URL, channel, product_id)
    ws.start()