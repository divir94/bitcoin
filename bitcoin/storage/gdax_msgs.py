import logging
import pandas as pd
import multiprocessing as mp
from time import time
from threading import Thread

import bitcoin.logs.logger
import bitcoin.storage.util as st_util
from bitcoin.websocket.core import WebSocket


GX_WS_URL = 'wss://ws-feed.gdax.com'
GX_CHANNEL = {'type': 'subscribe', 'product_ids': ['BTC-USD']}
GX_HTTP_URL = 'https://api.gdax.com/products/BTC-USD/book'
logger = logging.getLogger('gdax_msg_storage')
# logger.setLevel(logging.DEBUG)

# mapping of db table name to field names to store
MSG_DB_MAPPING = {
    'received': {
        'table': 'GdaxReceived',
        'fields': ['time', 'received_time', 'sequence', 'order_id', 'price', 'size', 'side', 'funds', 'order_type'],
    },
    'open': {
        'table': 'GdaxOpen',
        'fields': ['time', 'received_time', 'sequence', 'order_id', 'price', 'remaining_size', 'side'],
    },
    'done': {
        'table': 'GdaxDone',
        'fields': ['time', 'received_time', 'sequence', 'order_id', 'price', 'remaining_size', 'side', 'reason'],
    },
    'match': {
        'table': 'GdaxMatch',
        'fields': ['time', 'received_time', 'sequence', 'trade_id', 'maker_order_id', 'taker_order_id', 'price', 'size',
                   'side'],
    },
    'change': {
        'table': 'GdaxChange',
        'fields': ['time', 'received_time', 'sequence', 'order_id', 'price', 'new_size', 'old_size', 'side',
                   'new_funds', 'old_funds'],
    },
}


def store_msgs(msgs):
    """
    Store list of messages to db
    """
    logger.debug('Store start')
    start = time()

    msg_dict = {}
    # convert messages to dict[type, list[msg]]
    for msg in msgs:
        _type = msg['type']
        msg_dict[_type] = msg_dict.get(_type, []) + [msg]

    df_dict = {}
    # convert to dict[tbl_name, df]
    for _type, msg_lst in msg_dict.iteritems():
        # filter to fiels we care about
        fields = MSG_DB_MAPPING[_type]['fields']
        df = pd.DataFrame(msg_lst, columns=fields)
        df['time'] = pd.to_datetime(df['time'])

        # add to dict
        tbl_name = MSG_DB_MAPPING[_type]['table']
        df_dict[tbl_name] = df

    # store
    st_util.store_dfs(df_dict)

    time_elapsed = time() - start
    logger.info('Took {:.2f}s to store {} messages'.format(time_elapsed, len(msgs)))
    return


# TODO(divir): store level 3 book
class GdaxMsgStorage(WebSocket):
    def __init__(self):
        super(GdaxMsgStorage, self).__init__(GX_WS_URL, GX_CHANNEL)
        self.msgs = []
        self.pool = mp.Pool()
        self.last_sequence = -1

        self.store_freq = 60  # frequency of storing messages
        self.last_store_time = time()

    def on_message(self, msg):
        """
        Check if message is out of order and store messages at regular intervals
        """
        self.check_msg(msg)
        msg['received_time'] = pd.datetime.utcnow()
        self.msgs.append(msg)

        # store messages aynchronously in a separate process
        time_elapsed = time() - self.last_store_time
        if time_elapsed >= self.store_freq:
            logger.info('Storing {} messages after {:.2f}s'.format(len(self.msgs), time_elapsed))
            msgs_copy = list(self.msgs)
            self.pool.apply_async(store_msgs, args=(msgs_copy,))
            # Thread(target=store_msgs, args=(msgs_copy,)).start()
            self.msgs = []
            self.last_store_time = time()

    def check_msg(self, msg):
        """
        Log if message is out of order
        """
        sequence = msg['sequence']
        if sequence <= self.last_sequence:
            logger.error('Ignoring older msg: {}'.format(sequence))
        elif sequence != self.last_sequence + 1:
            logger.error('Out of sync: last sequence({}), message({})'.format(self.last_sequence, sequence))
        self.last_sequence = sequence


if __name__ == '__main__':
    ws = GdaxMsgStorage()
    ws.start()
