import logging
import pandas as pd
import multiprocessing as mp
from time import time

import bitcoin.logs.logger
import bitcoin.storage.util as st_util
from bitcoin.websocket.core import WebSocket


GX_WS_URL = 'wss://ws-feed.gdax.com'
GX_CHANNEL = {'type': 'subscribe', 'product_ids': ['BTC-USD']}
GX_HTTP_URL = 'https://api.gdax.com/products/BTC-USD/book'
logger = logging.getLogger('gdax_msg_storage')

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
    Store list of messages to db.
    """
    start = time()
    dfs = msgs_to_dfs(msgs)

    for _type, df in dfs.iteritems():
        tbl_name = MSG_DB_MAPPING[_type]['table']
        # filter to fiels we care about
        fields = MSG_DB_MAPPING[_type]['fields']
        df = df[fields]
        st_util.store_df(df, tbl_name)

    time_elapsed = time() - start
    logger.info('Took {:.2f}s to store {} messages'.format(time_elapsed, len(msgs)))
    return


def msgs_to_dfs(msgs):
    """
    Convert list of messages to a dict of dataframes based on message type.
    """
    msgs_dict = {}
    # add messages to dict
    for msg in msgs:
        _type = msg['type']
        msgs_dict[_type] = msgs_dict.get(_type, []) + [msg]

    # convert to df
    for k, v in msgs_dict.iteritems():
        df = pd.DataFrame(v)
        df['time'] = pd.to_datetime(df['time'])
        assert df['sequence'].is_unique
        msgs_dict[k] = df
    return msgs_dict


class GdaxMsgStorage(WebSocket):
    def __init__(self):
        super(GdaxMsgStorage, self).__init__(GX_WS_URL, GX_CHANNEL)
        self.msgs = []
        self.batch_size = 200  # wait for these many msgs before writing to db
        self.pool = mp.Pool()
        self.last_store_time = time()

    def on_message(self, msg):
        msg['received_time'] = pd.datetime.utcnow()
        self.msgs.append(msg)
        # store messages aynchronously in a separate process
        if len(self.msgs) >= self.batch_size:
            time_elapsed = time() - self.last_store_time
            logger.info('Storing {} messages after {:.2f}s'.format(len(self.msgs), time_elapsed))
            msgs_copy = list(self.msgs)
            self.pool.apply_async(store_msgs, args=(msgs_copy,))
            self.msgs = []
            self.last_store_time = time()


if __name__ == '__main__':
    ws = GdaxMsgStorage()
    ws.start()
