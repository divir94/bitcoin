import pandas as pd

import bitcoin.order_book.gdax_order_book as ob
import bitcoin.params as params
import bitcoin.storage.util as sutil
import bitcoin.logs.logger as lc


logger = lc.config_logger('storage_api', level='DEBUG', file_handler=False)


def get_book(exchange, product_id, time=None, id=None, live=False):
    # get latest snapshot
    snapshot_tbl = params.GX_SNAPSHOT_TBLS[product_id]
    msg_name = params.GX_MSG_TBLS[product_id]
    snapshot_df = get_closest_snapshot(snapshot_tbl, id)
    logger.debug('Got snapshot')

    # convert to book object
    book = get_book_from_df(snapshot_df)
    logger.debug('Got book: {}'.format(book.sequence))

    # get and apply messages
    messages = get_messages(msg_name, book.sequence)
    for i, msg in enumerate(messages):
        if i % 10000 == 0:
            logger.debug('Applying msg: {}'.format(msg['sequence']))
        book.process_message(msg)
    logger.debug('Book ready: {}'.format(book.sequence))
    return book


def get_closest_snapshot(table_name, sequence=None):
    if sequence:
        sql = '''
        SELECT * FROM {table}
        WHERE sequence = (
            SELECT sequence FROM {table}
            WHERE sequence <= {sequence}
            ORDER BY sequence desc
            LIMIT 1
        )
        '''.format(table=table_name, sequence=sequence)
    else:
        # latest snapshot
        sql = '''
        SELECT * FROM {table}
        WHERE sequence = (
            SELECT max(sequence) FROM {table}
        )
        '''.format(table=table_name)
    df = pd.read_sql(sql, con=sutil.ENGINE)
    return df


def get_book_from_df(df):
    # get sequence
    sequences = df['sequence'].unique()
    assert len(sequences) == 1
    sequence = sequences[0]

    bids = df[df['side'] == 'bid']
    asks = df[df['side'] == 'ask']
    columns = ['price', 'size', 'order_id']
    bids = bids[columns].values
    asks = asks[columns].values
    book = ob.GdaxOrderBook(sequence, bids, asks)
    return book


def get_messages(table_name, sequence=None):
    sequence = sequence or -1
    sql = 'SELECT * from {} WHERE sequence >= {}'.format(table_name, sequence)
    messages = sutil.xread_sql(sql)
    return messages
