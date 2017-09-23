import pandas as pd

import bitcoin.order_book.gdax_order_book as ob
import bitcoin.params as params
import bitcoin.storage.util as sutil
import bitcoin.logs.logger as lc


logger = lc.config_logger('storage_api', level='DEBUG', file_handler=False)


def get_book(exchange, product_id, timestamp=None, sequence=None, live=False):
    """
    Get orderbook at a particular time or sequence number

    Parameters
    ----------
    exchange: str
    product_id: str
    timestamp: datetime
        None returns the latest book
    sequence: int
        None returns the latest book
    live: bool

    Returns
    -------
    ob.GdaxOrderBook
    """
    assert not (timestamp and sequence), 'Cannot specify both timestamp or sequence'
    time_str = timestamp.strftime(params.DATE_FORMAT[exchange]) if timestamp else None

    # get latest snapshot
    snapshot_df = get_closest_snapshot(exchange, product_id, timestamp, sequence)
    logger.debug('Got snapshot')

    # convert to book object
    book = get_book_from_df(snapshot_df)
    logger.debug('Got book: {}'.format(book.sequence))

    # get and apply messages
    messages = get_messages(exchange, product_id, book.sequence)
    for i, msg in enumerate(messages):
        if i % 10000 == 0:
            logger.debug('Applying msgs starting: {}'.format(msg['sequence']))
        # break if reached id. If id is None apply all available msgs
        if (timestamp and msg['time'] > time_str) or (sequence and msg['sequence'] > sequence):
            break
        book.process_message(msg)
    logger.debug('Book ready: {}'.format(book.sequence))
    return book


def get_closest_snapshot(exchange, product_id, timestamp=None, sequence=None):
    table_name = params.SNAPSHOT_TBL[exchange][product_id]

    # get closest snapshot before timestamp or sequence
    if timestamp or sequence:
        _id = 'received_time' if timestamp else 'sequence'
        value = timestamp.strftime(params.DATE_FORMAT[exchange]) if timestamp else sequence

        sql = '''
        SELECT * FROM {table}
        WHERE {id} = (
            SELECT {id} FROM {table}
            WHERE {id} <= '{value}'
            ORDER BY {id} desc
            LIMIT 1
        )
        '''.format(table=table_name, id=_id, value=value)
    else:
        # latest snapshot
        sql = '''
        SELECT * FROM {table}
        WHERE sequence = (
            SELECT max(sequence) FROM {table}
        )
        '''.format(table=table_name)
    logger.debug(sql)
    df = pd.read_sql(sql, con=sutil.ENGINE)
    assert not df.empty, 'No snapshot found at time: {}, sequence: {}'.format(timestamp, sequence)
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


def get_messages(exchange, product_id, start=None, end=None):
    table_name = params.MSG_TBL[exchange][product_id]
    start = start or -1
    sql = 'SELECT * from {} WHERE sequence >= {}'.format(table_name, start)
    if end:
        sql += 'AND sequence <= {}'.format(end)
    logger.DEBUG(sql)
    messages = sutil.xread_sql(sql)
    return messages
