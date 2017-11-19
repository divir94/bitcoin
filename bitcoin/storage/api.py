import pandas as pd
from collections import namedtuple

import bitcoin.params as params
import bitcoin.storage.util as sutil
import bitcoin.logs.logger as lc
import bitcoin.util as util


logger = lc.config_logger('storage_api', level='DEBUG', file_handler=False)
DEFAULT_EXCHANGE = 'GDAX'
DEFAULT_PRODUCT = 'BTC-USD'
Dataset = namedtuple('Dataset', ['book', 'messages'])


def get_book(at=None, exchange=None, product=None):
    """
    Get order book at a particular time or sequence number.

    Parameters
    ----------
    at: (str, pd.datetime, int)
        str or datetime: get book at time
        int: get book at sequence number
        by default return the latest book
    exchange: str
    product: str

    Returns
    -------
    GdaxOrderBook
    """
    exchange = exchange or DEFAULT_EXCHANGE
    at = util.time_to_str(at, exchange)

    # get latest snapshot
    snapshot_df = get_closest_snapshot(at=at, exchange=exchange, product=product)

    # convert to book object
    book = sutil.df_to_book(snapshot_df)
    logger.debug('Got book: {}'.format(book.sequence))

    # get messages
    if isinstance(at, int):
        start = book.sequence
    else:
        # move start back by 1 sec to get any missing messages
        start = pd.to_datetime(book.time_str) - pd.offsets.Timedelta('1m')
        start = util.time_to_str(start, exchange)
    messages = get_messages(start=start, end=at, exchange=exchange, product=product)

    # apply messages
    for i, msg in enumerate(messages):
        # break if reached the end
        if isinstance(at, int) and book.sequence >= at:
            break
        elif isinstance(at, str) and book.time_str >= at:
            break
        else:
            book.process_message(msg)
    logger.debug('Book ready: {}'.format(book.sequence))
    return book


def get_closest_snapshot(at=None, exchange=None, product=None):
    """
    Get closest snapshot before timestamp of sequence base on `at`.

    Parameters
    ----------
    at: (str, pd.datetime, int)
        str or datetime: get book closet to time
        int: get book closest sequence number
        by default return the latest book
    exchange: str
    product: str

    Returns
    -------
    pd.DataFrame
    """
    exchange = exchange or DEFAULT_EXCHANGE
    product = product or DEFAULT_PRODUCT
    at = util.time_to_str(at, exchange)
    table_name = params.SNAPSHOT_TBL[exchange][product]

    # get closest snapshot before timestamp or sequence
    if at:
        _id = 'sequence' if isinstance(at, int) else 'received_time'

        sql = '''
        SELECT * FROM {table}
        WHERE {id} = (
            SELECT {id} FROM {table}
            WHERE {id} <= "{value}"
            ORDER BY {id} desc
            LIMIT 1
        )
        '''.format(table=table_name, id=_id, value=at)
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
    assert not df.empty, 'No snapshot found at {}'.format(at)
    return df


def get_messages(start=None, end=None, exchange=None, product=None):
    """
    Get messages by time or sequence number.

    Parameters
    ----------
    start: (str, pd.datetime, int)
        str or datetime: query by time
        int: query by sequence
    end: (str, pd.datetime, int)
        str or datetime: query by time
        int: query by sequence
    exchange: str
    product: str

    Returns
    -------
    generator
        yields array of results
    """
    assert type(start) == type(end), 'Start and end types do not match'
    exchange = exchange or DEFAULT_EXCHANGE
    product = product or DEFAULT_PRODUCT
    start = util.time_to_str(start, exchange)
    end = util.time_to_str(end, exchange)

    # create sql
    start_field = 'sequence' if isinstance(start, int) else 'time'
    end_field = 'sequence' if isinstance(end, int) else 'time'
    table_name = params.MSG_TBL[exchange][product]
    start_cond = '{field} >= "{value}"'.format(field=start_field, value=start) if start else ''
    end_cond = '{field} <= "{value}"'.format(field=end_field, value=end) if end else ''
    where_cond = 'WHERE' if start_cond or end_cond else ''
    and_cond = 'AND' if start_cond and end_cond else ''
    sql = '''
        SELECT * from {table} 
        {where_cond} {start_cond} {and_cond} {end_cond}
    '''.format(table=table_name,
               where_cond=where_cond,
               and_cond=and_cond,
               start_cond=start_cond,
               end_cond=end_cond)
    messages = sutil.xread_sql(sql)
    return messages


def store_dataset(name, start, end, exchange=None, product=None):
    """
    Store order book and messages in HDF5 for faster retrieval.

    Parameters
    ----------
    name: str
    start: pd.datetime
    end: pd.datetime
    exchange: str
    product: str

    Returns
    -------
    None
    """
    # get data
    book = get_book(at=start, exchange=exchange, product=product)
    start = start - pd.offsets.Timedelta('1m')
    msg_generator = get_messages(start=start, end=end, exchange=exchange, product=product)
    msg_list = [msg for msg in msg_generator]
    msg_df = pd.DataFrame(msg_list)

    # store
    fname = '../data/{}.hdf'.format(name)
    with pd.HDFStore(fname) as hdf:
        hdf['book'] = book.to_df(level_type=3)
        hdf['messages'] = msg_df


def get_dataset(name):
    """
    Get dataset from HDF store.

    Parameters
    ----------
    name: str
        e.g. '2017-11-10_00_to_2017-11-10_03'

    Returns
    -------
    namedtuple
        fields: [book, messages]
    """
    fname = '../data/{}.hdf'.format(name)
    book_df = pd.read_hdf(fname, key='book')
    book = sutil.df_to_book(book_df)
    messages = pd.read_hdf(fname, key='messages')
    dataset = Dataset(book=book, messages=messages)
    return dataset
