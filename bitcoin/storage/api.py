import pandas as pd
from collections import namedtuple

import bitcoin.params as pms
import bitcoin.storage.util as sutil
import bitcoin.logs.logger as lc
import bitcoin.util as util


logger = lc.config_logger('storage_api', level='DEBUG', file_handler=False)
Dataset = namedtuple('Dataset', ['books', 'messages'])


def get_book(at=None, exchange=None, product=None):
    """
    Get order book at a particular time or sequence number.

    Parameters
    ----------
    at: pd.datetime or int
        datetime: get book at time
        int: get book at sequence number
        by default return the latest book
    exchange: str
    product: str

    Returns
    -------
    GdaxOrderBook
    """
    exchange = exchange or pms.DEFAULT_EXCHANGE

    # get latest snapshot
    snapshot_df = get_closest_snapshot(at=at, exchange=exchange, product=product)

    # convert to book object
    book = sutil.df_to_book(snapshot_df)
    logger.debug('Got book: {}'.format(book.sequence))

    # get messages
    if isinstance(at, int):
        start = book.sequence
        end = at
    else:
        # move start back by 1 sec to get any missing messages
        start = pd.to_datetime(book.timestamp) - pd.offsets.Timedelta('1m')
        end = pd.to_datetime(at)
    messages = get_messages(start=start, end=end, exchange=exchange, product=product)

    # apply messages
    for msg in messages:
        # break if reached the end
        if isinstance(at, int) and book.sequence >= at:
            break
        elif isinstance(at, pd.datetime) and book.timestamp >= at:
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
    at: pd.datetime or int
        datetime: get book at time
        int: get book at sequence number
        by default return the latest book
    exchange: str
    product: str

    Returns
    -------
    pd.DataFrame
    """
    exchange = exchange or pms.DEFAULT_EXCHANGE
    product = product or pms.DEFAULT_PRODUCT
    at = util.time_to_str(at)
    table_name = pms.SNAPSHOT_TBL[exchange][product]

    # get closest snapshot before timestamp or sequence
    if at:
        _id = 'sequence' if isinstance(at, int) else 'received_time'
        value = at if isinstance(at, int) else '"{}"'.format(at)

        sql = '''
        SELECT * FROM {table}
        WHERE {id} = (
            SELECT {id} FROM {table}
            WHERE {id} <= {value}
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
    assert not df.empty, 'No snapshot found at {}'.format(at)
    return df


def get_messages(start=None, end=None, exchange=None, product=None):
    """
    Get messages by time or sequence number.

    Parameters
    ----------
    start: pd.datetime or int
        datetime: query by time
        int: query by sequence
    end: pd.datetime or int
        datetime: query by time
        int: query by sequence
    exchange: str
    product: str

    Returns
    -------
    generator
        yields array of dict with appropriate dtypes
    """
    assert type(start) == type(end), 'Start and end types do not match'

    start = util.time_to_str(start)
    end = util.time_to_str(end)
    # add quotes for time
    start_value = start if isinstance(start, int) else '"{}"'.format(start)
    end_value = end if isinstance(end, int) else '"{}"'.format(end)

    exchange = exchange or pms.DEFAULT_EXCHANGE
    product = product or pms.DEFAULT_PRODUCT
    table_name = pms.MSG_TBL[exchange][product]

    # create sql
    start_field = 'sequence' if isinstance(start, int) else 'time'
    end_field = 'sequence' if isinstance(end, int) else 'time'

    start_cond = '{field} >= {value}'.format(field=start_field, value=start_value) if start else ''
    end_cond = '{field} <= {value}'.format(field=end_field, value=end_value) if end else ''
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

    for msg in messages:
        msg = util.parse_message(msg)
        yield msg


def store_dataset(name, start, end, exchange=None, product=None):
    """
    Store order book and messages to disk for faster retrieval.

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
    exchange = exchange or pms.DEFAULT_EXCHANGE
    product = product or pms.DEFAULT_PRODUCT

    # get books
    table_name = pms.SNAPSHOT_TBL[exchange][product]
    sql = '''
    SELECT * FROM {table}
    WHERE received_time between "{start}" and "{end}"
    '''.format(table=table_name, start=util.time_to_str(start), end=util.time_to_str(end))
    books_df = pd.read_sql(sql, con=sutil.ENGINE)

    # get messages
    msg_generator = get_messages(start=start, end=end, exchange=exchange, product=product)
    msg_df = pd.DataFrame(list(msg_generator))

    # store
    fname = '../data/{}.hdf5'.format(name)
    books_df.to_hdf(fname, 'books')
    msg_df.to_hdf(fname, 'messages')


def get_dataset(name):
    """
    Get dataset from disk.

    Parameters
    ----------
    name: str
        e.g. '2017-11-10_00_to_2017-11-10_03'

    Returns
    -------
    namedtuple
        fields: [book, messages]
    """
    fname = '../data/{}.hdf5'.format(name)
    books_df = pd.read_hdf(fname, 'books')
    msg_df = pd.read_hdf(fname, 'messages')
    msg_lst = msg_df.to_dict(orient='records')
    dataset = Dataset(books=books_df, messages=msg_lst)

    num_books = len(books_df['sequence'].unique())
    num_msgs = len(msg_lst)
    logger.info('Got {:,} messages and {} books'.format(num_msgs, num_books))
    return dataset
