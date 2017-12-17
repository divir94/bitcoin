import json
import logging
import pandas as pd
from sqlalchemy import create_engine

import bitcoin.util as util
import bitcoin.order_book.gdax_order_book as ob


logger = logging.getLogger('db_util')


def get_sqlalchemy_engine():
    """
    Get engine connection by reading credentials.

    Returns
    -------
    engine
    """
    logger.info('Getting DB engine instance')
    root_path = util.get_project_root()
    creds = json.load(open('{}/credentials/mysql.json'.format(root_path), 'rb'))
    engine_name = 'mysql://{user}:{pwd}@{host}/{db}'.format(**creds)
    engine = create_engine(engine_name, pool_recycle=1800, echo=False)
    return engine


ENGINE = get_sqlalchemy_engine()


def store_df(df, tbl_name):
    """
    Store dataframe to DB.

    Parameters
    ----------
    df
    tbl_name

    Returns
    -------
    None
    """
    try:
        df.to_sql(name=tbl_name, con=ENGINE, if_exists='append', index=False)
        logger.debug('Stored {} rows in {}'.format(len(df), tbl_name))
    except Exception as e:
        logger.error('Failed to store in {}'.format(tbl_name))
        logger.error(e)
    return


def df_to_book(df):
    """
    Convert DataFrame to order book.

    Parameters
    ----------
    df: pd.DataFrame
        index: ordinal
        columns: [sequence, received_time, side, price, size, order_id]

    Returns
    -------
    GdaxOrderBook
    """
    # get sequence
    sequences = df['sequence'].unique()
    assert len(sequences) == 1
    sequence = sequences[0]

    bids = df[df['side'] == 'bid']
    asks = df[df['side'] == 'ask']
    columns = ['price', 'size', 'order_id']
    bids = bids[columns].values
    asks = asks[columns].values
    timestamp = pd.to_datetime(df['received_time'].unique()[0])
    book = ob.GdaxOrderBook(sequence, bids=bids, asks=asks, timestamp=timestamp)
    return book


def book_to_df(data, timestamp):
    """
    Convert order book data to DataFrame.

    Parameters
    ----------
    data: dict
        keys: [sequence, bids and asks]
    timestamp: pd.datetime

    Returns
    -------
    pd.DataFrame
        index: ordinal
        columns: [sequence, received_time, side, price, size, order_id]
    """
    # combine bids and asks
    columns = ['price', 'size', 'order_id']
    bids = pd.DataFrame(data['bids'], columns=columns)
    asks = pd.DataFrame(data['asks'], columns=columns)
    bids['side'] = 'bid'
    asks['side'] = 'ask'
    df = pd.concat([bids, asks])

    # add sequence and timestamp
    df['sequence'] = data['sequence']
    df['received_time'] = util.time_to_str(timestamp)
    return df


def xread_sql(sql, chunksize=100000):
    """
    Reads SQL from DB in a stream like way.

    Parameters
    ----------
    sql
    chunksize

    Returns
    -------
    generator
        yields dict with nans removed
    """
    offset = 0

    while True:
        if offset != 0 and offset < chunksize:
            # got all available rows
            raise StopIteration

        query = '{sql} LIMIT {offset}, {chunksize}'.format(sql=sql, offset=offset, chunksize=chunksize)
        logger.debug(query)
        result = ENGINE.execute(query)

        if not result.rowcount:
            raise StopIteration

        columns = result.keys()
        for row in result:
            row_dict = dict(zip(columns, row))
            offset += 1
            yield row_dict
