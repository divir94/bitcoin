import pandas as pd

import bitcoin.storage.util as sutil
import bitcoin.order_book as ob
import bitcoin.gdax.params as params


def get_gdax_book(sequence):
    snapshot_df, snapshot_sequence = get_closest_snapshot(sequence)
    book = get_book_from_df(snapshot_df, snapshot_sequence)
    messages = get_messages_between(snapshot_sequence, sequence)
    return book


def get_closest_snapshot(sequence):
    # get closest sequence
    sql = '''
    SELECT sequence FROM GdaxBookLevel3
    WHERE sequence <= {}
    ORDER BY sequence desc
    LIMIT 1
    '''.format(sequence)
    df = pd.read_sql(sql, con=sutil.ENGINE)
    closest_sequence = df.values.item()

    # get snapshot at closest sequence
    sql = '''
    SELECT * FROM GdaxBookLevel3
    WHERE sequence = {}
    '''.format(closest_sequence)
    df = pd.read_sql(sql, con=sutil.ENGINE)
    return df, closest_sequence


def get_book_from_df(df, sequence):
    bids = df[df['side'] == 'bid']
    asks = df[df['side'] == 'ask']
    columns = ['price', 'size', 'order_id']
    bids = bids[columns].values
    asks = asks[columns].values
    book = ob.OrderBook(sequence, bids, asks)
    return book


def get_messages_between(start_sequence, end_sequence):
    sql = '''
    SELECT * FROM {}
    WHERE sequence >= {}
    AND sequence <= {}
    '''.format(params.BTC_MSG_TBL, start_sequence, end_sequence)
    msgs = pd.read_sql(sql, con=sutil.ENGINE)
    return msgs


def apply_messages(snapshot, messages):
    pass
