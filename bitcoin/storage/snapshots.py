import json
import pandas as pd
import time
import requests
import logging

import bitcoin.logs.logger as lc
import bitcoin.storage.util as st_util


GDAX_URL = 'https://api.gdax.com/products/BTC-USD/book?level=2'
BITSTAMP_URL = 'https://www.bitstamp.net/api/order_book/'
TIMEOUT = 5
ROWS_PER_CALL = 50  # number of top bids and asks to store per call

GDAX_TBL_NAME = 'GdaxSnapshots'
BITSTAMP_TBL_NAME = 'BitstampSnapshots'
SPREAD_TBL_NAME = 'Spreads'

logger = lc.config_logger('snapshots')
logger.setLevel(logging.ERROR)


################
# HTTP Requests
################

def get_snapshot_json(url):
    """returns None if there is any error"""
    now = pd.datetime.now()
    time_msg = '=' * 20 + '\n{}\n'.format(now)
    failed_msg = time_msg + 'Get request failed for: {}\n\n'.format(url)

    try:
        response = requests.get(url, timeout=TIMEOUT)
        content_msg = 'Content: {}'.format(response.content)
    except requests.exceptions.RequestException as e:
        logger.error(failed_msg)
        logger.error(e)
        return

    try:
        snapshot = json.loads(response.content)
    except ValueError, e:
        logger.error(failed_msg)
        logger.error(e)
        logger.error(content_msg)
        return

    if 'bids' not in snapshot and 'asks' not in snapshot:
        logger.error(failed_msg)
        logger.error('Invalid json format {}'.format(content_msg))
        return
    return snapshot


#################
# Data Wrangling
#################

def snapshot_to_df(snapshot, timestamp, exchange_name):
    # combine bids and asks
    bids = pd.DataFrame(snapshot['bids'])[:ROWS_PER_CALL]
    asks = pd.DataFrame(snapshot['asks'])[:ROWS_PER_CALL]
    bids['side'] = 'bid'
    asks['side'] = 'ask'
    df = pd.concat([bids, asks])

    # remove 'num_orders' column from gdax
    if exchange_name == 'gdax':
        df.drop(2, axis=1, inplace=True)
    df.columns = ['price', 'size', 'side']

    # add ids
    _id = 'sequence' if exchange_name == 'gdax' else 'timestamp'
    df['id'] = snapshot[_id]
    df['time'] = timestamp

    # modify columns
    df.id = df.id.astype(str)
    df = df[['time', 'price', 'side', 'size', 'id']]
    return df


def snapshots_to_spread_df(gdax_snapshot, bitstamp_snapshot, timestamp):
    """snapshots can be None"""
    # gdax best bid and ask
    if gdax_snapshot:
        gdax_id = gdax_snapshot['sequence']
        bids = gdax_snapshot['bids']
        asks = gdax_snapshot['asks']
        gdax_bid, gdax_ask = bids[0][0], asks[0][0]
    else:
        gdax_id, gdax_bid, gdax_ask = (None,) * 3

    # bitstamp best bid and ask
    if bitstamp_snapshot:
        bitstamp_id = bitstamp_snapshot['timestamp']
        bids = bitstamp_snapshot['bids']
        asks = bitstamp_snapshot['asks']
        bitstamp_bid, bitstamp_ask = bids[0][0], asks[0][0]
    else:
        bitstamp_id, bitstamp_bid, bitstamp_ask = (None,) * 3

    df = pd.DataFrame([{
        'time': timestamp,
        'gdax_bid': gdax_bid,
        'gdax_ask': gdax_ask,
        'gdax_id': gdax_id,
        'bitstamp_bid': bitstamp_bid,
        'bitstamp_ask': bitstamp_ask,
        'bitstamp_id': bitstamp_bid,
    }])
    return df


######
# Run
######

def run(sleep=1):
    while True:
        timestamp = pd.datetime.now()

        # store gdax snapshot
        gdax_snapshot = get_snapshot_json(GDAX_URL)
        if gdax_snapshot:
            gdax_df = snapshot_to_df(gdax_snapshot, timestamp, exchange_name='gdax')
            st_util.store_df(gdax_df, GDAX_TBL_NAME)

        # store bitstamp snapshot
        bitstamp_snapshot = get_snapshot_json(BITSTAMP_URL)
        if bitstamp_snapshot:
            bitstamp_df = snapshot_to_df(bitstamp_snapshot, timestamp, exchange_name='bitstamp')
            st_util.store_df(bitstamp_df, BITSTAMP_TBL_NAME)

        # store spread
        spread = snapshots_to_spread_df(gdax_snapshot, bitstamp_snapshot, timestamp)
        st_util.store_df(spread, SPREAD_TBL_NAME)

        time.sleep(sleep)


if __name__ == '__main__':
    run()
