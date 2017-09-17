import json
import logging
import pandas as pd
from sqlalchemy import create_engine

import bitcoin.util as util

logger = logging.getLogger('db_util')


def get_sqlalchemy_engine():
    logger.info('Getting DB engine instance')
    root_path = util.get_project_root()
    creds = json.load(open('{}/credentials/mysql.json'.format(root_path), 'rb'))
    engine_name = 'mysql://{user}:{pwd}@{host}/{db}'.format(**creds)
    engine = create_engine(engine_name, pool_recycle=1800, echo=False)
    return engine

ENGINE = get_sqlalchemy_engine()


def store_df(df, tbl_name):
    try:
        df.to_sql(name=tbl_name, con=ENGINE, if_exists='append', index=False)
        logger.debug('Stored {} rows in {}'.format(len(df), tbl_name))
    except Exception as e:
        logger.error('Failed to store in {}'.format(tbl_name))
        logger.error(e)
    return


def gdax_book_to_df(data, timestamp):
    # combine bids and asks
    columns = ['price', 'size', 'order_id']
    bids = pd.DataFrame(data['bids'], columns=columns)
    asks = pd.DataFrame(data['asks'], columns=columns)
    bids['side'] = 'bid'
    asks['side'] = 'ask'
    df = pd.concat([bids, asks])

    # add sequence and timestamp
    df['sequence'] = data['sequence']
    df['received_time'] = timestamp
    return df