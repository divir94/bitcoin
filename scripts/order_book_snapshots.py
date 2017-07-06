# general
import json
import pandas as pd
from pprint import pprint
import time

# websocket
import websocket
import thread

# db
import MySQLdb
from sqlalchemy import create_engine

# http
import requests


TABLE_NAME = 'OrderBookSnapshots'

def get_snapshot_json():
    r = requests.get('https://api.gdax.com/products/BTC-USD/book?level=3')
    response = json.loads(r.content)
    return response


def snapshot_to_df(snapshot_json):
    bids = pd.DataFrame(snapshot_json['bids'])
    asks = pd.DataFrame(snapshot_json['asks'])
    bids['side'] = 'bid'
    asks['side'] = 'ask'
    df = pd.concat([bids, asks])
    df['sequence'] = snapshot_json['sequence']
    df.sequence = df.sequence.astype(str)
    df.columns = ['price', 'size', 'order_id', 'side', 'sequence']
    return df


def get_sqlalchemy_engine():
    creds = json.load(open('../mysql_creds.json', 'rb'))
    engine_name = 'mysql://{user}:{pwd}@{host}/{db}'.format(**creds)
    engine = create_engine(engine_name, echo=False)
    return engine


def run(sleep=60):
    engine = get_sqlalchemy_engine()
    while True:
        print '=' * 20

        try:
            snapshot_json = get_snapshot_json()
            print 'Got response from server'
        except Exception as e:
            print 'Failed to get response from server'
            print e
            continue

        try:
            snapshot_df = snapshot_to_df(snapshot_json)
            print 'Converted json to df'
        except Exception as e:
            print 'Failed to convert json to df'
            print e
            continue

        try:
            snapshot_df.to_sql(name=TABLE_NAME, con=engine, if_exists='append', index=False)
            print 'Written to df'
            print 'Sequence: {}'.format(snapshot_json['sequence'])
        except Exception as e:
            print 'Failed to write to db'
            print e

        print 'Sleeping ...'
        time.sleep(sleep)


if __name__ == '__main__':
    run()