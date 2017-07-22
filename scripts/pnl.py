# general
import json
import pandas as pd
import numpy as np
from pprint import pprint
from time import time


# exchanges
import gdax
import bitstamp.client


gdax_client = gdax.AuthenticatedClient(key="8e944b216e435691b071dd3f0f62caa9",
                                       b64secret="uSysPzywwj3LUlPuuNkKEHRpxhd4A+K/JGwbEfrc28nkd4c6qMLwr1IbRsw1GB1BKQzVXoj88n/WjMvIAvv/MA==",
                                       passphrase="ltg10b8unuo")

bitstamp_client = bitstamp.client.Trading(username='752298',
                                          key='otqVIe8cjvxONsfzb7rz34jMJRPlnx4G',
                                          secret='49VrY4K9sJp3aBzHq7l2fyBBnBcmNeYA')


def get_gdax_fills():
    # get df
    fills = gdax_client.get_fills(limit=100)
    fills = pd.DataFrame.from_records(fills[0], index='created_at')
    fills.index = pd.DatetimeIndex(fills.index)
    fills.index.name = 'datetime'

    # type cast
    numeric_cols = ['price', 'size', 'fee']
    fills[numeric_cols] = fills[numeric_cols].astype(float)

    # filter
    fills.loc[fills['side'] == 'sell', 'size'] *= -1
    fills = fills[fills.settled][['price', 'size', 'fee']]

    # add value
    fills['value'] = -fills['size'] * fills['price']
    fills['total'] = fills['value'] - fills['fee']

    order = ['price', 'size', 'value', 'fee', 'total']
    return fills[order]


def get_bitstamp_fills():
    # get df
    fills = bitstamp_client.user_transactions()
    fills = pd.DataFrame.from_records(fills, index='datetime')
    fills.index = pd.DatetimeIndex(fills.index)
    fills = fills.astype(float)

    # filter
    ignore_orders = {60752232.0}
    fills = fills[(fills.type == 2) & (~fills.order_id.isin(ignore_orders))]

    # rename
    fills = fills[['btc', 'btc_usd', 'fee', 'usd']]
    columns = {
        'btc_usd': 'price',
        'btc': 'size',
        'usd': 'value',
        'fee': 'fee'
    }
    fills = fills.rename(columns=columns)

    # total
    fills['total'] = fills['value'] - fills['fee']

    order = ['price', 'size', 'value', 'fee', 'total']
    return fills[order]


def plot_all_fills():
    import cufflinks as cf
    cf.go_offline()

    gdax_fills = get_gdax_fills()
    bitstamp_fills = get_bitstamp_fills()

    gdax_pnl = gdax_fills.total.groupby(gdax_fills.index).sum()
    bitstamp_pnl = bitstamp_fills.total.groupby(bitstamp_fills.index).sum()
    pnl = pd.DataFrame({
        'gdax': gdax_pnl,
        'bitstamp': bitstamp_pnl
    })
    pnl.reset_index(drop=True).iplot(kind='bar')
    return


def get_all_fills():
    gdax_fills = get_gdax_fills()
    bitstamp_fills = get_bitstamp_fills()

    gdax_fills['exchange'] = 'gdax'
    bitstamp_fills['exchange'] = 'bitstamp'
    pnl = pd.concat([gdax_fills, bitstamp_fills]).sort_index(ascending=False)
    return pnl


def get_hourly_pnl(fills):
    group = fills.total.groupby(pd.TimeGrouper(freq='60Min'))
    hourly_pnl = group.aggregate(np.sum).dropna().sort_index(ascending=False)
    return hourly_pnl


def get_balances():
    # gdax balances
    gdax_balance = gdax_client.get_accounts()
    gdax_balance = pd.DataFrame.from_records(gdax_balance, index='currency')
    numeric_cols = ['balance', 'available', 'hold']
    gdax_balance[numeric_cols] = gdax_balance[numeric_cols].astype('float')

    # bitstamp balances
    bitstamp_balance = bitstamp_client.account_balance()
    bitstamp_balance = pd.Series(bitstamp_balance).astype(float)

    # get best bid to sell coins
    gdax_bid = float(gdax_client.get_product_order_book(product_id='BTC-USD')['bids'][0][0])
    bitstamp_bid = float(bitstamp_client.ticker()['bid'])

    # combine
    balances = pd.DataFrame({'gdax':
        {
            'usd_balance': gdax_balance.loc['USD', 'balance'],
            'btc_balance': gdax_balance.loc['BTC', 'balance'],
            'bid_price': gdax_bid
        },
        'bitstamp': {
            'usd_balance': bitstamp_balance['usd_balance'],
            'btc_balance': bitstamp_balance['btc_balance'],
            'bid_price': bitstamp_bid
        }}).T

    # add totals
    balances['btc_value'] = balances['bid_price'] * balances['btc_balance']
    balances['total'] = balances['usd_balance'] + balances['btc_value']

    # reorder
    balances = balances.T
    idx_order = ['btc_balance', 'bid_price', 'btc_value', 'usd_balance', 'total']
    balances = balances.reindex(idx_order)[['gdax', 'bitstamp']]
    balances['total'] = balances.sum(axis=1)

    now = pd.datetime.utcnow().strftime('%b %d %Y, %I:%M %p UTC')
    print '\nBalance as of {}'.format(now)
    return balances


def run_report():
    plot_all_fills()

    fills = get_all_fills()
    print '\nLast 20 fills'
    print fills.head(20)

    hourly_pnl = get_hourly_pnl(fills)
    print '\nHourly PnL'
    print hourly_pnl

    balances = get_balances()
    print balances
    return pnl


if __name__ == '__main__':
    run_report()