import boto3
import requests
import json
import time
import pandas as pd
import pickle

from pprint import pprint


def send_email(orders, spread):
    sender = "divir94@gmail.com"
    recipients = ["divir94@gmail.com", "vidur94@gmail.com"]
    awsregion = "us-east-1"
    subject = "Bitcoin spread: ${}!".format(spread)
    textbody = str(orders)
    charset = "UTF-8"

    client = boto3.client('ses', region_name=awsregion)

    try:
        response = client.send_email(
            Destination={
                'ToAddresses': recipients,
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': charset,
                        'Data': textbody,
                    },
                },
                'Subject': {
                    'Charset': charset,
                    'Data': subject,
                },
            },
            Source=sender,
        )
    except Exception as e:
        print "Error: ", e
    else:
        print "Email sent!"
    return


def get_orders():
    endpoints = {
        'gdax': {
            'url': 'https://api.gdax.com/products/BTC-USD/book?level=2',
            'id': 'sequence'
        },
        'bitstamp': {
            'url': 'https://www.bitstamp.net/api/order_book/',
            'id': 'timestamp'
        }
    }

    orders = {}
    for name, obj in endpoints.iteritems():
        r = requests.get(obj['url'])
        book = json.loads(r.content)
        best_bid = book['bids'][0][0]
        best_ask = book['asks'][0][0]
        orders[name] = {'bid': best_bid, 'ask': best_ask}
    return orders


def calculate_spread(orders):
    orders = pd.DataFrame(orders)
    spread = abs(float(orders['bitstamp']['bid']) - float(orders['gdax']['bid']))
    return spread


def run(threshold=20):
    spread_hist = {}
    previous_spread = None

    while True:
        orders = get_orders()
        spread = calculate_spread(orders)

        timestamp = time.strftime('%Y-%m-%d, %I:%M %p')
        spread_hist[timestamp] = {
            'orders': orders,
            'spread': spread
        }

        print timestamp
        print 'Spread: ${}'.format(spread)
        pprint(orders)
        print '=' * 20

        if (spread > threshold) and (previous_spread < threshold):
            send_email(orders, spread)

        previous_spread = spread

        with open('spreads.pickle', 'wb') as handle:
            pickle.dump(spread_hist, handle, protocol=pickle.HIGHEST_PROTOCOL)
        time.sleep(60)


if __name__ == '__main__':
    run()