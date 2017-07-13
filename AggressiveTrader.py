import gdax
import bitstamp.client
import time
import datetime
import random
import pickle
import sys
import traceback
import signal

REQUIRED_BTC = 0.4
MIN_BALANCING_SPREAD = 0.2
MIN_UNBALANCING_SPREAD = 8.2
LIMIT_ORDER_SIZE = 0.01
TIME_BETWEEN_REBALANCES = 2
bitstamp_client = bitstamp.client.Trading(username='752298', key='zVkOwtFZppdcdcJoltubTQflC6uKiE7t', secret='lFP5T96pkoK4orvGX7K92gsZNhnpcflV')
gdax_client = gdax.AuthenticatedClient(key="8e944b216e435691b071dd3f0f62caa9", b64secret="uSysPzywwj3LUlPuuNkKEHRpxhd4A+K/JGwbEfrc28nkd4c6qMLwr1IbRsw1GB1BKQzVXoj88n/WjMvIAvv/MA==", passphrase="ltg10b8unuo")

def timeout_handler(signum, frame):
    gdax_client.cancel_all(data={"product": "BTC-USD"})
    print("Bot got stuck!")
    raise Exception("Bot got stuck!")

# TODO(vidurj) carefully think through whether we want to use balance or available in these two funcs
def get_gdax_available_balances():
    data = {}
    accounts = gdax_client.get_accounts()
    for account in accounts:
        data[(account["currency"], "available")] = float(account["available"])
        data[(account["currency"], "balance")] = float(account["balance"])
    return data

def get_bitstamp_available_balances():
    balances = bitstamp_client.account_balance()
    data = {
        ("USD", "available"): float(balances["usd_available"]),
        ("USD", "balance"): float(balances["usd_balance"]),
        ("BTC", "available"): float(balances["btc_available"]),
        ("BTC", "balance"): float(balances["btc_balance"])
    }
    return data

def buy_market_order(client, amount, base="btc", quote="usd"):
    """
    Order to buy amount of bitcoins for specified price.
    """
    data = {'amount': amount}
    url = client._construct_url("buy/market/", base, quote)
    return client._post(url, data=data, return_json=True, version=2)

def sell_market_order(client, amount, base="btc", quote="usd"):
    """
    Order to buy amount of bitcoins for specified price.
    """
    data = {'amount': amount}
    url = client._construct_url("sell/market/", base, quote)
    return client._post(url, data=data, return_json=True, version=2)

def get_price_estimate_for_usd(book, desired_vol, desired_orders=4):
    for item in book["asks"]:
        vol = float(item[1])
        desired_orders -= 1
        desired_vol -= vol
        if desired_vol <= 0 and desired_orders <= 0:
            price = float(item[0])
            return price
    return 10 ** 8

def get_price_estimate_for_btc(book, desired_vol, desired_orders=4):
    for item in book["bids"]:
        vol = float(item[1])
        desired_orders -= 1
        desired_vol -= vol
        if desired_vol <= 0 and desired_orders <= 0:
            price = float(item[0])
            return price
    return - 10 ** 8

def cancel_gdax_buy(outstanding_orders):
    if "buy" in outstanding_orders:
        resp = gdax_client.cancel_order(outstanding_orders["buy"]["id"])
        print("Cancelled buy", resp)

def cancel_gdax_sell(outstanding_orders):
    if "sell" in outstanding_orders:
        resp = gdax_client.cancel_order(outstanding_orders["sell"]["id"])
        print("Cancelled sell", resp)

def get_outstanding_orders_gdax():
    raw_orders = gdax_client.get_orders()
    assert len(raw_orders) == 1
    orders = raw_orders[0]
    outstanding_orders = {}
    print(" . " * 20)
    for order in orders:
        print(" " * 29 + "*")
        print(order)
        side = order["side"]
        assert side == "buy" or side == "sell"
        our_format = {
            "price": float(order["price"]),
            "id": str(order["id"]),
            "filled_size": float(order["filled_size"]),
            "created_at": order["created_at"]
        }
        if side in outstanding_orders:
            competing_order = outstanding_orders[side]
            if order["created_at"] > competing_order["created_at"]:
                resp = gdax_client.cancel_order(competing_order["id"])
                print("Cancelled stale order", resp)
                outstanding_orders[side] = our_format
            else:
                resp = gdax_client.cancel_order(order["id"])
                print("Cancelled stale order", resp)
        else:
            outstanding_orders[side] = our_format
    print(" . " * 20)
    return outstanding_orders


def buy_limit_order_with_retry(price):
    for _ in range(3):
        buy_resp = gdax_client.buy(type="limit", size=str(LIMIT_ORDER_SIZE), price=str(price),
                                   time_in_force="GTT", cancel_after="min", product_id='BTC-USD',
                                   post_only="True")
        if "message" in buy_resp and buy_resp["message"] == "Insufficient funds":
            time.sleep(0.2)
        elif "id" in buy_resp and "price" in buy_resp:
            return buy_resp
        else:
            raise Exception("Unable to place buy order.\n" + str(buy_resp))
    raise Exception("Insufficient funds.\n" + str(buy_resp))


def sell_limit_order_with_retry(price):
    for _ in range(3):
        sell_resp = gdax_client.sell(type="limit", size=str(LIMIT_ORDER_SIZE), price=str(price),
                                     time_in_force="GTT", cancel_after="min", product_id='BTC-USD',
                                     post_only="True")
        if "message" in sell_resp and sell_resp["message"] == "Insufficient funds":
            time.sleep(0.2)
        elif "id" in sell_resp and "price" in sell_resp:
            return sell_resp
        else:
            raise Exception("Unable to place buy order.\n" + str(sell_resp))
    raise Exception("Insufficient funds.\n" + str(sell_resp))


def rebalance():
    """
    :param data: Must contain the balances on both exchanges. These are maintained by
    """
    gdax_balances = get_gdax_available_balances()
    bitstamp_balances = get_bitstamp_available_balances()
    total_btc = bitstamp_balances[("BTC", "balance")] + gdax_balances[("BTC", "balance")]
    if total_btc >= REQUIRED_BTC + 0.01:
        size = round(total_btc - REQUIRED_BTC, 2)
        print("Executing market order on bitstamp. Selling ", size)
        sell_market_order(bitstamp_client, size)
        # Should cancel all limit orders and wait for a cycle to get accurate available balance info from
        # bitstamp
        gdax_client.cancel_all(data={"product": "BTC-USD"})
    elif REQUIRED_BTC >= total_btc + 0.01:
        size = round(REQUIRED_BTC - total_btc, 2)
        print("Executing market order on bitstamp. Buying ", size)
        buy_market_order(bitstamp_client, size)
        # Should cancel all limit orders and wait for a cycle to get accurate available balance info from
        # bitstamp
        gdax_client.cancel_all(data={"product": "BTC-USD"})
    else:
        gdax_book = gdax_client.get_product_order_book('BTC-USD', level=1)
        best_gdax_bid = float(gdax_book["bids"][0][0])
        best_gdax_ask = float(gdax_book["asks"][0][0])
        # Cannot place an ask at or below the best bid. Doesn't make sense to place ask $0.05 better than
        # best ask
        min_ask = max(best_gdax_bid + 0.01, best_gdax_ask - 0.05)
        # Cannot place a bid at or above the best ask. Doesn't make sense to place bid $0.05 better than
        # best bid
        max_bid = min(best_gdax_ask - 0.01, best_gdax_bid + 0.05)
        bitstamp_book = bitstamp_client.order_book()
        #TODO(vidurj) change this 0.5 if the LIMIT_ORDER_SIZE increases by much
        bitstamp_sell_price = get_price_estimate_for_btc(bitstamp_book, 1.5)
        bitstamp_buy_price = get_price_estimate_for_usd(bitstamp_book, 1.5)
        # Valid sell price on gdax is bitstamp buy + fees + spread
        if gdax_balances[("BTC", "balance")] - REQUIRED_BTC / 2 < 0.05:
            buy_spread = MIN_BALANCING_SPREAD
        else:
            buy_spread = MIN_UNBALANCING_SPREAD
        print("buy spread is", buy_spread)
        if REQUIRED_BTC / 2 - gdax_balances[("BTC", "balance")] < 0.05:
            sell_spread = MIN_BALANCING_SPREAD
        else:
            sell_spread = MIN_UNBALANCING_SPREAD
        print("sell spread is", sell_spread)
        gdax_profitable_sell_price = \
            round(bitstamp_buy_price + 0.0025 * bitstamp_buy_price + sell_spread, 2)
        # Valid buy price on gdax is bitstamp sell - fees - spread
        gdax_profitable_buy_price = \
            round(bitstamp_sell_price - 0.0025 * bitstamp_sell_price - buy_spread, 2)
        # TODO(vidurj) Adjust these prices so that we are within 7 cents of the closest order behind us
        limit_sell_price = max(gdax_profitable_sell_price, min_ask)
        limit_buy_price = min(gdax_profitable_buy_price, max_bid)
        # To support an ask on gdax, bitstamp must have the cash to buy an equivalent number of BTC
        bitstamp_supports_ask_on_gdax = \
            bitstamp_balances[("USD", "available")] >= LIMIT_ORDER_SIZE * bitstamp_buy_price * 1.0025
        # To support a bid on gdax, bit stamp must have an equivalent number of BTC to sell
        bitstamp_supports_bid_on_gdax = bitstamp_balances[("BTC", "available")] >= LIMIT_ORDER_SIZE
        outstanding_orders = get_outstanding_orders_gdax()
        if not bitstamp_supports_bid_on_gdax:
            cancel_gdax_buy(outstanding_orders)
        else:
            if "buy" in outstanding_orders:
                if abs(outstanding_orders["buy"]["price"] - limit_buy_price) > 0.2 or outstanding_orders["buy"]["filled_size"] > 0.05:
                    cancel_gdax_buy(outstanding_orders)
                    buy_limit_order_with_retry(limit_buy_price)
            elif gdax_balances[("USD", "balance")] >= LIMIT_ORDER_SIZE * limit_buy_price:
                buy_limit_order_with_retry(limit_buy_price)

        if not bitstamp_supports_ask_on_gdax:
            cancel_gdax_sell(outstanding_orders)
        else:
            if "sell" in outstanding_orders:
                if abs(outstanding_orders["sell"]["price"] - limit_sell_price) > 0.2 or outstanding_orders["sell"]["filled_size"] > 0.05:
                    cancel_gdax_sell(outstanding_orders)
                    sell_limit_order_with_retry(limit_sell_price)
            elif gdax_balances[("BTC", "balance")] >= LIMIT_ORDER_SIZE:
                sell_limit_order_with_retry(limit_sell_price)

signal.signal(signal.SIGALRM, timeout_handler)
gdax_client.cancel_all(data={"product": "BTC-USD"})
while True:
    try:
        start_time = time.time()
        signal.alarm(30)
        rebalance()
        signal.alarm(0)
        elapsed_time = time.time() - start_time
        print("elapsed time", elapsed_time)
        time.sleep(1)
    except:
        try:
            gdax_client.cancel_all(data={"product": "BTC-USD"})
            print("Unexpected error:", sys.exc_info()[0])
            print(traceback.format_exc())
            signal.alarm(0)
            time.sleep(30)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            print(traceback.format_exc())
            time.sleep(120)
            gdax_client.cancel_all(data={"product": "BTC-USD"})
    print("-" * 60)

