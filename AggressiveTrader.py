import gdax
import bitstamp.client
import time
import datetime
import random
import pickle
import sys
import traceback
import signal
from multiprocessing.pool import ThreadPool

POOL = ThreadPool(processes=5)
TEST = False
REQUIRED_BTC = 0.4
MIN_BALANCING_SPREAD = 0.2
MIN_UNBALANCING_SPREAD = 6.0
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

def async_cancel_gdax_buy(outstanding_orders):
    if "buy" in outstanding_orders:
        return [POOL.apply_async(cancel_gdax_order, (outstanding_orders["buy"]["id"],))]
    else:
        return []

def async_cancel_gdax_sell(outstanding_orders):
    if "sell" in outstanding_orders:
        return [POOL.apply_async(cancel_gdax_order, (outstanding_orders["sell"]["id"],))]
    else:
        return []

def cancel_gdax_order(order_id):
    return gdax_client.cancel_order(order_id)

def get_outstanding_orders_gdax():
    raw_orders = gdax_client.get_orders()
    assert len(raw_orders) == 1
    orders = raw_orders[0]
    outstanding_orders = {}
    print(" . " * 20)
    cancellations = []
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
                # resp = POOL.apply_async(cancel_gdax_order, (competing_order["id"],))
                # cancellations.append(resp)
                outstanding_orders[side] = our_format
            else:
                # resp = POOL.apply_async(cancel_gdax_order, (order["id"],))
                # cancellations.append(resp)
                pass
        else:
            outstanding_orders[side] = our_format
    print(" . " * 20)
    return outstanding_orders, cancellations

def gdax_limit_order(price, side):
    if side == "buy":
        resp = gdax_client.buy(type="limit", size=str(LIMIT_ORDER_SIZE), price=str(price),
                                     time_in_force="GTT", cancel_after="min", product_id='BTC-USD',
                                     post_only="True")
    elif side == "sell":
        resp = gdax_client.sell(type="limit", size=str(LIMIT_ORDER_SIZE), price=str(price),
                                time_in_force="GTT", cancel_after="min", product_id='BTC-USD',
                                post_only="True")
    else:
        raise Exception("Unknown  " + str(side))
    if "message" not in resp or resp["message"] == "Insufficient funds":
        return resp
    elif "id" in resp and "price" in resp:
        return resp
    else:
        raise Exception("Unable to place order.\n" + str(resp))


def cancel_all_gdax():
    return gdax_client.cancel_all(data={"product": "BTC-USD"})

def get_order_book_gdax():
    return gdax_client.get_product_order_book('BTC-USD', level=1)

def get_balances():
    gdax_balances_async = POOL.apply_async(get_gdax_available_balances)
    bitstamp_balances_async = POOL.apply_async(get_bitstamp_available_balances)
    return gdax_balances_async.get(), bitstamp_balances_async.get()

def compute_buy_and_sell_spreads(balances):
    if balances[("BTC", "balance")] - REQUIRED_BTC / 2 < 0.05:
        buy_spread = MIN_BALANCING_SPREAD
    else:
        buy_spread = MIN_UNBALANCING_SPREAD
    if REQUIRED_BTC / 2 - balances[("BTC", "balance")] < 0.05:
        sell_spread = MIN_BALANCING_SPREAD
    else:
        sell_spread = MIN_UNBALANCING_SPREAD
    return buy_spread, sell_spread

def compute_max_bid_and_min_ask(book):
    best_bid = float(book["bids"][0][0])
    best_ask = float(book["asks"][0][0])
    # Cannot place an ask at or below the best bid. Doesn't make sense to place ask $0.05 better than
    # best ask
    min_ask = max(best_bid + 0.01, best_ask - 0.05)
    # Cannot place a bid at or above the best ask. Doesn't make sense to place bid $0.05 better than
    # best bid
    max_bid = min(best_ask - 0.01, best_bid + 0.05)
    return max_bid, min_ask

def rebalance():
    """
    :param data: Must contain the balances on both exchanges. These are maintained by
    """
    if TEST:
        print("THIS IS A TEST.\n" * 5)

    async_gdax_order_book = POOL.apply_async(get_order_book_gdax)
    async_bitstamp_order_book = POOL.apply_async(bitstamp_client.order_book)
    async_outstanding_orders = POOL.apply_async(get_outstanding_orders_gdax)
    gdax_balances, bitstamp_balances = get_balances()
    total_btc = bitstamp_balances[("BTC", "balance")] + gdax_balances[("BTC", "balance")]
    if total_btc >= REQUIRED_BTC + 0.01:
        size = round(total_btc - REQUIRED_BTC, 2)
        print("Executing market order on bitstamp. Selling ", size)
        async_cancel = POOL.apply_async(cancel_all_gdax)
        if not TEST:
            sell_market_order(bitstamp_client, size)
            async_cancel.wait(timeout=5)
            # letting balances update
            time.sleep(1.5)
        else:
            print("TEST Mode " * 10)
    elif REQUIRED_BTC >= total_btc + 0.01:
        size = round(REQUIRED_BTC - total_btc, 2)
        print("Executing market order on bitstamp. Buying ", size)
        async_cancel = POOL.apply_async(cancel_all_gdax)
        if not TEST:
            buy_market_order(bitstamp_client, size)
            async_cancel.wait(timeout=5)
            # letting balances update
            time.sleep(1.5)
        else:
            print("TEST Mode " * 10)
    else:
        bitstamp_book = async_bitstamp_order_book.get()
        bitstamp_sell_price = get_price_estimate_for_btc(bitstamp_book, 1.5)
        bitstamp_buy_price = get_price_estimate_for_usd(bitstamp_book, 1.5)
        # Valid sell price on gdax is bitstamp buy + fees + spread
        buy_spread, sell_spread = compute_buy_and_sell_spreads(gdax_balances)
        print("buy spread:", buy_spread, "sell spread:", sell_spread)
        gdax_profitable_sell_price = \
            round(bitstamp_buy_price + 0.0025 * bitstamp_buy_price + sell_spread, 2)
        # Valid buy price on gdax is bitstamp sell - fees - spread
        gdax_profitable_buy_price = \
            round(bitstamp_sell_price - 0.0025 * bitstamp_sell_price - buy_spread, 2)
        # TODO(vidurj) Adjust these prices so that we are within 7 cents of the closest order behind us
        gdax_book = async_gdax_order_book.get()
        max_bid, min_ask = compute_max_bid_and_min_ask(gdax_book)
        limit_sell_price = max(gdax_profitable_sell_price, min_ask)
        limit_buy_price = min(gdax_profitable_buy_price, max_bid)
        # To support an ask on gdax, bitstamp must have the cash to buy an equivalent number of BTC
        bitstamp_supports_ask_on_gdax = \
            bitstamp_balances[("USD", "available")] >= LIMIT_ORDER_SIZE * bitstamp_buy_price * 1.0025
        # To support a bid on gdax, bit stamp must have an equivalent number of BTC to sell
        bitstamp_supports_bid_on_gdax = bitstamp_balances[("BTC", "available")] >= LIMIT_ORDER_SIZE
        outstanding_orders, cancellations = async_outstanding_orders.get()
        if not bitstamp_supports_bid_on_gdax:
            cancellations.extend(async_cancel_gdax_buy(outstanding_orders))
        else:
            if "buy" in outstanding_orders:
                if abs(outstanding_orders["buy"]["price"] - limit_buy_price) > 0.2 or outstanding_orders["buy"]["filled_size"] > 0.05:
                    cancellations.extend(async_cancel_gdax_buy(outstanding_orders))
                    gdax_limit_order(limit_buy_price, "buy")
            elif gdax_balances[("USD", "balance")] >= LIMIT_ORDER_SIZE * limit_buy_price:
                gdax_limit_order(limit_buy_price, "buy")

        if not bitstamp_supports_ask_on_gdax:
            cancellations.extend(async_cancel_gdax_sell(outstanding_orders))
        else:
            if "sell" in outstanding_orders:
                if abs(outstanding_orders["sell"]["price"] - limit_sell_price) > 0.2 or outstanding_orders["sell"]["filled_size"] > 0.05:
                    cancellations.extend(async_cancel_gdax_sell(outstanding_orders))
                    gdax_limit_order(limit_sell_price, "sell")
            elif gdax_balances[("BTC", "balance")] >= LIMIT_ORDER_SIZE:
                gdax_limit_order(limit_sell_price, "sell")

        for resp in cancellations:
            print("Cancelled order", resp.get())

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
        time.sleep(0.25)
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

