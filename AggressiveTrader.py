import gdax
import bitstamp.client
import time
import datetime
import random
import pickle
import sys
import traceback
import signal
import math
from multiprocessing.pool import ThreadPool

def create_clients():
    bitstamp_client = bitstamp.client.Trading(username='752298', key='vBrdMQODpkodqrP5fCoRwZfyGsTdtwKN',
                                              secret='LMZI5bd9OhS2T8jmi3pXklpE9776Cpwb')
    gdax_client = gdax.AuthenticatedClient(key="8e944b216e435691b071dd3f0f62caa9",
                                           b64secret="uSysPzywwj3LUlPuuNkKEHRpxhd4A+K/JGwbEfrc28nkd4c6qMLwr1IbRsw1GB1BKQzVXoj88n/WjMvIAvv/MA==",
                                           passphrase="ltg10b8unuo")
    return gdax_client, bitstamp_client

POOL = ThreadPool(processes=5)
TEST = False
MIN_BALANCING_SPREAD = 3.2
MIN_UNBALANCING_SPREAD = 10.2
TIME_BETWEEN_REBALANCES = 2
gdax_client, bitstamp_client = create_clients()


def timeout_handler(signum, frame):
    gdax_client.cancel_all(data={"product": "BTC-USD"})
    print("Bot got stuck!")
    raise Exception("Bot got stuck!")

def round_down(num):
    return math.floor(num * 100) / 100.0

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

def get_price_estimate_for_btc(book, desired_vol, desired_orders=6):
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
    resp = gdax_client.cancel_order(order_id)
    print("Attempted to cancel order", order_id, resp)

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

def gdax_limit_order(price, size, side):
    if side == "buy":
        resp = gdax_client.buy(type="limit", size=str(size), price=str(price),
                                     time_in_force="GTT", cancel_after="min", product_id='BTC-USD',
                                     post_only="True")
    elif side == "sell":
        resp = gdax_client.sell(type="limit", size=str(size), price=str(price),
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
    return gdax_client.get_product_order_book('BTC-USD', level=2)

def get_balances():
    gdax_balances_async = POOL.apply_async(get_gdax_available_balances)
    bitstamp_balances_async = POOL.apply_async(get_bitstamp_available_balances)
    return gdax_balances_async.get(), bitstamp_balances_async.get()

def compute_buy_and_sell_spreads(balances):
    if balances[("BTC", "balance")] - required_btc / 2 < 0.0:
        buy_spread = MIN_BALANCING_SPREAD
    else:
        buy_spread = MIN_UNBALANCING_SPREAD
    if required_btc / 2 - balances[("BTC", "balance")] < 0.0:
        sell_spread = MIN_BALANCING_SPREAD
    else:
        sell_spread = MIN_UNBALANCING_SPREAD
    return buy_spread, sell_spread

def compute_buy_order_size(gdax_balances, bitstamp_balances, gdax_buy_price):
    gdax_supported = gdax_balances[("USD", "balance")] / gdax_buy_price
    bitstamp_supported = bitstamp_balances[("BTC", "available")]
    return round_down(min(bitstamp_supported, gdax_supported / 2.0))

def compute_sell_order_size(gdax_balances, bitstamp_balances, bitstamp_buy_price):
    gdax_supported = gdax_balances[("BTC", "balance")]
    bitstamp_supported = bitstamp_balances[("USD", "available")] / bitstamp_buy_price
    return round_down(min(bitstamp_supported, gdax_supported / 2.0))

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

def price_at_vol(book, requested_vol):
    total_vol = 0.0
    for str_price, str_vol_at_price, _ in book:
        price = float(str_price)
        vol_at_price = float(str_vol_at_price)
        total_vol += vol_at_price
        if total_vol >= requested_vol:
            return price
    return price

def bid_and_ask_in_the_money(gdax_book, limit_buy_price, limit_sell_price):
    in_the_money_ask_price = price_at_vol(gdax_book["asks"], 15)
    best_ask = float(gdax_book["asks"][0][0])
    ask_in_money = (limit_sell_price < in_the_money_ask_price) and (limit_sell_price - best_ask < 5)
    in_the_money_bid_price = price_at_vol(gdax_book["bids"], 15)
    best_bid = float(gdax_book["bids"][0][0])
    bid_in_money = (limit_buy_price > in_the_money_bid_price) and (best_bid - limit_buy_price < 5)
    return bid_in_money, ask_in_money


def rebalance(data):
    """
    :param data: Must contain the balances on both exchanges. These are maintained by
    """
    if TEST:
        print("THIS IS A TEST.\n" * 5)

    async_gdax_order_book = POOL.apply_async(get_order_book_gdax)
    async_bitstamp_order_book = POOL.apply_async(bitstamp_client.order_book)
    async_outstanding_orders = POOL.apply_async(get_outstanding_orders_gdax)
    gdax_balances, bitstamp_balances = get_balances()
    if round(data["btc_on_bitstamp"], 2) != round(bitstamp_balances[("BTC", "balance")], 2):
        print("!" * 120)
        print("Local and Bitstamp balances out of sync", data["btc_on_bitstamp"], bitstamp_balances[("BTC", "balance")])
        gdax_client.cancel_all(data={"product": "BTC-USD"})
        time.sleep(1)
        return data
    total_btc =  data["btc_on_bitstamp"] + gdax_balances[("BTC", "balance")]
    if total_btc >= data["required_btc"] + 0.01:
        size = round(total_btc - data["required_btc"], 2)
        print("Executing market order on bitstamp. Selling ", size)
        async_cancel = POOL.apply_async(cancel_all_gdax)
        if not TEST:
            data["btc_on_bitstamp"] = data["btc_on_bitstamp"] - size
            sell_market_order(bitstamp_client, size)
            async_cancel.wait(timeout=5)
        else:
            print("TEST Mode " * 10)
    elif data["required_btc"] >= total_btc + 0.01:
        size = round(data["required_btc"] - total_btc, 2)
        print("Executing market order on bitstamp. Buying ", size)
        async_cancel = POOL.apply_async(cancel_all_gdax)
        if not TEST:
            data["btc_on_bitstamp"] = data["btc_on_bitstamp"] + size
            buy_market_order(bitstamp_client, size)
            async_cancel.wait(timeout=5)
        else:
            print("TEST Mode " * 10)
    else:
        bitstamp_book = async_bitstamp_order_book.get()
        cur_bitstamp_sell_price = get_price_estimate_for_btc(bitstamp_book, 5)
        cur_bitstamp_buy_price = get_price_estimate_for_usd(bitstamp_book, 5)
        data["bitstamp_sell_prices"].append(cur_bitstamp_sell_price)
        data["bitstamp_sell_prices"] = data["bitstamp_sell_prices"][1:]
        data["bitstamp_buy_prices"].append(cur_bitstamp_buy_price)
        data["bitstamp_buy_prices"] = data["bitstamp_buy_prices"][1:]
        bitstamp_sell_price = min(data["bitstamp_sell_prices"])
        bitstamp_buy_price = max(data["bitstamp_buy_prices"])
        # Valid sell price on gdax is bitstamp buy + fees + spread
        # buy_spread, sell_spread = compute_buy_and_sell_spreads(gdax_balances)
        # print("buy spread:", buy_spread, "sell spread:", sell_spread)
        print("bitstamp buy:", bitstamp_buy_price, "bitstamp sell:", bitstamp_sell_price)
        gdax_profitable_sell_price = round(bitstamp_buy_price * 1.0025 + 8.0, 2)
        # Valid buy price on gdax is bitstamp sell - fees - spread
        gdax_profitable_buy_price = round(bitstamp_sell_price * (1 - 0.0025) - 8.0, 2)
        print("gdax profitable buy price", gdax_profitable_buy_price,
              "gdax profitable sell price", gdax_profitable_sell_price)
        gdax_book = async_gdax_order_book.get()
        max_bid, min_ask = compute_max_bid_and_min_ask(gdax_book)
        limit_sell_price = max(gdax_profitable_sell_price, min_ask)
        limit_buy_price = min(gdax_profitable_buy_price, max_bid)
        bid_in_the_money, ask_in_the_money = bid_and_ask_in_the_money(gdax_book, limit_buy_price=limit_buy_price, limit_sell_price=limit_sell_price)
        buy_order_size = compute_buy_order_size(gdax_balances=gdax_balances,
                                                bitstamp_balances=bitstamp_balances,
                                                gdax_buy_price=limit_buy_price)
        sell_order_size = compute_sell_order_size(gdax_balances=gdax_balances,
                                                   bitstamp_balances=bitstamp_balances,
                                                   bitstamp_buy_price=bitstamp_buy_price)
        outstanding_orders, cancellations = async_outstanding_orders.get()
        if "buy" in outstanding_orders:
            if (not bid_in_the_money) or (outstanding_orders["buy"]["price"] != limit_buy_price) or (outstanding_orders["buy"]["filled_size"] > 0.05):
                cancellations.extend(async_cancel_gdax_buy(outstanding_orders))
                if buy_order_size > 0 and bid_in_the_money:
                    gdax_limit_order(price=limit_buy_price, size=buy_order_size, side="buy")
        elif buy_order_size > 0 and bid_in_the_money:
            gdax_limit_order(price=limit_buy_price, size=buy_order_size, side="buy")

        if "sell" in outstanding_orders:
            if (not ask_in_the_money) or (outstanding_orders["sell"]["price"] != limit_sell_price) or (outstanding_orders["sell"]["filled_size"] > 0.05):
                cancellations.extend(async_cancel_gdax_sell(outstanding_orders))
                if sell_order_size > 0 and ask_in_the_money:
                    gdax_limit_order(price=limit_sell_price, size=sell_order_size, side="sell")
        elif sell_order_size > 0 and ask_in_the_money:
            gdax_limit_order(price=limit_sell_price, size=sell_order_size, side="sell")
        if not ask_in_the_money and not bid_in_the_money:
            gdax_client.cancel_all(data={"product": "BTC-USD"})
            time.sleep(2)

        # for resp in cancellations:
        #     print("Cancelled order", resp.get())
    return data



signal.signal(signal.SIGALRM, timeout_handler)
gdax_client.cancel_all(data={"product": "BTC-USD"})
time.sleep(5)
gdax_balances, bitstamp_balances = get_balances()
required_btc = bitstamp_balances[("BTC", "balance")] + gdax_balances[("BTC", "balance")]
data = {
    "required_btc": required_btc,
    "btc_on_bitstamp": bitstamp_balances[("BTC", "balance")],
    "bitstamp_sell_prices": [1 for _ in range(10)],
    "bitstamp_buy_prices": [5000 for _ in range(10)]
}
while True:
    try:
        start_time = time.time()
        signal.alarm(15)
        data = rebalance(data)
        signal.alarm(0)
        elapsed_time = time.time() - start_time
        print("elapsed time", elapsed_time)
        if elapsed_time < 0.75:
            time.sleep(0.75 - elapsed_time)
    except:
        try:
            gdax_client.cancel_all(data={"product": "BTC-USD"})
            print("Unexpected error:", sys.exc_info()[0])
            print(traceback.format_exc())
            signal.alarm(0)
            time.sleep(10)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            print(traceback.format_exc())
            time.sleep(120)
            gdax_client, bitstamp_client = create_clients()
            gdax_client.cancel_all(data={"product": "BTC-USD"})
    print("-" * 60)

