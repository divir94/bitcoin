import gdax
import bitstamp.client
import time
import logging
import datetime
import random
import pickle
import sys
import traceback
import math
from multiprocessing.pool import ThreadPool
from datetime import datetime

# Exchange Name Constants
GDAX = "gdax"
BITSTAMP = "bitstamp"

# Operations
BUY = "buy"
SELL = "sell"

# logging
logger = logging.getLogger()
logging.Formatter.converter = time.gmtime
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

fh = logging.FileHandler('AggressiveTrader.log')
fh.setFormatter(formatter)
fh.setLevel(logging.INFO)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.WARN)
logger.addHandler(ch)


def create_clients():
    bitstamp_client = bitstamp.client.Trading(username='752298', key='MWaOh0LJmQLA2n6qxShMrFKTRNvqYXvO',
                                              secret='gTilVxPezUaJNlPqrEADwpigHYd0o41C')
    gdax_client = gdax.AuthenticatedClient(key="8e944b216e435691b071dd3f0f62caa9",
                                           b64secret="uSysPzywwj3LUlPuuNkKEHRpxhd4A+K/JGwbEfrc28nkd4c6qMLwr1IbRsw1GB1BKQzVXoj88n/WjMvIAvv/MA==",
                                           passphrase="ltg10b8unuo")
    return gdax_client, bitstamp_client


POOL = ThreadPool(processes=5)
TEST = False
MIN_BALANCING_SPREAD = 5.2
MIN_UNBALANCING_SPREAD = 5.2
gdax_client, bitstamp_client = create_clients()


def timeout_handler(signum, frame):
    gdax_client.cancel_all(data={"product": "BTC-USD"})
    logger.error("Bot got stuck!")
    raise Exception("Bot got stuck!")


def round_down(num):
    return math.floor(num * 100) / 100.0


# TODO(vidurj) carefully think through whether we want to use balance or available in these two funcs
def get_gdax_balances():
    data = {}
    accounts = gdax_client.get_accounts()
    for account in accounts:
        data[(account["currency"], "available")] = float(account["available"])
        data[(account["currency"], "balance")] = float(account["balance"])
    logger.info("GDAX balances {}".format(data))
    return data


def get_bitstamp_balances():
    balances = bitstamp_client.account_balance()
    data = {
        ("USD", "available"): float(balances["usd_available"]),
        ("USD", "balance"): float(balances["usd_balance"]),
        ("BTC", "available"): float(balances["btc_available"]),
        ("BTC", "balance"): float(balances["btc_balance"])
    }
    logger.info("Bitstamp balances {}".format(data))
    return data


def gdax_market_order(size, side):
    logger.info("executing gdax {} market order for {}".format(side, size))
    if side == BUY:
        return gdax_client.buy(type="market", size=str(size), product_id='BTC-USD')
    elif side == SELL:
        return gdax_client.sell(type="market", size=str(size), product_id='BTC-USD')
    else:
        raise Exception("{} not understood".format(side))


def bitstamp_market_order(size, side):
    """
    Order to buy amount of bitcoins for specified price.
    """
    data = {'amount': size}
    url = bitstamp_client._construct_url(side + "/market/", "btc", "usd")
    logger.info("executing bitstamp {} market order for {}".format(side, size))
    return bitstamp_client._post(url, data=data, return_json=True, version=2)


def wait_till_balance(exchange, btc_quant):
    if exchange == GDAX:
        balance_func = get_gdax_balances
    elif exchange == BITSTAMP:
        balance_func = get_bitstamp_balances
    else:
        raise Exception("{} not understood".format(exchange))
    balance = balance_func()
    while round(abs(balance[("BTC", "balance")] - btc_quant), 2) >= 0.01:
        time.sleep(1)
        logger.warn(
            "waiting for balance to update. Expected {} actual {} on {}".format(btc_quant, balance[("BTC", "balance")],
                                                                                exchange))
        balance = balance_func()


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
    if BUY in outstanding_orders:
        return [POOL.apply_async(cancel_gdax_order, (outstanding_orders[BUY]["id"],))]
    else:
        return []


def async_cancel_gdax_sell(outstanding_orders):
    if SELL in outstanding_orders:
        return [POOL.apply_async(cancel_gdax_order, (outstanding_orders[SELL]["id"],))]
    else:
        return []


def cancel_gdax_order(order_id):
    resp = gdax_client.cancel_order(order_id)
    logger.info("Attempted to cancel order {}\nResponse: {}".format(order_id, resp))


def get_outstanding_orders_gdax():
    raw_orders = gdax_client.get_orders()
    assert len(raw_orders) == 1
    orders = raw_orders[0]
    outstanding_orders = {}
    cancellations = []
    outstanding_order_str = ""
    for order in orders:
        outstanding_order_str += str(order)
        side = order["side"]
        assert side == BUY or side == SELL
        our_format = {
            "price": float(order["price"]),
            "id": str(order["id"]),
            "filled_size": float(order["filled_size"]),
            "created_at": order["created_at"]
        }
        if side in outstanding_orders:
            competing_order = outstanding_orders[side]
            if order["created_at"] > competing_order["created_at"]:
                outstanding_orders[side] = our_format
        else:
            outstanding_orders[side] = our_format
    logger.info(outstanding_order_str)
    return outstanding_orders, cancellations


def gdax_limit_order(price, size, side):
    if side == BUY:

        resp = gdax_client.buy(type="limit", size=str(size), price=str(price),
                               time_in_force="GTT", cancel_after="min", product_id='BTC-USD',
                               post_only="True")
    elif side == SELL:
        resp = gdax_client.sell(type="limit", size=str(size), price=str(price),
                                time_in_force="GTT", cancel_after="min", product_id='BTC-USD',
                                post_only="True")
    else:
        raise Exception("Unknown  " + str(side))
    logger.info("Placed {} limit order for size {} and price {}\nResponse: {}".format(side, size, price, resp))
    if "message" in resp and resp["message"] == "Insufficient funds":
        return resp
    elif "id" in resp and "price" in resp:
        return resp
    else:
        raise Exception("Unable to place order.\n" + str(resp))


def cancel_all_gdax():
    logger.info("Cancelling all GDAX orders")
    return gdax_client.cancel_all(data={"product": "BTC-USD"})


def get_order_book_gdax():
    book = gdax_client.get_product_order_book('BTC-USD', level=2)
    return book


def get_balances():
    gdax_balances_async = POOL.apply_async(get_gdax_balances)
    bitstamp_balances_async = POOL.apply_async(get_bitstamp_balances)
    return gdax_balances_async.get(timeout=3), bitstamp_balances_async.get(timeout=3)


def compute_buy_and_sell_spreads(balances):
    if balances[("BTC", "balance")] - required_btc / 2 <= 0.2:
        buy_spread = MIN_BALANCING_SPREAD
    else:
        buy_spread = MIN_UNBALANCING_SPREAD
    if required_btc / 2 - balances[("BTC", "balance")] <= 0.2:
        sell_spread = MIN_BALANCING_SPREAD
    else:
        sell_spread = MIN_UNBALANCING_SPREAD
    logger.info("gdax buy spread {} sell spread {}".format(buy_spread, sell_spread))
    return buy_spread, sell_spread


def compute_buy_order_size(gdax_balances, bitstamp_balances, gdax_buy_price):
    if gdax_buy_price:
        gdax_supported = gdax_balances[("USD", "balance")] / gdax_buy_price
        bitstamp_supported = bitstamp_balances[("BTC", "available")]
        return round_down(min(bitstamp_supported, gdax_supported))
    else:
        return None


def compute_sell_order_size(gdax_balances, bitstamp_balances, bitstamp_buy_price):
    gdax_supported = gdax_balances[("BTC", "balance")]
    bitstamp_supported = bitstamp_balances[("USD", "available")] / bitstamp_buy_price
    return round_down(min(bitstamp_supported, gdax_supported))


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


def get_vol_and_price(book):
    total_volume = 0
    vol_and_price = []
    for point in book:
        price = float(point[0])
        vol = float(point[1])
        vol_and_price.append((total_volume, price))
        total_volume += vol
    return vol_and_price


def compute_gdax_buy_price(gdax_book, bitstamp_book):
    spare_coins = 3
    gdax_buy_limit_vol_and_price = get_vol_and_price(gdax_book["bids"])
    bitstamp_sell_market_vol_and_price = get_vol_and_price(bitstamp_book["bids"])
    while len(gdax_buy_limit_vol_and_price) > 0 and len(bitstamp_sell_market_vol_and_price) > 0:
        gdax_price, gdax_vol = gdax_buy_limit_vol_and_price[0]
        bitstamp_price, bitstamp_vol = bitstamp_sell_market_vol_and_price[0]
        if gdax_vol + spare_coins <= bitstamp_vol:
            if gdax_price + 2 < bitstamp_price * (1 - 0.0025):
                return gdax_price + 0.01
            else:
                gdax_buy_limit_vol_and_price = gdax_buy_limit_vol_and_price[1:]
        else:
            bitstamp_sell_market_vol_and_price = bitstamp_sell_market_vol_and_price[1:]
    return None


def compute_gdax_sell_price(gdax_book, bitstamp_book):
    spare_coins = 5
    gdax_sell_limit_vol_and_price = get_vol_and_price(gdax_book["asks"])
    bitstamp_buy_market_vol_and_price = get_vol_and_price(bitstamp_book["asks"])
    while len(gdax_sell_limit_vol_and_price) > 0 and len(bitstamp_buy_market_vol_and_price) > 0:
        gdax_price, gdax_vol = gdax_sell_limit_vol_and_price[0]
        bitstamp_price, bitstamp_vol = bitstamp_buy_market_vol_and_price[0]
        if gdax_vol + spare_coins <= bitstamp_vol:
            if gdax_price > bitstamp_price * 1.0025 + 5:
                return gdax_price - 0.01
            else:
                gdax_sell_limit_vol_and_price = gdax_sell_limit_vol_and_price[1:]
        else:
            bitstamp_buy_market_vol_and_price = bitstamp_buy_market_vol_and_price[1:]
    return None


def average_price_for_coins(book, requested_vol):
    total_cost = 0.0
    remaining_vol = requested_vol
    for order in book:
        price = float(order[0])
        vol_at_price = float(order[1])
        satisfied = min(vol_at_price, remaining_vol)
        total_cost += price * satisfied
        remaining_vol -= satisfied
        if remaining_vol == 0:
            return total_cost / requested_vol
    return None


def rebalance(data):
    """
    :param data: Must contain the balances on both exchanges. These are maintained by
    """
    logger.info("rebalancing")
    if TEST:
        print("THIS IS A TEST.\n" * 5)

    async_gdax_order_book = POOL.apply_async(get_order_book_gdax)
    async_bitstamp_order_book = POOL.apply_async(bitstamp_client.order_book)
    async_outstanding_orders = POOL.apply_async(get_outstanding_orders_gdax)
    gdax_balances, bitstamp_balances = get_balances()
    bitstamp_book = async_bitstamp_order_book.get()
    logger.info("bitstamp best bid {}, best ask {}".format(bitstamp_book["bids"][0], bitstamp_book["asks"][0]))
    gdax_book = async_gdax_order_book.get()
    logger.info("gdax best bid {}, best ask {}".format(gdax_book["bids"][0], gdax_book["asks"][0]))
    total_btc = round(bitstamp_balances[("BTC", "balance")] + gdax_balances[("BTC", "balance")], 2)
    if total_btc >= round(data["required_btc"] + 0.01, 2):
        size = round(total_btc - data["required_btc"], 2)
        bitstamp_sell_price = average_price_for_coins(bitstamp_book["bids"], size)
        gdax_sell_price = average_price_for_coins(gdax_book["bids"], size)
        logger.warn("sell prices. gdax {} bitstamp {}".format(gdax_sell_price, bitstamp_sell_price))
        async_cancel_all = POOL.apply_async(cancel_all_gdax)
        if not TEST:
            if bitstamp_sell_price * (1 - 0.0022) >= gdax_sell_price * (1 - 0.0025):
                bitstamp_market_order(size=size, side=SELL)
                new_bitstamp_btc_balance = bitstamp_balances[("BTC", "balance")] - size
                wait_till_balance(BITSTAMP, new_bitstamp_btc_balance)
            else:
                gdax_market_order(size=size, side=SELL)
                new_gdax_btc_balance = gdax_balances[("BTC", "balance")] - size
                wait_till_balance(GDAX, new_gdax_btc_balance)
            async_cancel_all.wait(30)
        else:
            print("TEST Mode " * 10)
    elif data["required_btc"] >= round(total_btc + 0.01, 2):
        size = round(data["required_btc"] - total_btc, 2)
        bitstamp_buy_price = average_price_for_coins(bitstamp_book["asks"], size)
        gdax_buy_price = average_price_for_coins(gdax_book["asks"], size)
        logger.warn("buy prices. gdax {} bitstamp {}".format(gdax_buy_price, bitstamp_buy_price))
        async_cancel_all = POOL.apply_async(cancel_all_gdax)
        if not TEST:
            if bitstamp_buy_price * 1.0022 <= gdax_buy_price * 1.0025:
                bitstamp_market_order(size=size, side=BUY)
                new_bitstamp_btc_balance = bitstamp_balances[("BTC", "balance")] + size
                wait_till_balance(BITSTAMP, new_bitstamp_btc_balance)
            else:
                gdax_market_order(size=size, side=BUY)
                new_gdax_btc_balance = gdax_balances[("BTC", "balance")] + size
                wait_till_balance(GDAX, new_gdax_btc_balance)
            async_cancel_all.wait(30)
        else:
            print("TEST Mode " * 10)
    else:
        bitstamp_buy_price = average_price_for_coins(bitstamp_book["asks"], 10)
        gdax_profitable_sell_price = compute_gdax_sell_price(gdax_book, bitstamp_book)
        gdax_profitable_buy_price = compute_gdax_buy_price(gdax_book, bitstamp_book)
        max_bid, min_ask = compute_max_bid_and_min_ask(gdax_book)
        limit_sell_price = max(gdax_profitable_sell_price, min_ask)
        limit_buy_price = min(gdax_profitable_buy_price, max_bid)
        buy_order_size = compute_buy_order_size(gdax_balances=gdax_balances,
                                                bitstamp_balances=bitstamp_balances,
                                                gdax_buy_price=limit_buy_price)
        sell_order_size = compute_sell_order_size(gdax_balances=gdax_balances,
                                                  bitstamp_balances=bitstamp_balances,
                                                  bitstamp_buy_price=bitstamp_buy_price)
        outstanding_orders, cancellations = async_outstanding_orders.get()
        if BUY in outstanding_orders:
            if (abs(outstanding_orders[BUY]["price"] - limit_buy_price) > 1) or (
                        outstanding_orders[BUY]["filled_size"] > 0.05):
                cancellations.extend(async_cancel_gdax_buy(outstanding_orders))
                if buy_order_size > 0 and limit_buy_price:
                    POOL.apply_async(gdax_limit_order, (limit_buy_price, buy_order_size, BUY))
        elif buy_order_size > 0 and limit_buy_price:
            POOL.apply_async(gdax_limit_order, (limit_buy_price, buy_order_size, BUY))

        if SELL in outstanding_orders:
            if (abs(outstanding_orders[SELL]["price"] - limit_sell_price) > 1) or (
                        outstanding_orders[SELL]["filled_size"] > 0.05):
                cancellations.extend(async_cancel_gdax_sell(outstanding_orders))
                if sell_order_size > 0 and limit_sell_price:
                    POOL.apply_async(gdax_limit_order, (limit_sell_price, sell_order_size, SELL))
        elif sell_order_size > 0 and limit_sell_price:
            POOL.apply_async(gdax_limit_order, (limit_sell_price, sell_order_size, SELL))
    return data


gdax_client.cancel_all(data={"product": "BTC-USD"})
time.sleep(5)
gdax_balances, bitstamp_balances = get_balances()
required_btc = round(bitstamp_balances[("BTC", "balance")] + gdax_balances[("BTC", "balance")], 2)
while True:
    try:
        start_time = time.time()
        rebalance({"required_btc": required_btc})
        elapsed_time = time.time() - start_time
        logger.info("elapsed time {}".format(elapsed_time))
        if elapsed_time < 0.9:
            time.sleep(0.9 - elapsed_time)
    except:
        try:
            logger.error("Unexpected error: {}\n{}".format(sys.exc_info()[0], traceback.format_exc()))
            cancel_all_gdax()
        except:
            logger.error("Unexpected error in error handling: {}\n{}".format(sys.exc_info()[0], traceback.format_exc()))
            time.sleep(120)
            gdax_client, bitstamp_client = create_clients()
            cancel_all_gdax()
