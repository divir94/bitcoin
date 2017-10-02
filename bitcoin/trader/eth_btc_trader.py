# coding=utf-8
import logging
import math
import sys
import time
import traceback
from collections import namedtuple
from multiprocessing.pool import ThreadPool

import bitstamp.client

import gdax

# Exchange Name Constants
GDAX = "gdax"
BITSTAMP = "bitstamp"

BASE = 'eth'
QUOTE = 'btc'

# Operations
BUY = "buy"
SELL = "sell"

MIN_ORDER_SIZE = 0.1
SPARE_COINS = 5
MIN_TICK = 0.00001
PER_TRADE_SPREAD = 1.001

price_volume_pair = namedtuple('PriceVolumePair', ['price', 'volume'])

# logging
logger = logging.getLogger()
logging.Formatter.converter = time.gmtime
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

fh = logging.FileHandler('eth_btc_trader.log')
fh.setFormatter(formatter)
fh.setLevel(logging.INFO)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.WARN)
logger.addHandler(ch)


def create_clients():
    bitstamp_client = bitstamp.client.Trading(username='752298', key='bDtzFzYpceSrgN88E6AchsV0y4gX15GG',
                                              secret='QRJvxiBpqLNNFC2RI50ulw2SjdZ7oysF')
    gdax_client = gdax.AuthenticatedClient(key="8e944b216e435691b071dd3f0f62caa9",
                                           b64secret="uSysPzywwj3LUlPuuNkKEHRpxhd4A+K/JGwbEfrc28nkd4c6qMLwr1IbRsw1GB1BKQzVXoj88n/WjMvIAvv/MA==",
                                           passphrase="ltg10b8unuo")
    return gdax_client, bitstamp_client


POOL = ThreadPool(processes=5)
TEST = False
MIN_BALANCING_SPREAD = 5.2
MIN_UNBALANCING_SPREAD = 5.2
gdax_client, bitstamp_client = create_clients()


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
    balances = bitstamp_client.account_balance(base=BASE, quote=QUOTE)
    data = {
        ("BTC", "available"): float(balances["btc_available"]),
        ("BTC", "balance"): float(balances["btc_balance"]),
        ("ETH", "available"): float(balances["eth_available"]),
        ("ETH", "balance"): float(balances["eth_balance"])
    }
    logger.info("Bitstamp balances {}".format(data))
    return data


def gdax_market_order(size, side):
    logger.info("executing gdax {} market order for {}".format(side, size))
    if side == BUY:
        return gdax_client.buy(type="market", size=str(size), product_id='ETH-BTC')
    elif side == SELL:
        return gdax_client.sell(type="market", size=str(size), product_id='ETH-BTC')
    else:
        raise Exception("{} not understood".format(side))


def wait_till_balance(exchange, eth_quant):
    if exchange == GDAX:
        balance_func = get_gdax_balances
    elif exchange == BITSTAMP:
        balance_func = get_bitstamp_balances
    else:
        raise Exception("{} not understood".format(exchange))
    balance = balance_func()
    while round(abs(balance[("ETH", "balance")] - eth_quant), 2) >= MIN_ORDER_SIZE:
        time.sleep(1)
        logger.warn(
            "waiting for balance to update. Expected {} actual {} on {}".format(eth_quant, balance[("ETH", "balance")],
                                                                                exchange))
        balance = balance_func()



def get_price_estimate_for_eth(book, desired_vol, desired_orders=6):
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
    return outstanding_orders


def gdax_limit_order(price, size, side):
    if side == BUY:

        resp = gdax_client.buy(type="limit", size=str(size), price=str(price),
                               time_in_force="GTT", cancel_after="min", product_id='ETH-BTC',
                               post_only="True")
    elif side == SELL:
        resp = gdax_client.sell(type="limit", size=str(size), price=str(price),
                                time_in_force="GTT", cancel_after="min", product_id='ETH-BTC',
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
    return gdax_client.cancel_all(data={"product": "ETH-BTC"})


def get_order_book_gdax():
    book = gdax_client.get_product_order_book('ETH-BTC', level=2)
    return book


def get_balances():
    gdax_balances_async = POOL.apply_async(get_gdax_balances)
    bitstamp_balances_async = POOL.apply_async(get_bitstamp_balances)
    return gdax_balances_async.get(timeout=3), bitstamp_balances_async.get(timeout=3)



def compute_buy_order_size(gdax_balances, bitstamp_balances, gdax_buy_price):
    if gdax_buy_price:
        gdax_supported = gdax_balances[("BTC", "balance")] / gdax_buy_price
        bitstamp_supported = bitstamp_balances[("ETH", "available")]
        return round_down(min(bitstamp_supported, gdax_supported))
    else:
        return None


def compute_sell_order_size(gdax_balances, bitstamp_balances, bitstamp_buy_price):
    gdax_supported = gdax_balances[("ETH", "balance")]
    bitstamp_supported = bitstamp_balances[("BTC", "available")] / bitstamp_buy_price
    return round_down(min(bitstamp_supported, gdax_supported))


def compute_max_bid_and_min_ask(book):
    best_bid = float(book["bids"][0][0])
    best_ask = float(book["asks"][0][0])
    # Cannot place an ask at or below the best bid. Doesn't make sense to place ask $0.05 better than
    # best ask
    min_ask = max(best_bid + MIN_TICK, best_ask - 5 * MIN_TICK)
    # Cannot place a bid at or above the best ask. Doesn't make sense to place bid $0.05 better than
    # best bid
    max_bid = min(best_ask - MIN_TICK, best_bid + 5 * MIN_TICK)
    return max_bid, min_ask


def get_vol_and_price(book):
    total_volume = 0
    price_volume_pairs = []
    for point in book:
        price = float(point[0])
        vol = float(point[1])
        price_volume_pairs.append(price_volume_pair(price=price, volume=total_volume))
        total_volume += vol
    return price_volume_pairs


def compute_gdax_buy_price(gdax_book, bitstamp_book):
    gdax_buy_limit_vol_and_price = get_vol_and_price(gdax_book["bids"])
    bitstamp_sell_market_vol_and_price = get_vol_and_price(bitstamp_book["bids"])
    while len(gdax_buy_limit_vol_and_price) > 0 and len(bitstamp_sell_market_vol_and_price) > 0:
        gdax_pair = gdax_buy_limit_vol_and_price[0]
        bitstamp_pair = bitstamp_sell_market_vol_and_price[0]
        if bitstamp_pair.volume >= max(0.25 * gdax_pair.volume, SPARE_COINS):
            if bitstamp_pair.price > (gdax_pair.price + MIN_TICK) * PER_TRADE_SPREAD:
                return gdax_pair.price + MIN_TICK
            else:
                del gdax_buy_limit_vol_and_price[0]
        else:
            del bitstamp_sell_market_vol_and_price[0]
    return MIN_TICK


def compute_gdax_sell_price(gdax_book, bitstamp_book):
    gdax_sell_limit_vol_and_price = get_vol_and_price(gdax_book["asks"])
    bitstamp_buy_market_vol_and_price = get_vol_and_price(bitstamp_book["asks"])
    while len(gdax_sell_limit_vol_and_price) > 0 and len(bitstamp_buy_market_vol_and_price) > 0:
        gdax_point = gdax_sell_limit_vol_and_price[0]
        bitstamp_point = bitstamp_buy_market_vol_and_price[0]
        if bitstamp_point.volume >= max(0.25 * gdax_point.volume, SPARE_COINS):
            if gdax_point.price - MIN_TICK > bitstamp_point.price * PER_TRADE_SPREAD:
                return gdax_point.price - MIN_TICK
            else:
                del gdax_sell_limit_vol_and_price[0]
        else:
            del bitstamp_buy_market_vol_and_price[0]
    return 20000.0


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


def rebalance(required_eth):
    """
    :param required_eth:
    :param data: Must contain the balances on both exchanges. These are maintained by
    """
    assert required_eth is not None

    logger.info("rebalancing")
    if TEST:
        print("THIS IS A TEST.\n" * 5)

    async_gdax_order_book = POOL.apply_async(get_order_book_gdax)
    async_bitstamp_order_book = POOL.apply_async(bitstamp_client.order_book, (True, BASE, QUOTE))
    async_outstanding_orders = POOL.apply_async(get_outstanding_orders_gdax)
    gdax_balances, bitstamp_balances = get_balances()
    bitstamp_book = async_bitstamp_order_book.get()
    logger.info("bitstamp best bid {}, best ask {}".format(bitstamp_book["bids"][0], bitstamp_book["asks"][0]))
    gdax_book = async_gdax_order_book.get()
    logger.info("gdax best bid {}, best ask {}".format(gdax_book["bids"][0], gdax_book["asks"][0]))
    total_eth = round(bitstamp_balances[("ETH", "balance")] + gdax_balances[("ETH", "balance")], 2)
    if total_eth >= round(required_eth + MIN_ORDER_SIZE, 2):
        size = round(total_eth - required_eth, 2)
        bitstamp_sell_price = average_price_for_coins(bitstamp_book["bids"], size)
        gdax_sell_price = float(gdax_book["asks"][0][0])
        logger.warn("sell prices. gdax {} bitstamp {}".format(gdax_sell_price, bitstamp_sell_price))
        async_cancel_all = POOL.apply_async(cancel_all_gdax)
        if not TEST:
            # TODO(vidurj) same as below.
            if bitstamp_sell_price > gdax_sell_price:
                bitstamp_client.sell_limit_order(amount=size, price=gdax_sell_price, base=BASE, quote=QUOTE)
                time.sleep(0.5)
                bitstamp_client.cancel_all_orders()
                time.sleep(10)
            else:
                logger.warn("Missed trade")
                return total_eth
            async_cancel_all.wait(30)
        else:
            print("TEST Mode " * 10)
    elif required_eth >= round(total_eth + MIN_ORDER_SIZE, 2):
        size = round(required_eth - total_eth, 2)
        bitstamp_buy_price = average_price_for_coins(bitstamp_book["asks"], size)
        gdax_buy_price = float(gdax_book["bids"][0][0])
        logger.warn("buy prices. gdax {} bitstamp {}".format(gdax_buy_price, bitstamp_buy_price))
        async_cancel_all = POOL.apply_async(cancel_all_gdax)
        if not TEST:
            # TODO(vidurj) when picking which exchange to execute on, not only do we need to check
            # for best price, but also where there are assets to support the order.
            if bitstamp_buy_price < gdax_buy_price:
                bitstamp_client.buy_limit_order(amount=size, price=gdax_buy_price, base=BASE, quote=QUOTE)
                time.sleep(0.5)
                bitstamp_client.cancel_all_orders()
                time.sleep(10)
            else:
                logger.warn("Missed trade")
                return total_eth
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
        outstanding_orders = async_outstanding_orders.get()
        logger.info("buy price {} buy size {} sell price {} sell size {}".format(
            limit_buy_price, buy_order_size, limit_sell_price, sell_order_size
        ))
        if BUY in outstanding_orders:
            if (abs(outstanding_orders[BUY]["price"] - limit_buy_price) > 1) or \
                    (outstanding_orders[BUY]["filled_size"] > 0.05):
                async_cancel_gdax_buy(outstanding_orders)
                if buy_order_size > 0 and limit_buy_price:
                    POOL.apply_async(gdax_limit_order, (limit_buy_price, buy_order_size, BUY))
        elif buy_order_size > 0:
            POOL.apply_async(gdax_limit_order, (limit_buy_price, buy_order_size, BUY))

        if SELL in outstanding_orders:
            if (abs(outstanding_orders[SELL]["price"] - limit_sell_price) > 1) or \
                    (outstanding_orders[SELL]["filled_size"] > 0.05):
                async_cancel_gdax_sell(outstanding_orders)
                if sell_order_size > 0 and limit_sell_price:
                    POOL.apply_async(gdax_limit_order, (limit_sell_price, sell_order_size, SELL))
        elif sell_order_size > 0:
            POOL.apply_async(gdax_limit_order, (limit_sell_price, sell_order_size, SELL))
    return required_eth

if __name__ == "__main__":
    gdax_client.cancel_all(data={"product": "ETH-BTC"})
    time.sleep(5)
    gdax_balances, bitstamp_balances = get_balances()
    required_eth = round(bitstamp_balances[("ETH", "balance")] + gdax_balances[("ETH", "balance")], 2)
    while True:
        try:
            start_time = time.time()
            required_eth = rebalance(required_eth)
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
