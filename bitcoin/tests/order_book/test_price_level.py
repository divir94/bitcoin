from bitcoin.order_book import *


def test_price_level_add():
    level = PriceLevel(price=1000., orders={'abc': 5.})
    level.add(10., 'xyz')
    assert level.size == 15.
    orders = {
        'abc': 5.,
        'xyz': 10.
    }
    assert level.orders == orders


def test_price_level_remove():
    level = PriceLevel(price=1000., orders={'abc': 5., 'xyz': 10.})
    level.remove('xyz')
    assert level.size == 5.
    assert level.orders == {'abc': 5.}


def test_price_level_remove_last_order():
    level = PriceLevel(price=1000., orders={'abc': 5.})
    level.remove('abc')
    assert level.size == 0.
    assert level.orders == {}


def test_price_level_update():
    level = PriceLevel(price=1000., orders={'abc': 5., 'xyz': 10.})
    level.update('xyz', 15.)
    assert level.size == 20.
    orders = {
        'abc': 5.,
        'xyz': 15.
    }
    assert level.orders == orders


def test_price_level_update_to_zero():
    level = PriceLevel(price=1000., orders={'abc': 5., 'xyz': 10.})
    level.update('xyz', 0.)
    assert level.size == 5.
    assert level.orders == {'abc': 5.}

    level.update('abc', 0.)
    assert level.size == 0.
    assert level.orders == {}
