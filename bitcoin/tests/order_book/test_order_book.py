import pytest

from bitcoin.order_book.order_book import OrderBook
from bitcoin.order_book.price_level import PriceLevel


def test_order_book_add_single_order():
    book = OrderBook(1)
    book.add('buy', 100., 5., 'a')
    assert len(book.bids) == 1
    assert book.bids[0] == PriceLevel(100., {'a': 5.})
    assert book.orders == {'a': 100.}


def test_order_book_add_multiple_orders():
    book = OrderBook(1)
    book.add('buy', 100., 5., 'a')
    book.add('sell', 105., 1., 'b')
    assert len(book.asks) == 1
    assert book.asks[0] == PriceLevel(105., {'b': 1.})
    assert book.orders == {'a': 100., 'b': 105.}

    book.add('buy', 90., 2., 'c')
    assert len(book.bids) == 2
    assert book.bids[0] == PriceLevel(100., {'a': 5.})
    assert book.orders == {'a': 100., 'b': 105., 'c': 90.}


def test_order_book_add_orders_at_same_price():
    book = OrderBook(1)
    book.add('buy', 110., 1., 'a')
    book.add('buy', 100., 2., 'b')
    book.add('sell', 150., 5., 'c')
    book.add('buy', 100., 1., 'd')
    assert len(book.bids) == 2
    assert book.bids[-1] == PriceLevel(100., {'b': 2., 'd': 1.})
    assert book.orders == {'a': 110.,
                           'b': 100.,
                           'c': 150.,
                           'd': 100.}


def test_order_book_get():
    book = OrderBook(1)
    book.add('buy', 100., 5., 'a')
    book.add('sell', 110., 5., 'b')
    book.add('buy', 100., 7., 'c')
    assert book.get('a') == (100., 5., 'a')
    assert book.get('b') == (110., 5., 'b')
    assert book.get('c') == (100., 7., 'c')


def test_order_book_get_assert():
    book = OrderBook(1)
    with pytest.raises(AssertionError):
        book.get('a')


def test_order_book_update():
    book = OrderBook(1)
    book.add('buy', 100., 5., 'a')
    book.update('a', 15.)
    assert len(book.bids) == 1
    assert book.bids[0] == PriceLevel(100., {'a': 15.})
    assert book.orders == {'a': 100.}


def test_order_book_update_order_at_same_price():
    book = OrderBook(1)
    book.add('buy', 100., 5., 'a')
    book.add('buy', 100., 10., 'b')
    book.update('a', 15.)
    assert len(book.bids) == 1
    assert book.bids[0] == PriceLevel(100., {'a': 15., 'b': 10.})
    assert book.orders == {'a': 100., 'b': 100.}


def test_order_book_remove():
    book = OrderBook(1)
    book.add('buy', 100., 5., 'a')
    book.add('buy', 100., 10., 'b')
    book.update('a', 0.)
    assert len(book.bids) == 1
    assert book.bids[0] == PriceLevel(100., {'b': 10.})
    assert book.orders == {'b': 100.}
