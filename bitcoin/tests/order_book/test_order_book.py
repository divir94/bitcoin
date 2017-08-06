from sortedcontainers import SortedListWithKey

from bitcoin.order_book import *


def test_order_book_add():
    book = OrderBook(1)
    book.add('buy', 100., 5., 'abc')
    assert len(book.bids) == 1
    assert book.bids[0] == PriceLevel(100., {'abc': 5.})
    assert book.orders == {'abc': 5.}

    book.add('sell', 105., 1., 'xyz')
    assert len(book.asks) == 1
    assert book.bids[0] == PriceLevel(105., {'xyz': 1.})
    assert book.orders == {'abc': 5., 'xyz': 1.}

    book.add('buy', 90., 2., 'lmn')
    assert len(book.bids) == 2
    assert book.bids[1] == PriceLevel(90., {'lmn': 2.})
    assert book.orders == {'abc': 5., 'xyz': 1., 'lmn': 2.}


def test_order_book_get():
    pass


def test_order_book_remove():
    pass


def test_order_book_update():
    pass
