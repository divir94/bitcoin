import bitcoin.gdax.public_client as gdax
from bitcoin.order_book.util import compare_books, order_book_data_to_set
from bitcoin.order_book.order_book import OrderBook


def test_conversion_between_order_book_and_set():
    gdax_client = gdax.PublicClient()
    data = gdax_client.get_product_order_book(product_id='BTC-USD', level=3)
    book = OrderBook(data['sequence'], bids=data['bids'], asks=data['asks'])
    expected = order_book_data_to_set(data)
    actual = book.to_set()
    assert actual == expected


def test_compare_books():
    data = {
        'sequence': 1,
        'bids': [[u'3218.36', u'0.5399', u'3f681726-9078-4b8c-bfb2-dbc25910b75e'],
                 [u'3218.35', u'2.31', u'f3a5568b-236b-4d26-9082-6b527d6f1f61']],
        'asks': [[u'3221.33', u'0.539', u'8a69b302-dc49-469e-ae53-959568355cf2']]
    }
    expected = OrderBook(data['sequence'], bids=data['bids'], asks=data['asks'])
    actual = OrderBook(data['sequence'], bids=data['bids'], asks=data['asks'])
    actual.update(order_id='3f681726-9078-4b8c-bfb2-dbc25910b75e', new_size=1)
    num_diff = compare_books(actual, expected)
    assert num_diff == 2
