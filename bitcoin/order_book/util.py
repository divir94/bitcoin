from cdecimal import Decimal


def order_book_data_to_set(data):
    """order book json to set of (price, size, order_id)"""
    orders = data['bids'] + data['asks']
    result = {(Decimal(price), Decimal(size), order_id) for price, size, order_id in orders}
    return result


def compare_books(actual, expected):
    actual = actual.to_set()
    expected = expected.to_set()
    extra = actual.difference(expected)
    missing = expected.difference(actual)
    result = len(extra.union(missing))
    return result
