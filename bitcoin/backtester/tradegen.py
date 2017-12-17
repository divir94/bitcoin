import pandas as pd
import numpy as np
import uuid

import bitcoin.backtester.util as butil
import bitcoin.util as util


class TradeGen(object):
    """
    Implements the view by placing orders. Since you can only have one view at every point in time,
    it is currently not possible to have more than one order resting on the book.
    """
    def __init__(self):
        self.sig_price_chg = 1.

        # balance records usd and btc and other accounting measures
        self._balance = []
        self._balance_cols = ['time', 'usd', 'btc', 'price', 'value']

        # orders record how long it took to execute each order and at what price
        # sign of size determines side
        self._orders = {}
        self._orders_cols = ['order_id', 'start_time', 'end_time', 'status', 'time_elapsed', 'price', 'size',
                             'remaining_size']

        # fills record individual fills for orders
        # fill_size is always positive
        self._fills = {}
        self._fills_cols = ['order_id', 'fill_id', 'order_time', 'fill_time', 'price', 'fill_size',
                            'order_size', 'remaining_size']

        self._open_orders = {}

    @property
    @util.memoize
    def balance(self):
        df = pd.DataFrame(self._balance)

        # new columns
        df['value'] = df['usd'] + df['price'] * df['btc']

        # index
        df = df[self._balance_cols].set_index('time')
        return df

    @property
    @util.memoize
    def orders(self):
        df = pd.DataFrame.from_dict(self._orders, orient='index')

        # new columns
        df['time_elapsed'] = (df.end_time - df.start_time).dt.total_seconds()

        # index
        df = df[self._orders_cols].set_index('order_id').sort_values('start_time')
        return df

    @property
    @util.memoize
    def fills(self):
        df = pd.DataFrame.from_dict(self._fills, orient='index')

        # index
        df = df[self._fills_cols].set_index(['order_id', 'fill_id']).sort_values('fill_time')
        return df

    @property
    def exposure(self):
        """
        Exposure tracks the view and is equal to the amount of crypto held (not including open orders).

        Returns
        -------
        int
        """
        return self._balance[-1]['btc'] if self._balance else 0

    def run(self, view, message, book):
        """
        Get fills and place orders.

        Parameters
        ----------
        view: namedtuple
        message: dict
        book: GdaxOrderBook

        Returns
        -------
        None
        """
        fill_inst = self.get_fill_instructions(open_orders=self._open_orders.values(),
                                               message=message,
                                               book=book)
        self.handle_instructions(fill_inst)

        order_inst = self.get_order_instructions(open_orders=self._open_orders.values(),
                                                 view=view,
                                                 exposure=self.exposure,
                                                 book=book,
                                                 sig_price_chg=self.sig_price_chg)
        self.handle_instructions(order_inst)

    def handle_instructions(self, instructions):
        """
        Instructions update balance, orders and fills.

        Parameters
        ----------
        instructions: list

        Returns
        -------
        None
        """
        for inst in instructions:
            if isinstance(inst, butil.FillOrder):
                self.handle_fill_order(inst)

            elif isinstance(inst, butil.DoneOrder):
                self.handle_done_order(inst)

            elif isinstance(inst, butil.OpenOrder):
                self.handle_open_order(inst)

            else:
                raise ValueError('Invalid instruction {}'.format(inst))

    def handle_open_order(self, open_inst):
        """
        Add to orders and open_orders.

        Parameters
        ----------
        open_inst: namedtuple

        Returns
        -------
        None
        """
        order_id = uuid.uuid4().hex

        order = dict(
            order_id=order_id,
            price=open_inst.price,
            size=open_inst.size,
            status='open',
            remaining_size=abs(open_inst.size),
            start_time=open_inst.time,
            end_time=None,
        )

        # add to orders and open orders
        self._orders[order_id] = order
        self._open_orders[order_id] = order

    def handle_fill_order(self, fill_inst):
        """
        Update orders, add to bill and add to balance.

        Parameters
        ----------
        fill_inst: namedtuple

        Returns
        -------
        None
        """
        fill_id = uuid.uuid4().hex

        # update remaining size of order
        remaining_size = self._orders[fill_inst.order_id]['remaining_size']
        assert fill_inst.fill_size <= remaining_size, (
            'remaining size: {}, order fill: {}'.format(remaining_size, fill_inst.fill_size)
        )
        self._orders[fill_inst.order_id]['remaining_size'] -= fill_inst.fill_size

        # get order
        order = self._orders[fill_inst.order_id]

        # add to fills
        self._fills[fill_id] = dict(
            fill_id=fill_id,
            order_id=fill_inst.order_id,
            fill_time=fill_inst.fill_time,
            price=order['price'],
            fill_size=fill_inst.fill_size,
            remaining_size=order['remaining_size'],
            order_time=order['start_time'],
            order_size=order['size'],
        )

        # update balance
        size = np.sign(order['size']) * fill_inst.fill_size
        prev_usd = self._balance[-1]['usd'] if self._balance else 0
        prev_btc = self._balance[-1]['btc'] if self._balance else 0
        usd = prev_usd + order['price'] * -size
        btc = prev_btc + size

        self._balance.append(dict(
            time=fill_inst.fill_time,
            usd=usd,
            btc=btc,
            price=order['price'],
        ))

    def handle_done_order(self, done_inst):
        """
        Done order is sent for cancelled or filled orders. Updates orders and open orders.

        Parameters
        ----------
        done_inst: namedtuple

        Returns
        -------
        None
        """
        self._orders[done_inst.order_id]['end_time'] = done_inst.time
        self._orders[done_inst.order_id]['status'] = done_inst.status

        # remove from open orders
        del self._open_orders[done_inst.order_id]

    @staticmethod
    def get_fill_instructions(open_orders, message, book):
        """
        Get fill and done instructions.

        Parameters
        ----------
        open_orders: list[dict]
        message: dict
        book: GdaxOrderBook

        Returns
        -------
        list
        """
        fill_inst = []
        if not message['type'] == 'match':
            return fill_inst

        # the message side field indicates the maker order side
        maker_side = message['side']
        match_price = message['price']
        match_size = message['size']
        # if maker_time is None, it means that the maker order existed before we got a snapshot and
        # thus we don't have a timestamp. In this case, our order was after the maker order
        maker_time = book.order_to_time.get(message['maker_order_id'])

        for order in open_orders:
            # our order is competitive if it has a better price than the match price
            competitive_price = order['price'] > match_price if order['size'] > 0 else order['price'] < match_price
            # match at same price if our order came before. Note that order.timestamp < None is False and implies
            # that maker order is created before we got the snapshot
            earlier_time = maker_time and (order['start_time'] < maker_time)
            early_at_same_price = (order['price'] == match_price) and earlier_time
            # order has to be same side as maker i.e. opposite side of taker
            same_side_as_maker = butil.SIDE_DICT[maker_side] == np.sign(order['size'])
            if same_side_as_maker and (competitive_price or early_at_same_price):
                fill_size = min(match_size, order['remaining_size'])

                # fill order
                fill_inst.append(
                    butil.FillOrder(order_id=order['order_id'], fill_time=book.timestamp, fill_size=fill_size)
                )

                # done order
                if fill_size == order['remaining_size']:
                    fill_inst.append(
                        butil.DoneOrder(order_id=order['order_id'], time=book.timestamp, status='filled')
                    )

        return fill_inst

    # TODO(divir): break this into smaller and more manageable functions
    @staticmethod
    def get_order_instructions(open_orders, view, exposure, book, sig_price_chg):
        """
        Get open cancel order instructions.

        Parameters
        ----------
        open_orders: list[dict]
        view: namedtuple
        exposure: float
        book: GdaxOrderBook
        sig_price_chg: float

        Returns
        -------
        list
        """
        eps = 1e-4
        cancel_inst = []
        open_inst = []

        target_size = view.size - exposure
        target_bid, target_ask = butil.get_competitive_prices(book)
        target_price = min(target_bid, view.price) if target_size >= 0 else max(target_ask, view.price)

        # if exposure is same as view, cancel all orders
        if np.isnan(target_size) or abs(target_size) < eps:
            for order in open_orders:
                cancel_inst.append(
                    butil.DoneOrder(order_id=order['order_id'], time=book.timestamp, status='cancelled')
                )
            return cancel_inst

        if open_orders:
            # track exposure in open orders
            open_exposure = 0

            # cancel all orders on opposite side or with significant price change
            for order in open_orders:
                opp_side = np.sign(order['size']) != np.sign(target_size)
                price_close = util.is_close(order['price'], target_price, abs_tol=sig_price_chg)

                if opp_side or not price_close:
                    cancel_inst.append(
                        butil.DoneOrder(order_id=order['order_id'], time=book.timestamp, status='cancelled')
                    )
                else:
                    open_exposure += np.sign(order['size']) * order['remaining_size']

            # adjust target size and place new orders
            if abs(open_exposure) > abs(target_size):
                assert np.sign(open_exposure) == np.sign(target_size), '{}, {}'.format(open_exposure, target_size)
                while abs(open_exposure) > abs(target_size):
                    # cut down open exposure
                    order = open_orders.pop()
                    cancel_inst.append(
                        butil.DoneOrder(order_id=order['order_id'], time=book.timestamp, status='cancelled')
                    )
                    open_exposure -= np.sign(order['size']) * order['remaining_size']

            # open new order
            size = target_size - open_exposure
            if abs(size) > eps:
                open_inst.append(
                    butil.OpenOrder(time=book.timestamp, price=target_price, size=size)
                )
        else:
            # no open orders. create new order with target size
            open_inst.append(
                butil.OpenOrder(time=book.timestamp, price=target_price, size=target_size)
            )

        all_orders = open_inst + cancel_inst
        return all_orders
