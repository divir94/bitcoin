import pandas as pd
from sortedcontainers import SortedList

import bitcoin.logs.logger as lc
import bitcoin.storage.util as sutil

import bitcoin.backtester.viewgen as vg
import bitcoin.backtester.tradegen as tg


logger = lc.config_logger('backtester', level='DEBUG', file_handler=False)


class BackTester(object):
    def __init__(self, strategy):
        self.viewgen = vg.ViewGen(strategy)
        self.tradegen = tg.TradeGen()

    def run(self, data, start=None, end=None, run_tradegen=False):
        """
        Run viewgen and tradegen. Viewgen outputs how much and at what price to buy for and tradegen implements
        the view by placing limit orders.

        Parameters
        ----------
        data: namedtuple
            has book and messages as fields
        start: datetime or str
        end: datetime or str
            used to end the backtester early
        run_tradegen: bool

        Returns
        -------
        self.result
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)
        book = None
        messages = data.messages

        if not data.messages:
            logger.error('No messages found!')
            return

        for msg in messages:
            # continue till start
            if start and msg['time'] < start:
                continue

            # return when reached the end
            if end and book is not None and book.timestamp > end:
                return

            # get a new book if no book defined or missing sequence
            if not book or msg['sequence'] > book.sequence + 1:
                book = self.get_next_book(sequence=msg['sequence'],
                                          timestamp=msg['time'],
                                          books_df=data.books)
                # no more books available
                if book is None:
                    return
                # cancel open orders
                self.tradegen.cancel_all_orders(msg['time'])

            # skip earlier messages
            if book is not None and msg['sequence'] < book.sequence:
                continue

            # received messages have no impact, but increment book
            if msg['type'] == 'received':
                book.sequence = msg['sequence']
                continue

            # run viewgen and tradegen
            view = self.viewgen.run(book=book)

            if run_tradegen:
                self.tradegen.run(view=view, message=msg, book=book)

            # update book
            book.process_message(msg)

    @staticmethod
    def get_next_book(sequence, timestamp, books_df):
        """
        Get next book after the sequence if available.

        Parameters
        ----------
        sequence: int
        timestamp: datetime
        books_df: DataFrame
            multiple books

        Returns
        -------
        GdaxOrderBook or None
        """
        if books_df.empty:
            logger.error('No books available.')
            return

        # get index of next sequence
        seq_list = SortedList(books_df['sequence'].unique())
        next_seq_idx = seq_list.bisect_left(sequence)

        if next_seq_idx >= len(seq_list):
            logger.error('No book found after {} ({}). Available books: {}'.format(timestamp, sequence, seq_list))
            return

        # next book
        next_seq = seq_list[next_seq_idx]
        book_df = books_df[books_df['sequence'] == next_seq]
        book = sutil.df_to_book(book_df)
        logger.info('Current message at {} ({}). Got book at {} ({})'.format(
            timestamp, sequence, book.sequence, book.timestamp)
        )
        return book
