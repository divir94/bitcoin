import pandas as pd
import cufflinks as cf

import bitcoin.strategies.util as autil
import bitcoin.util as util

import bitcoin.backtester.util as butil


cf.go_offline()


class ViewGen(object):
    """
    ViewGen takes a strategy and computers a view (number) i.e. desired exposure at every point in time.
    ViewGen also generates a tear sheet comprising of tables and charts to analyze the view.
    """
    def __init__(self, strategy):
        """

        Parameters
        ----------
        strategy: Strategy object
            implements rebalance method
        """
        self.strategy = strategy
        self._context = butil.Context()

    def run(self, book):
        """
        Generate the view by calling strategy's rebalance method and updates `self.result` with the view and
        other variables from the strategy.

        Parameters
        ----------
        book: GdaxOrderBook

        Returns
        -------
        view: namedtuple
            size: +1, 0 or -1
            price: best price to pay
        """
        view = self.strategy.rebalance(self._context, book)
        return view

    @property
    @util.memoize
    def result(self):
        """
        Return a DataFrame with only view changes.

        Returns
        -------
        pd.DataFrame
            index: time
            columns: [price, view, view_diff, returns, cum_return ...]
        """
        df = self._context.result
        df['view_diff'] = df['view'].diff()
        df = df[df['view_diff'].abs() > 0]
        # return
        df['fwd_price_change'] = df['price'].shift(-1) - df['price']
        df['returns'] = df['fwd_price_change'] * df['view']
        df['cum_return'] = df['returns'].cumsum()
        return df

    def create_tear_sheet(self):
        """
        Visualizes the result of viewgen.
        """
        self.returns_tear()
        self.trades_dist_tear()
        autil.get_mom_deciles(self.result.price)

    def returns_tear(self):
        """
        Create the following charts:
            1. Dollar cumulative return and price vs time
            2. Dollar return and price vs time
            3. Dollar return distribution
            4. View time series
            5. Mean return by view
        """
        df = self.result

        # cum return chart
        chart_df = df[['price', 'cum_return']].fillna(method='pad')
        chart_df.iplot(title='Cumulative Return and Price',
                       yTitle='Dollar Cumulative Return',
                       secondary_y='price',
                       secondary_y_title='Price')

        # return chart
        chart_df = df[['price', 'returns']].fillna(method='pad')
        chart_df.iplot(title='Return and Price',
                       yTitle='Dollar Return',
                       secondary_y='price',
                       secondary_y_title='Price')

        # return distribution chart
        chart_df = df.loc[df['view'] != 0, 'returns'].dropna()
        chart_df.iplot(title='Return Distribution',
                       kind='hist',
                       xTitle='Return',
                       yTitle='Occurrences')

        # view time series
        figure = df[['view', 'price']].iplot(title='View and Price',
                                             kind='area',
                                             fill=True,
                                             secondary_y='price',
                                             yTitle='View',
                                             secondary_y_title='Price',
                                             asFigure=True)
        figure['data'][0]['line']['shape'] = 'hv'
        cf.iplot(figure)

        # view statistics
        return_group = df.groupby(df.view)['fwd_price_change']
        view_stats = return_group.describe()
        view_stats['cum_return'] = return_group.sum()
        print view_stats
        view_stats['mean'].iplot(title='Mean Return by View',
                                 kind='bar',
                                 xTitle='View',
                                 yTitle='Mean Dollar Return')

        return

    def trades_dist_tear(self):
        """
        Plot number of trades over time.
        """
        num_trades = self.result.groupby(pd.TimeGrouper('1T')).size()
        num_trades.iplot(title='Number of Trades', xTitle='Time', yTitle='Num Trades')
