# GDAX/Bitstamp Arbitrage
When the spread on the two exchanges is greater than some threshold, we buy BTC on the cheaper exchange and sell on the more expensive one. We don't want to have any exposure to BTC so we are never long/short BTC. In order to make sure that we don't have only one side filled, we place a limit buy order on the cheaper exchange and when that fills we immediately execute a market order on the other exchange.

Features
- We want to have an equal amount of BTC and cash on both exchanges in order to take advantage of a big spread in one direction
- We make all calls async to reduce delays

## Tasks

1. Increase the reliability of the currently deployed strategy so that we can add more cash to it.
2. Often the bid-ask spread on Bitstamp is more than $5. Instead of just executing market orders on Bitstamp, we could put in very competitive limit orders to capture some of this spread. To be able to do this safely we will have to have very low latency updates to and from Bitstamp.
3. Instead of placing arbitrary profitable limit orders on Gdax, we could adjust the order with respect to the other orders in the book. For instance, it is worth forgoing ~10 cents to get in front of a large order, similarly it's worth gaining ~30 cents by letting several small orders get in front of our order. Additionally, instead of hardcoding the spreads we want for our cross-exchange trade, we could adapt it to the gdax order book. If I can sell on Gdax and buy on Bitstamp for a $2 profit, whether or not I should take this opportunity depends on the Gdax order book. If there aren't many sell orders, I should wait for a better spread, if there is a huge sell order at $2.10 spread, I should take the $2 spread (since the gap is unlikely to widen, at least from Gdax's perspective).
4. Think about whether we want progressively larger spreads for trades that make our balance worse.
5. Look into unwinding gdax limit orders on gdax itself rather than on bitstamp. This requires having a good model for how likely an order is to be executed depending on its depth in the order book, and how much waiting will harm our execution price on bitstamp (market and limit orders).
6. Sometimes it makes sense to execute a market order on gdax instead of a limit order cause there are too many orders in front of ours, and not enough volume on the opposite side.
7. Making our price updates faster should be better for everything.
8. Add various price forecasts, tune all the parameters via backtesting etc.

## Value Estimates

1. Adding 2x $$$ directly results in 2x profits at our present size (Might need to think harder about order sizes if we add a lot more $$$)
2. This should increase our per trade profit by 1.5x to 2x by adding ~$5 to our spreads.
3. Hard to estimate. This could result in a lot more trades while making up for lower spreads on some trades.
4. TODO
5. TODO, but this could be the way to scale without adding more $$$ to both exchanges.
6. Probably tiny gains, but it's an easy task.
7. TODO
8. TODO
