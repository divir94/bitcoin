# Crypto Trader

## Backtest GDAX strategies
- Implement and backtest simple "wall strategy". (*divir*)
- Backtest a model that uses order book imbalance and other features. (*vidur*)

## Backtest GDAX-BITSTAMP strategies
- Backtest ETH-USD arbitrage by collecting Bitstamp data and using new backtester. (*divir/vidur*)
- Backtest BTC-USD arbitrage using existing spreads data. Add realistic delays and use snapshots data to adjust
when the book is thin at the best price. If we feel this works well, we can scale our existing strategy. (*divir*)

## Backtester improvements
- Make backtester faster so we can run through a week's data in 10 min. (*vidur*)
- Create a tool to visualize order book and have "previous" and "next" buttons to simulate running the order book.
This can be useful to get intuition for strategies. Implement "unprocessing" messages to use the "previous" feature.
(*divir*)

## Current Bot
- Backtest ETH-USD arbitrage by improving the PnL system to see how the strategy performed so far. (*divir*)
- Rewrite current bot to use web socket and have better PnL reporting. Maybe send emails everyday. (*divir/vidur*)
