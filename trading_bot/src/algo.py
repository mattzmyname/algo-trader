import alpaca_trade_api as tradeapi
import requests
import time
import functools
import asyncio
import stock_data
from ta.trend import macd
import numpy as np
from datetime import datetime, timedelta
from pytz import timezone
from credentials import alpaca

base_url = 'https://paper-api.alpaca.markets'
api_key_id = alpaca['api_key']
api_secret = alpaca['secret_key']
api = tradeapi.REST(
	base_url=base_url,
	key_id=api_key_id,
	secret_key=api_secret
)

session = requests.session()

# We only consider stocks with per-share prices inside this range
min_share_price = 5.0
max_share_price = 50
# Minimum previous-day dollar volume for a stock we might consider
min_last_dv = 1000000
# Stop limit to default to
default_stop = .95
# How much of our portfolio to allocate to any one position
max_allocation = 0.01


@functools.lru_cache(maxsize=128)
def get_current_portfolio_value():
	return float(api.get_account().portfolio_value)


def get_1000m_history_data(symbols):
	print('Getting historical data...')
	minute_history = {}
	c = 0
	for symbol in symbols:
		minute_history[symbol] = stock_data.get_minute_historical(symbol, num_minutes=1000)
		c += 1
		print('{}/{}'.format(c, len(symbols)))
	print('Success.')
	return minute_history


def get_tickers():
	print('Getting current ticker data...')
	assets = api.list_assets()
	symbols = {asset.symbol for asset in assets if asset.tradable}
	tickers = api.polygon.all_tickers()
	print('Success.')
	
	return [ticker for ticker in tickers if (
			ticker.ticker in symbols and
			ticker.lastTrade['p'] >= min_share_price and
			ticker.lastTrade['p'] <= max_share_price and
			ticker.prevDay['v'] * ticker.lastTrade['p'] > min_last_dv and
			ticker.todaysChangePerc >= 3.5
	)]


def find_stop(current_value, minute_history, now):
	# this functions finds the price of the most recent price valley eg. 26 -> {24} -> 25
	# otherwise limit our loss to 5%
	series = minute_history['low'][-100:]
	diff = np.diff(series.values)
	low_index = np.where((diff[:-1] <= 0) & (diff[1:] > 0))[0] + 1
	if len(low_index) > 0:
		return series[low_index[-1]] - 0.01
	return current_value * default_stop


def run(tickers, market_open_dt, market_close_dt):
	# Establish streaming connection
	conn = tradeapi.StreamConn(base_url=base_url, key_id=api_key_id, secret_key=api_secret)
	# Update initial state with information from tickers
	volume_today = {}
	prev_closes = {}
	for ticker in tickers:
		symbol = ticker.ticker
		prev_closes[symbol] = ticker.prevDay['c']
		volume_today[symbol] = ticker.day['v']
	
	# generate our list of watched symbols
	symbols = {ticker.ticker for ticker in tickers}
	print('Tracking {} symbols.'.format(len(symbols)))
	minute_history = get_1000m_history_data(symbols)
	open_orders = {}
	positions = {}
	
	# Cancel any existing open orders on watched symbols
	existing_orders = api.list_orders(limit=500)
	for order in existing_orders:
		if order.symbol in symbols:
			api.cancel_order(order.id)
	
	stop_prices = {}
	latest_cost_basis = {}
	
	# Track any positions bought during previous executions
	existing_positions = api.list_positions()
	for position in existing_positions:
		symbols.add(position.symbol)
		# if position.symbol in symbols:
		positions[position.symbol] = float(position.qty)
		# Recalculate cost basis and stop price
		latest_cost_basis[position.symbol] = float(position.cost_basis)
		stop_prices[position.symbol] = (
				float(position.cost_basis) * default_stop
		)  # limit our loss to 5% from cost basis
		if position.symbol not in minute_history:
			minute_history[position.symbol] = stock_data.get_minute_historical(position.symbol, num_minutes=1000)
	# Keep track of what we're buying/selling
	target_prices = {}
	partial_fills = {}
	
	# Use trade updates to keep track of our portfolio
	@conn.on(r'trade_update')
	async def handle_trade_update(conn, channel, data):
		symbol = data.order['symbol']
		last_order = open_orders.get(symbol)
		if last_order is not None:
			event = data.event
			if event == 'partial_fill':
				qty = int(data.order['filled_qty'])
				if data.order['side'] == 'sell':
					qty = qty * -1
				positions[symbol] = (
						positions.get(symbol, 0) - partial_fills.get(symbol, 0)
				)
				partial_fills[symbol] = qty
				positions[symbol] += qty
				open_orders[symbol] = data.order
				print(f"Filled partial {data.order['side']} order. {abs(qty)} shares of {symbol} @ {data.order['filled_avg_price']} ")
			
			elif event == 'fill':
				qty = int(data.order['filled_qty'])
				if data.order['side'] == 'sell':
					qty = qty * -1
				positions[symbol] = (
						positions.get(symbol, 0) - partial_fills.get(symbol, 0)
				)
				partial_fills[symbol] = 0
				positions[symbol] += qty
				open_orders[symbol] = None
				print(f"Filled {data.order['side']} order. {abs(qty)} shares of {symbol} @ {data.order['filled_avg_price']} ")
			elif event == 'canceled' or event == 'rejected':
				partial_fills[symbol] = 0
				open_orders[symbol] = None
	
	@conn.on(r'A$')
	async def handle_second_bar(conn, channel, data):
		# First, aggregate 1s bars for up-to-date MACD calculations
		ts = data.start
		ts -= timedelta(seconds=ts.second, microseconds=ts.microsecond)
		try:
			current = minute_history[data.symbol].loc[ts]
		except KeyError:
			current = None
		if current is None:
			new_data = [
				data.open,
				data.high,
				data.low,
				data.close,
				data.volume
			]
		else:
			new_data = [
				current.open,
				max(data.high, current.high),
				min(data.low, current.low),
				data.close,
				current.volume + data.volume
			]
		minute_history[data.symbol].loc[ts] = new_data
		# Next, check for existing orders for the stock
		existing_order = open_orders.get(data.symbol)
		if existing_order is not None:
			# Make sure the order's not too old
			submission_ts = existing_order.submitted_at.astimezone(
				timezone('America/New_York')
			)
			order_lifetime = ts - submission_ts
			if order_lifetime.seconds // 60 > 1:
				# Cancel it so we can try again for a fill
				api.cancel_order(existing_order.id)
			return
		
		# Now we check to see if it might be time to buy or sell
		since_market_open = ts - market_open_dt
		until_market_close = market_close_dt - ts
		if (
				15 < since_market_open.seconds // 60 < 60
		):
			# Check for buy signals
			# See if we've already bought in first
			if positions.get(data.symbol, 0) > 0:
				print(f"Existing order for {data.symbol}")
				return
			
			# See how high the price went during the first 15 minutes
			lbound = market_open_dt
			ubound = lbound + timedelta(minutes=15)
			try:
				high_15m = minute_history[data.symbol][lbound:ubound]['high'].max()
			except Exception as e:
				# Because we're aggregating on the fly, sometimes the datetime
				# index can get messy until it's healed by the minute bars
				return
			
		
			# Get the change since yesterday's market close
			daily_pct_change = (
					(data.close - prev_closes[data.symbol]) / prev_closes[data.symbol]
			)
			if (
					daily_pct_change > .04 and
					data.close > high_15m and
					volume_today[data.symbol] > 30000
			):
				# check for a positive, increasing MACD
				hist = macd(
					minute_history[data.symbol]['close'].dropna(),
					n_fast=12,
					n_slow=26
				)
				if (
						hist[-1] < 0 or
						not (hist[-3] < hist[-2] < hist[-1])
				):
					return
				hist = macd(
					minute_history[data.symbol]['close'].dropna(),
					n_fast=40,
					n_slow=60
				)
				if hist[-1] < 0 or np.diff(hist)[-1] < 0:
					return
				
				# Stock has passed all checks; figure out how much to buy
				stop_price = find_stop(
					data.close, minute_history[data.symbol], ts
				)
				stop_prices[symbol] = stop_price
				target_prices[data.symbol] = data.close + (
						(data.close - stop_price) * 3
				)
				# buy enough shares to account for 1% of portfolio
				shares_to_buy = get_current_portfolio_value() * max_allocation // data.close
				
				if shares_to_buy == 0:
					shares_to_buy = 1
				shares_to_buy -= positions.get(data.symbol, 0)
				
				if shares_to_buy < 1 or data.close - stop_price <= 0 or shares_to_buy * data.close > float(api.get_account().cash):
					# do not buy if the price is below or stop price
					# or we do not have enough cash
					return
					
				print('Submitting buy for {} shares of {} at {}'.format(
					shares_to_buy, data.symbol, data.close
				))
				try:
					o = api.submit_order(
						symbol=data.symbol, qty=str(shares_to_buy), side='buy',
						type='limit', time_in_force='day',
						limit_price=str(data.close)
					)
					open_orders[symbol] = o
					latest_cost_basis[data.symbol] = data.close
				except Exception as e:
					print(e)
				return
		if (
				since_market_open.seconds // 60 >= 15 and
				until_market_close.seconds // 60 > 15
		):
			# Check for liquidation signals
			
			# We can't liquidate if there's no position
			if positions.get(data.symbol, 0) == 0:
				return
			
			# Sell for a loss if it's fallen below our stop price
			# Sell for a loss if it's below our cost basis and MACD < 0
			# Sell for a profit if it's above our target price
			hist = macd(
				minute_history[data.symbol]['close'].dropna(),
				n_fast=12,
				n_slow=21
			)
			if (
					data.close <= stop_prices[data.symbol] or
					(data.close >= target_prices[data.symbol] and hist[-1] <= 0) or
					(data.close <= latest_cost_basis[data.symbol] and hist[-1] <= 0)
			):
				print('Submitting sell for {} shares of {} at {}'.format(
					positions.get(data.symbol, 0), data.symbol, data.close
				))
				try:
					o = api.submit_order(
						symbol=data.symbol, qty=str(positions.get(data.symbol, 0)), side='sell',
						type='limit', time_in_force='day',
						limit_price=str(data.close)
					)
					open_orders[data.symbol] = o
					latest_cost_basis[data.symbol] = data.close
				except Exception as e:
					print(e)
			return
		elif until_market_close.seconds // 60 <= 15:
			# Liquidate remaining positions on watched symbols at market
			try:
				position = api.get_position(data.symbol)
			except Exception as e:
				# Exception here indicates that we have no position
				return
			print('Trading over, liquidating remaining position in {}'.format(
				data.symbol)
			)
			api.submit_order(
				symbol=data.symbol, qty=position.qty, side='sell',
				type='market', time_in_force='day'
			)
			symbols.remove(data.symbol)
			if len(symbols) <= 0:
				conn.close()
			conn.deregister([
				'A.{}'.format(data.symbol),
				'AM.{}'.format(data.symbol)
			])

	# Replace aggregated 1s bars with incoming 1m bars
	@conn.on(r'AM$')
	async def handle_minute_bar(conn, channel, data):
		ts = data.start
		ts -= timedelta(microseconds=ts.microsecond)
		minute_history[data.symbol].loc[ts] = [
			data.open,
			data.high,
			data.low,
			data.close,
			data.volume
		]
		# insert bar into minute_stock db
		
		minute_data = {
			'timestamp': ts.to_pydatetime(),
			'symbol': data.symbol,
			'open': data.open,
			'high': data.high,
			'low': data.low,
			'close': data.close,
			'volume': data.volume
		}
		loop = asyncio.get_running_loop()
		await stock_data.a_insert(loop, "minute_stocks", minute_data)
		volume_today[data.symbol] += data.volume
	
	channels = ['trade_updates']
	for symbol in symbols:
		symbol_channels = ['A.{}'.format(symbol), 'AM.{}'.format(symbol)]
		channels += symbol_channels
	print('Watching {} symbols.'.format(len(symbols)))
	if len(symbols) > 0:
		run_ws(conn, channels)


# Handle failed websocket connections by reconnecting
def run_ws(conn, channels):
	try:
		conn.run(channels)
	except Exception as e:
		print(e)
		print("Reconnecting...")
		conn.close()
		run_ws(conn, channels)


def main():
	market_open, market_close = stock_data.trading_times()
	# Wait until just before we might want to trade
	current_dt = datetime.today().astimezone(timezone('America/New_York'))
	since_market_open = current_dt - market_open
	while since_market_open.seconds // 60 <= 14:
		time.sleep(1)
		since_market_open = current_dt - market_open
	run(get_tickers(), market_open, market_close)


if __name__ == "__main__":
	main()
