import pandas as pd
import numpy as np
import requests
import pytz
import string
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from credentials import td, alpaca
import alpaca_trade_api as tradeapi


# Price History API Documentation
# https://developer.tdameritrade.com/price-history/apis/get/marketdata/%7Bsymbol%7D/pricehistory

# Function to turn a datetime object into unix
def unix_time_millis(dt):
	epoch = datetime.utcfromtimestamp(0)
	return int((dt - epoch).total_seconds() * 1000.0)


def epoch_to_dt(epoch):
	return datetime.fromtimestamp(epoch).strftime('%c')


# Get the historical dates you need.
def get_nyse_symbols():
	# Get a current list of all the stock symbols for the NYSE
	alpha = list(string.ascii_uppercase)

	symbols = []

	for each in alpha:
		url = 'http://eoddata.com/stocklist/NYSE/{}.htm'.format(each)
		resp = requests.get(url)
		site = resp.content
		soup = BeautifulSoup(site, 'html.parser')
		table = soup.find('table', {'class': 'quotes'})
		for row in table.findAll('tr')[1:]:
			symbols.append(row.findAll('td')[0].text.rstrip())

	# Remove the extra letters on the end
	symbols_clean = []

	for each in symbols:
		each = each.replace('.', '-')
		symbols_clean.append((each.split('-')[0]))

	return symbols_clean


def get_alpaca_api():
	base_url = 'https://paper-api.alpaca.markets'
	api_key_id = alpaca['api_key']
	api_secret = alpaca['secret_key']

	api = tradeapi.REST(
		base_url=base_url,
		key_id=api_key_id,
		secret_key=api_secret
	)
	return api


def get_historical_stock_data(symbols=None, days=1, to_csv=False):
	# Only doing one day here as an example
	if symbols is None:
		symbols = get_nyse_symbols()

	api = get_alpaca_api()

	from_day = (datetime.today() - timedelta(days=days)).astimezone(pytz.timezone("America/New_York"))
	from_day_fmt = from_day.strftime('%Y-%m-%d')
	today = datetime.today().astimezone(pytz.timezone("America/New_York"))
	today_fmt = today.strftime('%Y-%m-%d')

	hist_data = []
	for symbol in symbols:
		data = api.polygon.historic_agg_v2(
			symbol=symbol, multiplier=30, timespan='minute', limit=1000, _from=from_day_fmt, to=today_fmt
		).df
		data.insert(0, column='symbol', value=symbol)
		hist_data.append(data)

	df = pd.concat([each for each in hist_data], sort=False)
	if to_csv:
		df.to_csv('stocks.csv')  # TODO change to database insert later
	return df


def daily_equity_quotes(symbols=None, to_csv=False):
	api = get_alpaca_api()
	try:
		market_status = api.get_clock()
		if market_status.is_open is True:
			print("open")
		# get today's stock info
		else:
			print("Market Not Open Today")

	except KeyError:
		# Not a weekday
		pass


if __name__ == '__main__':
	start_time = time.time()
	daily_equity_quotes(symbols=['AAPL'], to_csv=True)
	print(f'Completed in {time.time() - start_time} seconds ')
