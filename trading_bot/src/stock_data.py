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


def get_historical_stock_data(symbols=None, days=1, to_csv=False):
	# Only doing one day here as an example
	if symbols is None:
		symbols = get_nyse_symbols()
	
	base_url = 'https://paper-api.alpaca.markets'
	api_key_id = alpaca['api_key']
	api_secret = alpaca['secret_key']
	
	api = tradeapi.REST(
		base_url=base_url,
		key_id=api_key_id,
		secret_key=api_secret
	)
	
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
		df.to_csv('stocks.csv')
	return df


def daily_equity_quotes(symbols=None, to_csv=False):
	api_key = td['consumer_key']
	
	# Check if the market was open today.
	today = datetime.today().astimezone(pytz.timezone("America/New_York"))
	today_fmt = today.strftime('%Y-%m-%d')
	
	# Call the td ameritrade hours endpoint for equities to see if it is open
	market_url = 'https://api.tdameritrade.com/v1/marketdata/EQUITY/hours'
	
	params = {
		'apikey': api_key,
		'date': today_fmt
	}
	
	request = requests.get(
		url=market_url,
		params=params
	).json()
	try:
		if request['equity']['equity']['isOpen'] is False:
			if symbols is None:
				symbols = get_nyse_symbols()
			
			# The TD Ameritrade api has a limit to the number of symbols you can get data for
			# in a single call so we chunk the list into 200 symbols at a time
			def chunks(l, n):
				"""
				Takes in a list and how long you want
				each chunk to be
				"""
				n = max(1, n)
				return (l[i:i + n] for i in range(0, len(l), n))
			
			symbols_chunked = list(chunks(list(set(symbols)), 200))
			
			# Function for the api request to get the data from td ameritrade
			def quotes_request(stocks):
				"""
				Makes an api call for a list of stock symbols
				and returns a dataframe
				"""
				url = r"https://api.tdameritrade.com/v1/marketdata/quotes"
				params = {
					'apikey': api_key,
					'symbol': stocks
				}
				
				resp = requests.get(
					url=url,
					params=params
				).json()
				time.sleep(1)
				return pd.DataFrame.from_dict(
					resp,
					orient='index'
				).reset_index(drop=True)
			
			# Loop through the chunked list of symbols
			# and call the api. Append all the resulting dataframes into one
			df = pd.concat([quotes_request(each) for each in symbols_chunked], sort=False)
			df['date'] = pd.to_datetime(df['date'])
			df['date'] = df['date'].dt.date
			df['divDate'] = pd.to_datetime(df['divDate'])
			df['divDate'] = df['divDate'].dt.date
			df['divDate'] = df['divDate'].fillna(np.nan)
			
			# Remove anything without a price
			df = df.loc[df['bidPrice'] > 0]
			
			# Rename columns and format for bq (can't start with a number)
			df = df.rename(columns={
				'52WkHigh': '_52WkHigh',
				'52WkLow': '_52WkLow'
			})
			
			# Save to csv
			if to_csv:
				df.to_csv('stocks.csv', index=False)
			return df
		
		else:
			# Market Not Open Today
			pass
	except KeyError:
		# Not a weekday
		pass


if __name__ == '__main__':
	start_time = time.time()
	get_historical_stock_data(symbols=['AAPL'], to_csv=True)
	print(f'Completed in {time.time() - start_time} seconds ')
