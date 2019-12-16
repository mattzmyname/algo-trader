import pandas as pd
import numpy as np
import requests
import pytz
import string
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from credentials import td, alpaca, rds
from sqlalchemy import create_engine
from pytz import timezone
import alpaca_trade_api as tradeapi


# Price History API Documentation
# https://developer.tdameritrade.com/price-history/apis/get/marketdata/%7Bsymbol%7D/pricehistory

# Function to turn a datetime object into unix
def unix_time_millis(dt):
	epoch = datetime.utcfromtimestamp(0)
	return int((dt - epoch).total_seconds() * 1000.0)


def epoch_to_dt(epoch):
	return datetime.fromtimestamp(epoch).strftime('%c')


def get_db_connection():
	user = rds['user']
	password = rds['password']
	host = "db-1.cztzalypzdly.us-east-1.rds.amazonaws.com"
	connect_str = f"mysql+pymysql://{user}:{password}@{host}:3306/mydb"
	engine = create_engine(connect_str, echo=True)
	
	return engine


def index_to_timestamp(df):
	df.reset_index(level=0, inplace=True)
	df = df.rename(columns={'index': 'timestamp'})
	df['timestamp'] = df['timestamp'].astype('datetime64')
	df['timestamp'].apply(lambda x: pd.to_datetime(x).tz_localize('US/Eastern'))
	
	return df


def trading_times():
	# Get when the market opens or opened today
	nyc = timezone('America/New_York')
	today = datetime.today().astimezone(nyc)
	today_str = datetime.today().astimezone(nyc).strftime('%Y-%m-%d')
	calendar = get_alpaca_api().get_calendar(start=today_str, end=today_str)[0]
	market_open = today.replace(
		hour=calendar.open.hour,
		minute=calendar.open.minute,
		second=0
	)
	market_open = market_open.astimezone(nyc)
	market_close = today.replace(
		hour=calendar.close.hour,
		minute=calendar.close.minute,
		second=0
	)
	market_close = market_close.astimezone(nyc)
	return market_open, market_close


# Get the historical dates you need.
def get_tradable_symbols():
	# Get a current list of all the stock symbols
	assets = get_alpaca_api().list_assets()
	symbols = [asset.symbol for asset in assets if asset.tradable]
	return symbols


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


def get_historical_stock_data(symbols=None, days=1, to_db=False):
	# Only doing one day here as an example
	if symbols is None:
		symbols = get_tradable_symbols()
	
	api = get_alpaca_api()
	
	from_day = (datetime.today() - timedelta(days=days)).astimezone(pytz.timezone("America/New_York"))
	from_day_fmt = from_day.strftime('%Y-%m-%d')
	today = datetime.today().astimezone(pytz.timezone("America/New_York"))
	today_fmt = today.strftime('%Y-%m-%d')
	
	hist_data = []
	for symbol in symbols:
		try:
			data = api.polygon.historic_agg_v2(
				symbol=symbol, multiplier=1, timespan='hour', _from=from_day_fmt, to=today_fmt
			).df
			data.insert(0, column='symbol', value=symbol)
			hist_data.append(data)
		except:
			print(f"No data for {symbol}")
	
	df = pd.concat([each for each in hist_data], sort=False)
	df = index_to_timestamp(df)
	if to_db:
		e = get_db_connection()
		df.to_sql(name='stocks', con=e, if_exists='append', chunksize=1000, index=False)
	return df


def daily_equity_prices(symbols=None):
	api = get_alpaca_api()
	try:
		today = datetime.today()
		if api.get_calendar(today, today) == today:
			return get_historical_stock_data(symbols, days=0, to_db=False)
		# get today's stock info
		else:
			print("Market Not Open Today")
	
	except KeyError:
		# Not a weekday
		pass


if __name__ == '__main__':
	start_time = time.time()
	print(get_historical_stock_data(days=100, to_db=True))
	print(f'Completed in {time.time() - start_time} seconds ')
