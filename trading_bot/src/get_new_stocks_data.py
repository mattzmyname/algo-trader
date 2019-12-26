from stock_data import get_tradable_symbols, get_historical_stock_data, get_alpaca_api
from datetime import timedelta, datetime
from pytz import timezone
import time


# READ THIS
# cron this script to the frequency in which we want data during open market


def main():
	tradable = get_tradable_symbols()
	for i in range(len(tradable)):
		symbol = tradable[i:i + 1]
		nyc = timezone('America/New_York')
		today = datetime.today().astimezone(nyc)
		yesterday = today - timedelta(days=10)
		try:
			print(get_historical_stock_data(symbols=symbol, sd=yesterday, to_db=True))
		except:
			pass


if __name__ == '__main__':
	start_time = time.time()
	main()
	print(f'Completed in {time.time() - start_time} seconds ')
