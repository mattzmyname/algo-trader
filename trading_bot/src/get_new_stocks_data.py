from stock_data import get_db_connection, get_minute_historical, index_to_timestamp, get_alpaca_api
import time
import bs4 as bs
import requests
import pandas as pd


# READ THIS
# cron this script to the frequency in which we want data during open market

def get_sp500_tickers():
	resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
	soup = bs.BeautifulSoup(resp.text, 'lxml')
	table = soup.find('table', {'class': 'wikitable sortable'})
	tickers = []
	for row in table.findAll('tr')[1:]:
		ticker = row.findAll('td')[0].text.strip()
		if ticker != 'JEC':
			tickers.append(ticker)
	
	return tickers


def main():
	api = get_alpaca_api()
	sp500 = get_sp500_tickers()
	if not api.get_clock().is_open:
		data = []
		for symbol in sp500:
			minute_df = get_minute_historical(symbol)
			minute_df.insert(0, column='symbol', value=symbol)
			data.append(minute_df)
			time.sleep(.01)
		
		df = pd.concat([each for each in data], sort=False)
		df = index_to_timestamp(df)
		try:
			e = get_db_connection()
			df.to_sql(name='minute_stocks', con=e, if_exists='append', chunksize=1000, index=False)
			e.dispose()
		except Exception as e:
			print(e)


if __name__ == '__main__':
	start_time = time.time()
	main()
	print(f'Completed in {time.time() - start_time} seconds ')
