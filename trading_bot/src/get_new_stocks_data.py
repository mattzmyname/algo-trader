from stock_data import get_db_connection, get_minute_historical, index_to_timestamp, get_alpaca_api
import time
import bs4 as bs
import requests


def get_sp500_tickers():
	resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
	soup = bs.BeautifulSoup(resp.text, 'lxml')
	table = soup.find('table', {'class': 'wikitable sortable'})
	tickers = []
	for row in table.findAll('tr')[1:]:
		ticker = row.findAll('td')[0].text.strip()
		tickers.append(ticker)
	
	return tickers


if __name__ == '__main__':
	start_time = time.time()
	api = get_alpaca_api()
	if api.get_clock().is_open:
		for symbol in get_sp500_tickers():
			try:
				minute_df = get_minute_historical(symbol)
				e = get_db_connection()
				minute_df.insert(0, column='symbol', value=symbol)
				minute_df = index_to_timestamp(minute_df)
				minute_df.to_sql(name='minute_stocks', con=e, if_exists='append', index=False)

			except Exception as e:
				print(e)
	print(f'Completed in {time.time() - start_time} seconds ')

