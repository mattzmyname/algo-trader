from stock_data import get_db_connection, get_minute_historical, index_to_timestamp, get_alpaca_api
import time
import bs4 as bs
import requests
import asyncio


def get_sp500_tickers():
	resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
	soup = bs.BeautifulSoup(resp.text, 'lxml')
	table = soup.find('table', {'class': 'wikitable sortable'})
	tickers = []
	for row in table.findAll('tr')[1:]:
		ticker = row.findAll('td')[0].text.strip()
		tickers.append(ticker)
	
	return tickers


async def insert_one_to_db(symbol):
	e = get_db_connection()
	minute_df = get_minute_historical(symbol)
	minute_df.insert(0, column='symbol', value=symbol)
	minute_df = index_to_timestamp(minute_df)
	minute_df.to_sql(name='minute_stocks', con=e, if_exists='append', index=False)
	e.dispose()


async def main():
	start_time = time.time()
	api = get_alpaca_api()
	sp500 = get_sp500_tickers()
	if not api.get_clock().is_open:
		tasks = []
		for symbol in sp500:
			try:
				tasks.append(insert_one_to_db(symbol))
			except Exception as e:
				print(e)
		await asyncio.gather(*tasks)
	print(f'Completed in {time.time() - start_time} seconds ')


if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
