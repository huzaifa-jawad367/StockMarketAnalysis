import alpaca_trade_api as tradeapi
import pandas as pd
import sqlite3

class AlpacaStockScraper:
    def __init__(self, companies, start_date, end_date, timeframe='day',
                 api_key='PKCNI0Y0XAHDTQ0X45I1', api_secret='y3OMojQ17W2SsnaKsBoTj0Xcq4Ah34kWemeRSo4U',
                 base_url='https://paper-api.alpaca.markets/v2',
                 db_filename='stock_data_hourly.db', table_name='prices'):
        """
        Initialize the AlpacaStockScraper with company data, date range, Alpaca API credentials, and SQLite configuration.

        :param companies: Dictionary mapping company names to ticker symbols.
        :param start_date: Start date for historical data (YYYY-MM-DD).
        :param end_date: End date for historical data (YYYY-MM-DD).
        :param timeframe: Time interval of bars (e.g., 'minute', 'day').
        :param api_key: Alpaca API key.
        :param api_secret: Alpaca API secret.
        :param base_url: Alpaca API base URL.
        :param db_filename: Filename for the local SQLite database.
        :param table_name: Name of the table to store stock data.
        """
        self.companies = companies
        self.start_date = start_date
        self.end_date = end_date
        self.timeframe = timeframe
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.api = tradeapi.REST(self.api_key, self.api_secret, self.base_url, api_version='v2')
        self.db_filename = db_filename
        self.table_name = table_name

    def fetch_stock_data(self, ticker):
        """
        Fetch historical stock data for a given ticker using Alpaca API.
        
        :param ticker: The ticker symbol.
        :return: DataFrame containing the historical bars.
        """
        try:
            bars = self.api.get_bars(ticker, self.timeframe, start=self.start_date, end=self.end_date).df
            if not bars.empty:
                bars['ticker'] = ticker
            return bars
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            return pd.DataFrame()

    def scrape(self):
        """
        Iterate over companies, fetch their data, and return a combined DataFrame.
        """
        all_data = []
        for company, ticker in self.companies.items():
            print(f"Fetching data for {company} ({ticker})...")
            df = self.fetch_stock_data(ticker)
            if df.empty:
                print(f"No data found for {company} ({ticker}).")
                continue
            df['company'] = company
            all_data.append(df)
        if all_data:
            combined_df = pd.concat(all_data)
            print("Scraping complete. Combined data:")
            print(combined_df)
            return combined_df
        else:
            print("No data was scraped.")
            return None

    def save_to_sqlite(self, data):
        """
        Save the combined DataFrame to a local SQLite database.
        
        :param data: DataFrame containing the historical stock data.
        """
        try:
            with sqlite3.connect(self.db_filename) as conn:
                data.to_sql(self.table_name, conn, if_exists='append', index=False)
                print(f"Data saved to SQLite database '{self.db_filename}' in table '{self.table_name}'.")
        except Exception as e:
            print(f"Error saving data to SQLite: {e}")

if __name__ == "__main__":
    companies = {
        "NVIDIA": "NVDA",
        "QUALCOMM": "QCOM",
        "APPLE": "AAPL",
        "GOOGLE": "GOOGL",
        "FACEBOOK": "META",  # Facebook is now Meta Platforms.
        "MICROSOFT": "MSFT",
        "AMAZON": "AMZN"
    }
    
    # Set your desired date range (format: YYYY-MM-DD).
    start_date = "2021-01-01"
    end_date = "2021-12-31"
    
    # Create an instance of the scraper (update API credentials as needed).
    scraper = AlpacaStockScraper(companies, start_date, end_date,
                                 timeframe='1H',
                                 api_key='PKCNI0Y0XAHDTQ0X45I1',
                                 api_secret='y3OMojQ17W2SsnaKsBoTj0Xcq4Ah34kWemeRSo4U',
                                 base_url='https://paper-api.alpaca.markets/v2',
                                 db_filename='stock_data_hourly.db',
                                 table_name='prices')
    
    # Fetch the stock data.
    data = scraper.scrape()
    
    # If data was successfully scraped, save it to the local SQLite database.
    if data is not None:
        scraper.save_to_sqlite(data)
