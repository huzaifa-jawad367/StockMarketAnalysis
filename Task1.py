import alpaca_trade_api as tradeapi
import pandas as pd
from pymongo import MongoClient

class AlpacaStockScraper:
    def __init__(self, companies, start_date, end_date, timeframe='day',
                 api_key='YOUR_API_KEY', api_secret='YOUR_API_SECRET',
                 base_url='https://paper-api.alpaca.markets',
                 mongo_uri='mongodb://localhost:27017/', mongo_db='stock_data', mongo_collection='prices'):
        """
        Initialize the AlpacaStockScraper with company data, date range, Alpaca API credentials, and MongoDB configuration.

        :param companies: Dictionary mapping company names to ticker symbols.
        :param start_date: Start date for historical data (YYYY-MM-DD).
        :param end_date: End date for historical data (YYYY-MM-DD).
        :param timeframe: Time interval of bars (e.g., 'minute', 'day').
        :param api_key: Alpaca API key.
        :param api_secret: Alpaca API secret.
        :param base_url: Alpaca API base URL.
        :param mongo_uri: MongoDB connection URI.
        :param mongo_db: MongoDB database name.
        :param mongo_collection: MongoDB collection name.
        """
        self.companies = companies
        self.start_date = start_date
        self.end_date = end_date
        self.timeframe = timeframe
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.api = tradeapi.REST(self.api_key, self.api_secret, self.base_url, api_version='v2')
        
        # Setup MongoDB connection.
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.mongo_client = MongoClient(self.mongo_uri)
        self.db = self.mongo_client[self.mongo_db]
        self.collection = self.db[self.mongo_collection]

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

    def save_to_mongo(self, data):
        """
        Save the combined DataFrame to MongoDB.

        :param data: DataFrame containing the historical stock data.
        """
        try:
            records = data.to_dict("records")
            if records:
                result = self.collection.insert_many(records)
                print(f"Inserted {len(result.inserted_ids)} documents into MongoDB collection '{self.mongo_collection}'.")
        except Exception as e:
            print(f"Error saving data to MongoDB: {e}")

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
    
    # Create an instance of the scraper (update API and MongoDB credentials as needed).
    scraper = AlpacaStockScraper(companies, start_date, end_date,
                                 timeframe='day',
                                 api_key='YOUR_API_KEY',
                                 api_secret='YOUR_API_SECRET',
                                 base_url='https://paper-api.alpaca.markets',
                                 mongo_uri='mongodb://localhost:27017/',
                                 mongo_db='stock_data',
                                 mongo_collection='prices')
    
    # Fetch the stock data.
    data = scraper.scrape()
    
    # If data was successfully scraped, save it to MongoDB.
    if data is not None:
        scraper.save_to_mongo(data)
