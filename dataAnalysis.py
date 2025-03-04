import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import timedelta
import numpy as np
import sqlite3

# db_filename = "Data/stock_data_hourly_5y.db"
# table_name = "prices"  # Replace with actual table name

# with sqlite3.connect(db_filename) as conn:
#     df = pd.read_sql(f"SELECT * FROM {table_name}", conn)  # Corrected SQL query
#     print(df.head())
#     print(df.describe())  # Summary statistics
#     print(df.info())  # Prints DataFrame information

class StockMarketApp:
    def __init__(self):
        # Load or initialize data
        self.df = self.load_data()
        # Set Seaborn style for consistency
        sns.set_theme(style="whitegrid")
    
    def load_data(self, ticker="AAPL"):
        """
        Loads the stock data.
        Replace the sample data generation with your actual data loading logic.
        """
        # # Generate sample data (replace with your CSV or API data)
        # date_range = pd.date_range(start="2021-01-01", periods=19691, freq='H')
        # df = pd.DataFrame({
        #     "timestamp": date_range,
        #     "close": np.random.uniform(500, 550, size=len(date_range)),
        #     "high": np.random.uniform(500, 550, size=len(date_range)),
        #     "low": np.random.uniform(500, 550, size=len(date_range)),
        #     "trade_count": np.random.randint(10, 500, size=len(date_range)),
        #     "open": np.random.uniform(500, 550, size=len(date_range)),
        #     "volume": np.random.randint(1000, 10000, size=len(date_range)),
        #     "vwap": np.random.uniform(500, 550, size=len(date_range)),
        #     "ticker": ["NVDA"] * len(date_range),
        #     "company": ["NVIDIA"] * len(date_range)
        # })
        # df['timestamp'] = pd.to_datetime(df['timestamp'])

        db_filename = "Data/stock_data_hourly_5y.db"
        table_name = "prices"  # Replace with actual table name

        with sqlite3.connect(db_filename) as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name} WHERE ticker = '{ticker}'", conn)
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

    def get_interval_dates(self, interval):
        """
        Returns the start and end dates based on the selected time interval.
        """
        end_date = self.df['timestamp'].max()
        if interval == "1 Day":
            start_date = end_date - timedelta(days=1)
        elif interval == "1 Week":
            start_date = end_date - timedelta(weeks=1)
        elif interval == "1 Month":
            start_date = end_date - timedelta(days=30)
        elif interval == "6 Months":
            start_date = end_date - timedelta(days=182)  # Approx. 6 months
        elif interval == "YTD":
            start_date = pd.Timestamp(year=end_date.year, month=1, day=1, tz=end_date.tz)
        elif interval == "1 Year":
            start_date = end_date - timedelta(days=365)
        elif interval == "5 Year":
            start_date = end_date - timedelta(days=1825)
        else:
            start_date = self.df['timestamp'].min()
        return start_date, end_date

    def filter_data(self, start_date, end_date):
        """
        Filters the data between the start and end dates.
        """
        return self.df[(self.df['timestamp'] >= start_date) & (self.df['timestamp'] <= end_date)].copy()

    def plot_data(self, filtered_df, interval):
        """
        Plots the 'close' price over time using matplotlib and seaborn.
        """
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.lineplot(data=filtered_df, x="timestamp", y="close", ax=ax)
        ax.set_title(f"Stock Close Price Over {interval}")
        ax.set_xlabel("Time")
        ax.set_ylabel("Close Price")
        plt.xticks(rotation=45)
        plt.tight_layout()
        st.pyplot(fig)

    def run(self):
        """
        Runs the Streamlit app by setting up the sidebar, filtering data, and displaying the chart and data.
        """
        st.sidebar.header("Time Interval Selector")
        interval = st.sidebar.selectbox(
            "Select Time Interval",
            ("1 Day", "1 Week", "1 Month", "6 Months", "YTD", "1 Year", "5 Year")
        )
        start_date, end_date = self.get_interval_dates(interval)
        filtered_df = self.filter_data(start_date, end_date)
        self.plot_data(filtered_df, interval)
        st.write(f"Displaying data from {start_date.date()} to {end_date.date()}")
        st.dataframe(filtered_df.head(10))

# Run the app if this script is executed
if __name__ == '__main__':
    app = StockMarketApp()
    app.run()
