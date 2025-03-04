import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import timedelta
import sqlite3

class StockMarketApp:
    def __init__(self):
        # Set Seaborn style for consistency
        sns.set_theme(style="whitegrid")
    
    def load_data(self, ticker="AAPL"):
        """
        Loads the stock data for a given ticker from the SQLite database.
        """
        db_filename = "Data/stock_data_hourly_5y.db"
        table_name = "prices"  # Replace with actual table name

        with sqlite3.connect(db_filename) as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name} WHERE ticker = '{ticker}'", conn)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df

    def get_interval_dates(self, df, interval):
        """
        Returns the start and end dates based on the selected time interval.
        """
        end_date = df['timestamp'].max()
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
            start_date = df['timestamp'].min()
        return start_date, end_date

    def filter_data(self, df, start_date, end_date):
        """
        Filters the data between the start and end dates.
        """
        return df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)].copy()

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
        Runs the Streamlit app by setting up the sidebar, loading data for the selected ticker,
        filtering data by the selected time interval, and displaying the chart and data.
        """
        st.sidebar.header("Options")
        
        # Ticker dropdown menu
        ticker = st.sidebar.selectbox(
            "Select Ticker",
            ("AAPL", "NVDA", "QCOM", "AMZN", "GOOGL", "META", "MSFT")
        )
        
        # Load data for the selected ticker
        df = self.load_data(ticker)
        
        st.sidebar.header("Time Interval Selector")
        interval = st.sidebar.selectbox(
            "Select Time Interval",
            ("1 Day", "1 Week", "1 Month", "6 Months", "YTD", "1 Year", "5 Year")
        )
        
        start_date, end_date = self.get_interval_dates(df, interval)
        filtered_df = self.filter_data(df, start_date, end_date)
        self.plot_data(filtered_df, interval)
        st.write(f"Displaying data from {start_date.date()} to {end_date.date()}")
        st.dataframe(filtered_df.head(10))

# Run the app if this script is executed
if __name__ == '__main__':
    app = StockMarketApp()
    app.run()
