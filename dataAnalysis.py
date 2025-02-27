import pandas as pd
import sqlite3

db_filename = "stock_data.db"
table_name = "prices"  # Replace with actual table name

with sqlite3.connect(db_filename) as conn:
    df = pd.read_sql(f"SELECT * FROM {table_name} WHERE ticker = 'AAPL'", conn)  # Corrected SQL query
    print(df.describe())  # Summary statistics
    print(df.info())  # Prints DataFrame information
