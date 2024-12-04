import yfinance as yf
import pyodbc
import pandas as pd
from datetime import datetime

# Database connection setup
connection_string = (
    "Driver={SQL Server};"
    "Server=.\\SQLEXPRESS;"
    "Database=StockMarketDB;"
    "Trusted_Connection=yes;"
)

# Get tickers and StockIDs from the database
def get_tickers_from_db():
    connection = pyodbc.connect(connection_string)
    query = "SELECT StockID, Ticker FROM Stocks"
    tickers = pd.read_sql(query, connection)
    connection.close()
    return tickers

# Fetch stock data
def fetch_stock_data(symbol, start_date, end_date):
    ticker = yf.Ticker(symbol)
    data = ticker.history(start=start_date, end=end_date)
    data.reset_index(inplace=True)  # Reset index for easier iteration
    return data

# Upload stock data to the database
def upload_data_to_db(data, stock_id):
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Prepare data for batch insertion
        rows_to_insert = []
        for _, row in data.iterrows():
            trade_date = row['Date'].strftime('%Y-%m-%d')  # Format date
            rows_to_insert.append((
                stock_id, 
                trade_date, 
                row['Open'], 
                row['High'], 
                row['Low'], 
                row['Close'], 
                row['Volume']
            ))

        # Batch insert
        query = """
        INSERT INTO HistoricalPrices (StockID, TradeDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.executemany(query, rows_to_insert)
        connection.commit()
        print(f"Data for StockID {stock_id} uploaded successfully.")
    except Exception as e:
        print(f"Error uploading data for StockID {stock_id}: {e}")
    finally:
        connection.close()

# Main function
if __name__ == "__main__":
    print("Fetching tickers from the database...")
    tickers = get_tickers_from_db()

    start_date = "2023-12-31"
    end_date = "2024-12-4"

    print("Fetching stock data and uploading to the database...")
    for _, row in tickers.iterrows():
        stock_id = row['StockID']
        symbol = row['Ticker']
        print(f"Processing {symbol} (StockID: {stock_id})...")
        try:
            stock_data = fetch_stock_data(symbol, start_date, end_date)
            if not stock_data.empty:
                upload_data_to_db(stock_data, stock_id)
        except Exception as e:
            print(f"Failed to fetch data for {symbol}: {e}")
    print("Process complete.")
