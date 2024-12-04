import yfinance as yf 
import pyodbc 
import pandas as pd
from datetime import datetime, timedelta

conn = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=.\\SQLEXPRESS;"
    "Database=StockMarketDB;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

cursor.execute("SELECT StockID, Ticker FROM Stocks")
stocks = cursor.fetchall()

def fetchNupdateStockData(stock_id, ticker):
    print(f"Updating data for {ticker}...")
    try:
        cursor.execute("SELECT MAX(TradeDate) FROM HistoricalPrices WHERE StockID = ?", stock_id)
        last_date = cursor.fetchone()[0]

        if last_date:
            last_date = datetime.strptime(last_date, "%Y-%m-%d") if isinstance(last_date, str) else last_date
            start_date = (last_date + timedelta(days=1)) 
        else:
            start_date = datetime(2020, 1, 1)
        
        end_date = datetime.now()
        
        print(f"Fetching data from {start_date} to {end_date} for {ticker}")
        data = yf.download(ticker, start=start_date, end=end_date)
        if data.empty:
            print(f"No new data available for {ticker}.")
            return

        data.reset_index(inplace=True)

        for _, row in data.iterrows():
            cursor.execute("""
                INSERT INTO HistoricalPrices
                (StockID, TradeDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume)
                VALUES (?, ?, ?, ?, ?, ?, ?,)""",
                stock_id,
                row['Date'].date(),
                row['Open'],
                row['High'],
                row['Low'],
                row['Close'],
                row['Volume']
            )
            conn.commit()
        print(f"Data for {ticker} updated successfully.")
    except Exception as e:
        print(f"Error updating data for {ticker}: {e}")

test_stock_id = 40
test_ticker = 'AAPL'

#Loop thru all stocks and update data
#for stock_id, ticker in stocks:
#    fetchNupdateStockData(stock_id, ticker)
fetchNupdateStockData(test_stock_id, test_ticker)

conn.close()
print("Nighty update completed.")

