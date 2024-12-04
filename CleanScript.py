import pandas as pd
import pyodbc

conn = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=.\\SQLEXPRESS;"
    "Database=StockMarketDB;"
    "Trusted_Connection=yes;"
)

query = "SELECT * FROM HistoricalPrices"
data = pd.read_sql(query, conn)

#remove dups
data.drop_duplicates(inplace=True)

#handle missing values
data.fillna(method='ffill', inplace=True)

#valid price and volume values
data = data[(data['OpenPrice'] > 0) & (data['HighPrice'] > 0) & (data['LowPrice'] > 0) & (data['ClosePrice'] > 0) & (data['Volume'] > 0)]

data.to_csv('cleaned_historical_prices.csv', index=False)

print("Data cleaning complete.")
conn.close()