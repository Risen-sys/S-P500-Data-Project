import pyodbc 
import pandas as pd

csv_file = r"C:\Users\MatthewSilbernagel\Desktop\sp500_companies.csv"

conn = pyodbc.connect("Driver={SQL Server};"
                      "Server=.\\SQLEXPRESS;"
                      "Database=StockMarketDB;"
                      "Trusted_Connection=yes;")

df = pd.read_csv(csv_file)

for index, row in df.iterrows():
    cursor = conn.cursor()
    cursor.execute("INSERT INTO Stocks (Ticker, CompanyName, Sector, Industry, DateAdded, Location) VALUES (?, ?, ?, ?, ?, ?)",
                    row['Ticker'], row['CompanyName'], row['Sector'], row['Industry'], row['DateAdded'], row['Location'])
    conn.commit()

conn.close()

