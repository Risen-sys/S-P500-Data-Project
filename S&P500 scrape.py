import pandas as pd
import requests
from bs4 import BeautifulSoup

# URL of the Wikipedia page
url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Fetch the webpage
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Find the first table (the one with the S&P 500 companies)
table = soup.find('table', {'class': 'wikitable'})

# Read the table into a DataFrame
df = pd.read_html(str(table))[0]

# Save the DataFrame to a CSV file
df.to_csv("sp500_companies.csv", index=False)

print("S&P 500 list saved as sp500_companies.csv")
