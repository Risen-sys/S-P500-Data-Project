import matplotlib.pyplot as plt 
import pandas as pd

data = pd.read_csv('cleaned_historical_prices.csv')

aapl_data = data[data['StockID'] ==  40]

aapl_data['TradeDate'] = pd.to_datetime(aapl_data['TradeDate'])

#plot closing price
plt.figure(figsize=(12, 6))
plt.plot(aapl_data['TradeDate'], aapl_data['ClosePrice'], label='AAPL')
plt.title('AAPL Closing Prices')
plt.xlabel('Date')
plt.ylabel('Close Price')
plt.legend()
plt.grid()
plt.show()

plt.savefig('aapl_closing_prices.png')