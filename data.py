import pandas as pd
import config


from datetime import date,datetime,timedelta


from alpaca.data import  StockHistoricalDataClient
from alpaca.data.timeframe import TimeFrame
from alpaca.data.requests import StockBarsRequest


API_KEY = config.api_key
API_SECRETKEY = config.secrte_key

#client = CryptoHistoricalDataClient()
stock_client = StockHistoricalDataClient(API_KEY,API_SECRETKEY)


def fetchHS(Timeframe,start,end,TICKER):
    
    match Timeframe:
      case "Hour":
          time_frame = TimeFrame.Hour
           
      case "Minute":
          time_frame = TimeFrame.Minute

    
    requestParams = StockBarsRequest(
        symbol_or_symbols = TICKER,
        timeframe         = time_frame,
        start             = start,
        end               = end,
        adjustment        = 'all'
        )

    data = stock_client.get_stock_bars(requestParams)
    df = data.df

    df.reset_index(inplace=True)
    #df = df.drop(['symbol'], axis=1)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    #csvName =  "HS_3650_Minute" + ".csv"
    #df.to_csv(csvName, index=False)

    print(df.head())
    print(df.tail())
    return df 
            
        
        

        
        
    
if __name__ == "__main__":
    timeFrame = "Minute"  # or "Minute"
    today = str(date.today())
    yesterday = str(date.today() - timedelta(days = 365*10))
    TICKER = tickers = [
    'NVDA', 'MSFT', 'AAPL', 'AVGO', 'TSM', 'ORCL', 'PLTR', 'SAP', 'AMD', 'ASML',
    'CSCO', 'CRM', 'IBM', 'INTU', 'NOW', 'UBER', 'ACN', 'ARM', 'QCOM', 'TXN',
    'SHOP', 'ADBE', 'ANET', 'AMAT', 'SONY', 'APH', 'ADP', 'LRCX', 'MU', 'PANW',
    'KLAC', 'SNPS', 'CRWD', 'ADI', 'MSTR', 'CDNS', 'DELL', 'INTC', 'FTNT', 'FI',
    'SNOW', 'MSI', 'INFY', 'MRVL', 'NET', 'ADSK', 'WDAY', 'TEL', 'ROP', 'NXPI',
    'GLW', 'PAYX', 'TEAM', 'DDOG', 'CRWV', 'XYZ', 'ZS', 'GRMN', 'FIS', 'MCHP',
    'FICO', 'CTSH', 'SMCI', 'MPWR', 'STX', 'WIT', 'BR', 'KEYS', 'HUBS', 'TOST',
    'HPE', 'UI', 'IT', 'TDY', 'ERIC', 'VRSN', 'WDC', 'JBL', 'PTC', 'ON',
    'CDW', 'TYL', 'HPQ', 'STM', 'CPAY', 'GDDY', 'ZM', 'IOT', 'ASX', 'GIB',
    'AFRM', 'GFS', 'CYBR', 'GRAB', 'NOK', 'NTAP', 'ALAB', 'SSNC', 'CHKP', 'LDOS'
    ]
    all_data = pd.DataFrame()
    df = pd.DataFrame()
    for ticker in tickers:
        df = fetchHS(timeFrame, yesterday, today, ticker)
        if not df.empty:
            # Add ticker column to identify the stock
            df['ticker'] = ticker
            all_data = pd.concat([all_data, df], ignore_index=True)
        print("stock data for ", ticker, " fetched successfully.")
    
    # Save combined data
    if not all_data.empty:
        all_data.to_csv("HS_3650_Minute_All.csv", index=False)
        print("\nCombined DataFrame:")
        print("Head:")
        print(all_data.head())
        print("Tail:")
        print(all_data.tail())
    else:
        print("No data was retrieved.")