from tradelocker import TLAPI
import pandas as pd
from datetime import datetime

def get_last_candle_details():
    # Initialize the API client with your credentials
    tl = TLAPI(environment="https://demo.tradelocker.com",
               username="user@email.com",
               password="somepassword",
               server="yourBrokerProfirm")
    
    # Specify the instrument you're interested in
    instrument_name = "BTCUSD"  # Change this to your desired instrument
    
    # Get the instrument ID
    instrument_id = tl.get_instrument_id_from_symbol_name(instrument_name)
    
    # Fetch the price history for the instrument
    price_history = tl.get_price_history(instrument_id, resolution="1D", start_timestamp=0, end_timestamp=0, lookback_period="5D")
    
    # Convert the price history to a DataFrame for easier manipulation
    df = pd.DataFrame(price_history)
    
    # Rename the first column to 'unix_timestamp_millis' for clarity
    df.rename(columns={df.columns[0]: 'unix_timestamp_millis'}, inplace=True)
    
    # Convert the millisecond timestamps to datetime objects
    df['datetime'] = pd.to_datetime(df['unix_timestamp_millis'], unit='ms')
    
    # Sort the DataFrame by the new 'datetime' column in descending order
    df_sorted = df.sort_values(by='datetime', ascending=False)
    
    # Access the first row of the sorted DataFrame, which should now be the last candle
    last_candle_row = df_sorted.iloc[0]
    
    # Extracting the candle details
    timestamp = last_candle_row['datetime']
    date_time = timestamp.strftime('%Y-%m-%d %H:%M:%S')  # Format the datetime object as a string
    open_price = last_candle_row['o']
    close_price = last_candle_row['c']
    low_price = last_candle_row['l']
    high_price = last_candle_row['h']
    
    return {
        "Date": date_time,
        "Time": date_time.split(' ')[1],  # Extract just the time part
        "Open": open_price,
        "Close": close_price,
        "Low": low_price,
        "High": high_price
    }

# Print the last candle details
result = get_last_candle_details()
if result:
    print(result)
else:
    print("Failed to retrieve the last candle details.")
