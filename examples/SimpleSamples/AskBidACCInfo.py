from tradelocker import TLAPI
import pandas as pd
from datetime import datetime

# Initialize the TradeLocker API client with your credentials
tl_api = TLAPI(
    environment="https://demo.tradelocker.com",
    username="user@email.com",
    password="yourpassword",
    server="yourBrokerPropFirm"
)

pair = input('Pair: ').strip().upper()

# Get the instrument ID
pair_id = tl_api.get_instrument_id_from_symbol_name(pair)

# Get price information
price = tl_api.get_latest_asking_price(pair_id)
acc_info = tl_api.get_account_state()
balance = acc_info['balance']
actual_balance = acc_info['availableFunds']
ask = tl_api.get_latest_asking_price(pair_id)
bid = tl_api.get_latest_bid_price(pair_id)


def acc_info():
    print('Pair Price: ', str(price), '\nBalance: ', str(balance), '\nActual Balance: ', actual_balance, '\nAsk: ', str(ask), 'Bid: ', str(bid))


def main():
    acc_info()

if __name__ == "__main__":
    main()
