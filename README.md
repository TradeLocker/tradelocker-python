# TradeLocker Python API Wrapper

This project provides a Python wrapper for TradeLocker's public API. It simplifies the process of making requests to the API by providing Pythonic interfaces.

A full description of the TradeLocker's public API can be found at [TradeLocker API Documentation](https://tradelocker.com/api)

---

## Table of Contents
1. [Getting Started](#getting-started)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Contributing](#contributing)
5. [License](#license)

## Getting Started

To use this Python wrapper, you'll need the username, password and the id of the server that you use to access TradeLocker.
If you don't already have access, you need to find a broker that supports TradeLocker and create an account.

## Installation

This package requires Python 3.11 or later.
The easiest way to install this package is using pip:

```shell
pip install tradelocker
```

## Usage

Here's a simple example on how to use the TradeLocker Python API wrapper.
The code below initializes a TLAPI object with authentication data.
It then: fetches price history and latest price, creates an order that converts into a position, waits for 2 seconds, and finally closes the same position.

```python
from tradelocker import TLAPI
import time, random

# Initialize the API client with the information you use to login
tl = TLAPI(environment = "https://demo.tradelocker.com", username = "user@email.com", password = "YOUR_PASS", server = "SERVER_NAME")

symbol_name = "BTCUSD" # "RANDOM"
all_instruments = tl.get_all_instruments()
if symbol_name == "RANDOM":
	instrument_id = int(random.choice(all_instruments['tradableInstrumentId']))
else:
	instrument_id = tl.get_instrument_id_from_symbol_name(symbol_name)
price_history = tl.get_price_history(instrument_id, resolution="1D", start_timestamp=0, end_timestamp=0,lookback_period="5D")
latest_price = tl.get_latest_asking_price(instrument_id)
order_id = tl.create_order(instrument_id, quantity=0.01, side="buy", type_="market")
if order_id:
	print(f"Placed order with id {order_id}, sleeping for 2 seconds.")
	time.sleep(2)
	tl.close_position(order_id)
	print(f"Closed order with id {order_id}.")
else:
	print("Failed to place order.")
```

For more detailed examples, see the `examples` directory.

## Contributing

To contribute to the development of this project, please create an issue, or a pull request.

Steps to create a pull request:
1. Clone the project.
2. Create your feature branch (`git checkout -b feature/YourFeature`).
3. Commit your changes (`git commit -am 'Add some feature'`).
4. Push to the branch (`git push origin feature/YourFeature`).
5. Create a new Merge Request.

## License

This project is licensed under the terms of the MIT license. See [LICENSE](https://github.com/ivosluganovic/tl/blob/main/LICENSE.txt) for more details.
