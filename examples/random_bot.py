import time
import sys
import os
import random
import traceback
from tradelocker import TLAPI
from tradelocker.utils import load_env_config


# Yolo Trade function that succeeds in 30% of cases
def yolo_trade():
    return random.random() < 0.3


class RandomTradingBot:
    def __init__(self, tlAPI, trade_probability, sleep_time, instrument_id=0):
        self.trade_probability = trade_probability
        self.tlAPI = tlAPI
        self.sleep_time = sleep_time
        self.instrument_id = instrument_id

    def calculate_position_size(self):
        """Calculate position size based on a random number."""
        return round(random.uniform(0.01, 0.1), 2)  # for example

    def run(self):
        """Run the trading bot."""

        all_instruments = self.tlAPI.get_all_instruments()

        while True:
            sys.stdout.flush()  # This is to make sure all output happens immediately.

            try:
                # Fetch the latest prices
                latest_prices = self.tlAPI.get_price_history(
                    instrument_id=277, resolution="1D", lookback_period="3D"
                )
                latest_close = latest_prices["c"].iloc[-1]
                print("latest_close: \n", latest_close)

                # Decide to buy or sell
                if random.random() < self.trade_probability:
                    position_size = self.calculate_position_size()

                    instrument_id = (
                        self.instrument_id
                        if self.instrument_id
                        else random.choice(all_instruments["tradableInstrumentId"])
                    )

                    # Randomly decide to buy or sell
                    if random.choice([True, False]):
                        print("Buy decision.")
                        order_id = self.tlAPI.create_order(
                            instrument_id, position_size, "buy"
                        )
                    else:
                        print("Sell decision.")
                        order_id = self.tlAPI.create_order(
                            instrument_id, position_size, "sell"
                        )

                    holding_time = self.sleep_time

                    # Sleep for the chosen delay
                    print(
                        f"Keeping the position for the next {holding_time} seconds..."
                    )
                    time.sleep(holding_time)

                    # Close the position
                    print("Closing the position...")
                    self.tlAPI.close_position(order_id)
                else:
                    print("--> Decided not to make any orders in this iteration.")

                print(f"Sleeping for {self.sleep_time} seconds before next trade...")
                time.sleep(self.sleep_time)

            except Exception as e:
                print(f"An error occurred: {e}")
                print(e)
                traceback.print_exc()
                sys.exit(1)


if __name__ == "__main__":
    config = load_env_config(__file__, backup_env_file="../.env")

    tlAPI = TLAPI(
        environment=config["tl_environment"],
        username=config["tl_email"],
        password=config["tl_password"],
        server=config["tl_server"],
        log_level="debug",
    )

    bot = RandomTradingBot(
        tlAPI, 0.5, 5, tlAPI.get_instrument_id_from_symbol_name("BTCUSD")
    )

    # Run the bot
    bot.run()
