import time, random
from tradelocker import TLAPI
from tradelocker.utils import load_env_config

if __name__ == "__main__":
    config = load_env_config(__file__, backup_env_file="../.env")

    # Initialize the API client with the information you use to login
    tl = TLAPI(
        environment=config["tl_environment"],
        username=config["tl_email"],
        password=config["tl_password"],
        server=config["tl_server"],
        log_level=config["tl_log_level"],
        acc_num=int(config["tl_acc_num"]),
    )

    symbol_name = "BTCUSD"  # "RANDOM"
    all_instruments = tl.get_all_instruments()
    if symbol_name == "RANDOM":
        instrument_id = int(random.choice(all_instruments["tradableInstrumentId"]))
    else:
        instrument_id = tl.get_instrument_id_from_symbol_name(symbol_name)
    price_history = tl.get_price_history(
        instrument_id,
        resolution="1D",
        start_timestamp=0,
        end_timestamp=0,
        lookback_period="5D",
    )
    latest_price = tl.get_latest_asking_price(instrument_id)
    order_id = tl.create_order(instrument_id, quantity=0.01, side="buy", type_="market")
    if order_id:
        print(f"Placed order with id {order_id}, sleeping for 2 seconds.")
        time.sleep(2)
        tl.close_position(order_id)
        print(f"Closed order with id {order_id}.")
    else:
        print("Failed to place order.")
