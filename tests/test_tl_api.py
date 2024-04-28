from time import sleep
import re
import os
import pandas as pd
import pytest
from typing import Literal
from typeguard import TypeCheckError
from tradelocker.utils import load_env_config, tl_check_type
from tradelocker import TLAPI
from tradelocker.types import (
    AccountDetailsColumns,
    InstrumentDetailsType,
    SessionDetailsType,
    QuotesType,
    QuotesKeyType,
    OrdersColumns,
    ExecutionsColumns,
    PositionsColumns,
)


# Create the global fixture
@pytest.fixture(scope="session", autouse=True)
def setup_everything():
    global tl, config, default_instrument_id, default_symbol_name

    parent_folder_env = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    config = load_env_config(__file__, backup_env_file=parent_folder_env)

    if "tl_acc_num" not in config:
        config["tl_acc_num"] = 0
    config["tl_acc_num"] = int(config["tl_acc_num"])

    tl = TLAPI(
        environment=config["tl_environment"],
        username=config["tl_email"],
        password=config["tl_password"],
        server=config["tl_server"],
        acc_num=int(config["tl_acc_num"]),
    )

    default_symbol_name = "BTCUSD"  # Since the market is always open for crypto
    default_instrument_id = tl.get_instrument_id_from_symbol_name(default_symbol_name)
    assert default_instrument_id


def test_user_accounts():
    all_account_nums = tl.get_all_accounts()["accNum"]
    first_account_id = int(tl.get_all_accounts()["id"].iloc[0])

    with pytest.raises(ValueError):
        tl0 = TLAPI(
            environment=config["tl_environment"],
            username=config["tl_email"],
            password=config["tl_password"],
            server=config["tl_server"],
            acc_num=-1,
        )

    tl1 = TLAPI(
        environment=config["tl_environment"],
        username=config["tl_email"],
        password=config["tl_password"],
        server=config["tl_server"],
        acc_num=int(all_account_nums.iloc[0]),
    )

    assert tl1

    tl1_by_id = TLAPI(
        environment=config["tl_environment"],
        username=config["tl_email"],
        password=config["tl_password"],
        server=config["tl_server"],
        account_id=first_account_id,
    )

    assert len(all_account_nums) > 0

    if len(all_account_nums) > 1:
        tl2 = TLAPI(
            environment=config["tl_environment"],
            username=config["tl_email"],
            password=config["tl_password"],
            server=config["tl_server"],
            acc_num=int(all_account_nums.iloc[1]),
        )

        assert tl2.account_id != tl1.account_id


# test that __about__.py 's __version__ is set
def test_version():
    from tradelocker.__about__ import __version__

    assert __version__ is not None
    pattern = r"^\d+\.\d+\.\d+(-(alpha|beta|rc)\.\d+)?$"
    assert re.match(pattern, __version__), f"Invalid version format: {__version__}"


def test_refresh_tokens():
    old_access_token = tl.get_access_token()
    old_refresh_token = tl.get_refresh_token()
    tl.refresh_access_tokens()
    assert tl.get_access_token() != old_access_token
    assert tl.get_refresh_token() != old_refresh_token


def test_latest_asking_price():
    latest_price = tl.get_latest_asking_price(default_instrument_id)
    tl_check_type(latest_price, float)
    assert latest_price


def test_quotes():
    quotes = tl.get_quotes(default_instrument_id)
    tl_check_type(quotes, QuotesType)
    keys = QuotesKeyType.__args__
    for key in keys:
        assert key in quotes


def test_get_instrument_id():
    eurusd_id = tl.get_instrument_id_from_symbol_name("EURUSD")
    assert eurusd_id == 278
    with pytest.raises(ValueError):
        tl.get_instrument_id_from_symbol_name("DOESNOTEXIST")

    all_instruments = tl.get_all_instruments()
    assert eurusd_id in all_instruments["tradableInstrumentId"].values

    eurusdSymbolId = int(
        all_instruments[all_instruments["tradableInstrumentId"] == eurusd_id][
            "id"
        ].values[0]
    )

    eurusd_id_from_symbol_id = tl.get_instrument_id_from_symbol_id(eurusdSymbolId)
    assert eurusd_id_from_symbol_id == eurusd_id


# TODO: test this properly!
def test_all_executions():
    all_executions = tl.get_all_executions()
    tl_check_type(all_executions, pd.DataFrame)
    assert set(all_executions.columns) == set(ExecutionsColumns.keys())

    tl.create_order(
        default_instrument_id, quantity=0.01, side="buy", price=0, type_="market"
    )
    assert "positionId" in all_executions


def test_access_token():
    # Check whether access token was received
    assert hasattr(tl, "_access_token")
    assert tl._access_token != ""


def test_old_history():
    prices_history_last_2Y = tl.get_price_history(
        default_instrument_id, resolution="1D", lookback_period="3Y"
    )

    assert not prices_history_last_2Y.empty

    ts_2022_05_01 = 1651396149000
    ts_2022_07_01 = 1656666549000
    prices_history_2022 = tl.get_price_history(
        default_instrument_id,
        resolution="1H",
        start_timestamp=ts_2022_05_01,
        end_timestamp=ts_2022_07_01,
    )

    assert not prices_history_2022.empty
    # size > 100
    assert len(prices_history_2022) > 100


def test_multiton_single_account():
    tl2 = TLAPI(
        environment=config["tl_environment"],
        username=config["tl_email"],
        password=config["tl_password"],
        server=config["tl_server"],
        acc_num=int(config["tl_acc_num"]),
    )

    assert tl2
    assert tl2 == tl


def test_multiton_multiple_accounts():
    all_account_nums = tl.get_all_accounts()["accNum"]

    # Check that there are more than one account in the list (required for testing)
    if len(all_account_nums) == 1:
        pytest.skip("Need more than one account to test multiton")

    assert len(all_account_nums) > 1, "Need more than one account to test multiton"

    other_acc_num = -1
    for acc_num in all_account_nums:
        if acc_num != config["tl_acc_num"]:
            other_acc_num = acc_num
            break

    tl3 = TLAPI(
        environment=config["tl_environment"],
        username=config["tl_email"],
        password=config["tl_password"],
        server=config["tl_server"],
        acc_num=int(other_acc_num),
    )

    assert tl3
    assert tl3 != tl


def test_price_history():
    price_history = tl.get_price_history(
        default_instrument_id, resolution="1D", lookback_period="5D"
    )
    assert not price_history.empty
    tl_check_type(price_history, pd.DataFrame)
    assert "c" in price_history
    assert "h" in price_history
    assert "l" in price_history
    assert "o" in price_history
    assert price_history["c"].iloc[-1] > 0

    # Check that ValueError is raised when resolution is not valid
    with pytest.raises(TypeCheckError):
        price_history = tl.get_price_history(
            default_instrument_id, resolution="bla", lookback_period="5D"
        )
        assert price_history == None

    # Should fail since "m4" is not a good lookback period value
    with pytest.raises(ValueError):
        price_history = tl.get_price_history(
            default_instrument_id, resolution="15m", lookback_period="m4"
        )

    # Should fail due to trying to fetch too much data
    with pytest.raises(ValueError):
        tl.get_price_history(
            default_instrument_id, resolution="5m", lookback_period="5Y"
        )

    # Should also fail due to fetching too much data
    with pytest.raises(ValueError):
        tl.get_price_history(
            default_instrument_id, resolution="1m", lookback_period="2Y"
        )

    price_history_15m_1M = tl.get_price_history(
        default_instrument_id, resolution="15m", lookback_period="1M"
    )
    assert not price_history_15m_1M.empty

    price_history_1H_1Y = tl.get_price_history(
        default_instrument_id, resolution="15m", lookback_period="1M"
    )
    assert not price_history_1H_1Y.empty

    jan_1st_2020_ms: int = 1578524400000
    jan_1st_2023_ms: int = 1672527600000
    jan_9th_2023_ms: int = 1673218800000
    jun_1st_2023_ms: int = 1685570400000
    jun_9th_2023_ms: int = 1686261600000

    no_data_history = tl.get_price_history(
        default_instrument_id,
        resolution="1W",
        start_timestamp=jan_9th_2023_ms,
        end_timestamp=jan_9th_2023_ms,
    )
    assert no_data_history.empty

    price_history_timestamps = tl.get_price_history(
        default_instrument_id,
        resolution="1H",
        start_timestamp=jun_1st_2023_ms,
        end_timestamp=jun_9th_2023_ms,
    )
    assert not price_history_timestamps.empty

    price_history_1Y = tl.get_price_history(
        default_instrument_id,
        resolution="1D",
        start_timestamp=jan_1st_2020_ms,
        end_timestamp=jan_1st_2023_ms,
    )
    assert not price_history_1Y.empty

    with pytest.raises(ValueError):
        # Wrong order
        tl.get_price_history(
            default_instrument_id,
            resolution="1H",
            start_timestamp=jan_9th_2023_ms,
            end_timestamp=jan_1st_2023_ms,
        )
        # Too much data
        tl.get_price_history(
            default_instrument_id,
            resolution="1m",
            start_timestamp=jan_1st_2020_ms,
            end_timestamp=jan_1st_2023_ms,
        )
        # Too much data / non-existing start and lookback
        tl.get_price_history(
            default_instrument_id,
            resolution="1m",
            start_timestamp=0,
            end_timestamp=jan_1st_2020_ms,
        )

    price_history_no_end_timestamp = tl.get_price_history(
        default_instrument_id, resolution="1H", start_timestamp=jun_1st_2023_ms
    )
    assert not price_history_no_end_timestamp.empty


def test_get_all_instruments():
    all_instruments = tl.get_all_instruments()
    assert not all_instruments.empty
    tl_check_type(all_instruments, pd.DataFrame)
    assert len(all_instruments.columns) > 1
    assert all_instruments["name"].str.contains("USD").any()
    assert all_instruments["name"].str.contains("EURUSD").any()
    assert not all_instruments["name"].str.contains("DOES_NOT_EXIST").any()
    assert all_instruments[all_instruments["name"] == "EURUSD"]["id"].values[0] == 315


def test_instrument_and_session_details():
    with pytest.raises(TypeCheckError):
        instrument_details = tl.get_instrument_details(
            default_instrument_id, locale="BLA"
        )

    instrument_details: InstrumentDetailsType = tl.get_instrument_details(
        default_instrument_id
    )
    assert instrument_details
    tl_check_type(instrument_details, InstrumentDetailsType)
    assert instrument_details["name"] == default_symbol_name

    session_id: int = instrument_details["tradeSessionId"]
    tl_check_type(session_id, int)
    assert session_id

    session_details: SessionDetailsType = tl.get_session_details(session_id)
    assert session_details
    tl_check_type(session_details, SessionDetailsType)

    # validate that ValueError is raised when session_id is not an int
    with pytest.raises(TypeCheckError):
        error_session_details = tl.get_session_details("STRING_NOT_INT")
        assert error_session_details == None

    session_status_id = instrument_details["tradeSessionStatusId"]
    assert session_status_id

    session_status_details = tl.get_session_status_details(session_status_id)
    assert session_status_details
    tl_check_type(session_status_details, dict)
    assert len(session_status_details["allowedOperations"]) == 3
    assert len(session_status_details["allowedOrderTypes"]) == 6


def test_get_market_depth():
    market_depth = tl.get_market_depth(default_instrument_id)
    assert market_depth
    tl_check_type(market_depth, dict)
    assert "asks" in market_depth
    assert "bids" in market_depth
    tl_check_type(market_depth["asks"], list)
    tl_check_type(market_depth["bids"], list)


def test_get_daily_bar():
    daily_bar = tl.get_daily_bar(default_instrument_id)
    assert daily_bar
    tl_check_type(daily_bar, dict)
    assert "o" in daily_bar
    assert "h" in daily_bar
    assert "l" in daily_bar
    assert "c" in daily_bar
    assert "v" in daily_bar
    tl_check_type(daily_bar["o"], float)

    with pytest.raises(TypeCheckError):
        tl.get_daily_bar(default_instrument_id, bar_type="NOT_VALID_BAR_TYPE")


def test_instrument_id_from_symbol_name():
    btcusd_instrument_id = tl.get_instrument_id_from_symbol_name("BTCUSD")
    assert btcusd_instrument_id == 206


def get_order_status(order_id: int) -> str:
    return tl.get_order_details(order_id)["status"]


def _columns_set(columns_list: list[dict[Literal["id"], str]]) -> set[str]:
    return set([column["id"] for column in columns_list])


def test_get_config():
    config = tl.get_config()
    assert config
    tl_check_type(config, dict)

    # check that config.keys() equals to ['customerAccess', 'positionsConfig', 'ordersConfig', 'ordersHistoryConfig', 'filledOrdersConfig', 'accountDetailsConfig', 'rateLimits', 'limits']
    expected_config_keys = [
        "customerAccess",
        "positionsConfig",
        "ordersConfig",
        "ordersHistoryConfig",
        "filledOrdersConfig",
        "accountDetailsConfig",
        "rateLimits",
        "limits",
    ]
    assert list(config.keys()) == expected_config_keys

    assert _columns_set(config["positionsConfig"]["columns"]) == set(
        PositionsColumns.keys()
    )

    assert _columns_set(config["ordersConfig"]["columns"]) == set(OrdersColumns.keys())

    assert _columns_set(config["ordersHistoryConfig"]["columns"]) == set(
        OrdersColumns.keys()
    )

    assert _columns_set(config["filledOrdersConfig"]["columns"]) == set(
        ExecutionsColumns.keys()
    )

    assert _columns_set(config["accountDetailsConfig"]["columns"]) == set(
        AccountDetailsColumns.keys()
    )


def test_orders_history_with_limit_order(ensure_order_fill: bool = False):
    # What am I expecting the final order status to be?
    expected_order_status: str = "Cancelled" if not ensure_order_fill else "Filled"
    # Decide whether I am trying to buy or sell for severely under-market price
    order_side: str = (
        "sell" if ensure_order_fill else "buy"
    )  # I am using a super-low price, so using "sell" will always fill

    # oh_X -> orders history --> all final orders EVER
    # o_X -> current orders --> all non-final and final orders in this session
    oh_initial: pd.DataFrame = tl.get_all_orders(history=True)
    o_initial: pd.DataFrame = tl.get_all_orders(history=False)

    with pytest.raises(ValueError):
        order_id: int = tl.create_order(
            default_instrument_id,
            quantity=0.01,
            side=order_side,
            price=0.01,
            type_="limit",
            validity="IOC",
        )

    order_id: int = tl.create_order(
        default_instrument_id,
        quantity=0.01,
        side=order_side,
        price=0.01,
        type_="limit",
        validity="GTC",
    )

    sleep(1)

    # Let's wait for a max of 10 seconds for the order to be filled
    max_wait_seconds: int = 10
    sleep_delay: int = 2
    if ensure_order_fill:
        for i in range(0, max_wait_seconds, sleep_delay):
            oh_after_order: pd.DataFrame = tl.get_all_orders(history=True)
            if (
                order_id in oh_after_order["id"].values
                and oh_after_order[oh_after_order["id"] == order_id]["status"].values[0]
                == "Filled"
            ):
                break
            else:
                sleep(sleep_delay)

            if i + sleep_delay >= max_wait_seconds:
                break

    oh_after_order: pd.DataFrame = tl.get_all_orders(history=True)
    o_after_order: pd.DataFrame = tl.get_all_orders(history=False)

    # Go over each element in oh_initial and check if they are inside oh_after_order
    is_in_oh_after = oh_initial["id"].isin(oh_after_order["id"])
    assert is_in_oh_after.all()

    assert not oh_after_order.empty
    if ensure_order_fill:
        assert len(o_after_order) == len(o_initial)
    else:
        assert not o_after_order.empty

    tl_check_type(oh_after_order, pd.DataFrame)
    tl_check_type(o_after_order, pd.DataFrame)

    # If the order was filled, it shows up in history
    assert len(oh_after_order) == len(oh_initial) + (1 if ensure_order_fill else 0)

    # If the order was filled, it does not show up in /orders
    assert len(o_after_order) == len(o_initial) + (0 if ensure_order_fill else 1)

    delete_success = tl.delete_order(order_id)

    sleep(1)

    if not ensure_order_fill:
        assert delete_success
    else:
        assert not delete_success

    oh_after_delete = tl.get_all_orders(history=True)
    o_after_delete = tl.get_all_orders(history=False)

    # Check that an order is always visible in order history, regardless whether
    # it deleted or previously filled
    assert len(oh_after_delete) == len(oh_initial) + 1

    # Assert that the order is not visible in current orders
    assert len(o_after_delete) == len(o_initial)

    # ----------Ensure that the order is visible in ordersHistory, but not orders----------

    assert order_id in oh_after_delete["id"].values
    # check the status of the deleted order in order history
    assert (
        oh_after_delete[oh_after_delete["id"] == order_id]["status"].values[0]
        == expected_order_status
    )

    assert order_id not in o_after_delete["id"].values
    # check the order status of the deleted order


def test_orders_history_with_filled_limit_order():
    test_orders_history_with_limit_order(ensure_order_fill=True)


def test_get_trade_accounts():
    trade_accounts = tl.get_trade_accounts()
    # check that trade accounts is a dataframe
    tl_check_type(trade_accounts, list)
    assert len(trade_accounts) > 0
    tl_check_type(trade_accounts[0], dict)
    assert trade_accounts[0]["id"]
    tl_check_type(trade_accounts[0]["tradingRules"], dict)


def test_orders():
    ###### Printing order history (len)
    all_orders = tl.get_all_orders(history=False)
    tl_check_type(all_orders, pd.DataFrame)
    assert len(all_orders.columns) > 1


def test_get_account_state():
    account_state = tl.get_account_state()
    assert len(account_state) > 0
    # check that this is a dataframe
    tl_check_type(account_state, dict)
    assert account_state["balance"] > 0
    assert account_state["availableFunds"] > 0

    fields = [
        "balance",
        "projectedBalance",
        "availableFunds",
        "blockedBalance",
        "cashBalance",
        "unsettledCash",
        "withdrawalAvailable",
        "stocksValue",
        "optionValue",
        "initialMarginReq",
        "maintMarginReq",
        "marginWarningLevel",
        "blockedForStocks",
        "stockOrdersReq",
        "stopOutLevel",
        "warningMarginReq",
        "marginBeforeWarning",
        "todayGross",
        "todayNet",
        "todayFees",
        "todayVolume",
        "todayTradesCount",
        "openGrossPnL",
        "openNetPnL",
        "positionsCount",
        "ordersCount",
    ]

    # Check if all fields are in account_info columns
    assert all(field in account_state for field in fields)


def test_create_and_close_position():
    ###### Getting all positions
    positions = tl.get_all_positions()
    len_positions_initial = len(positions)
    assert len_positions_initial >= 0

    ##### Creating and placing an order
    tl_check_type(default_instrument_id, int)
    order_id = tl.create_order(
        default_instrument_id, quantity=0.01, side="buy", price=0, type_="market"
    )
    assert order_id

    all_orders_history = tl.get_all_orders(history=True)
    assert not all_orders_history.empty

    len_positions_after_order = len(tl.get_all_positions())
    assert len_positions_after_order == len_positions_initial + 1

    tl.close_position(order_id)
    len_positions_after_close = len(tl.get_all_positions())

    assert len_positions_after_close == len_positions_initial
    assert len_positions_after_close == len_positions_after_order - 1


def test_close_position_partial():
    ###### Getting all positions
    positions = tl.get_all_positions()
    len_positions_initial = len(positions)
    assert len_positions_initial >= 0

    ##### Creating and placing an order
    tl_check_type(default_instrument_id, int)
    order_id = tl.create_order(
        default_instrument_id, quantity=0.02, side="buy", price=0, type_="market"
    )
    assert order_id

    all_orders_history = tl.get_all_orders(history=True)
    assert not all_orders_history.empty

    len_positions_after_order = len(tl.get_all_positions())
    assert len_positions_after_order == len_positions_initial + 1

    tl.close_position(order_id=order_id, close_quantity=0.01)
    positions_after_close = tl.get_all_positions()
    len_positions_after_close = len(positions_after_close)

    assert len_positions_after_close == len_positions_initial + 1
    assert len_positions_after_close == len_positions_after_order

    # get the position from the order_id
    position_id = position_id_from_order_id(order_id)
    assert position_id in positions_after_close["id"].values
    assert (
        positions_after_close[positions_after_close["id"] == position_id]["qty"].values[
            0
        ]
        == 0.01
    )

    tl.close_position(position_id=position_id, close_quantity=0.01)
    positions_final = tl.get_all_positions()
    assert len(positions_final) == len_positions_initial


def test_position_netting():
    # Test that position_netting = False yields in two positions
    tl.close_all_positions()
    order1_id = tl.create_order(
        default_instrument_id, quantity=0.01, side="buy", price=0, type_="market"
    )
    sleep(1)
    order2_id = tl.create_order(
        default_instrument_id, quantity=0.03, side="sell", price=0, type_="market"
    )
    sleep(1)
    all_positions = tl.get_all_positions()
    # Expected: 0.01 buy ; 0.01 sell
    assert len(all_positions) == 2

    # Create another position, which should fully cancel the position created in the first order
    order3_id = tl.create_order(
        default_instrument_id,
        quantity=0.01,
        side="sell",
        price=0,
        type_="market",
        position_netting=True,
    )
    sleep(1)
    # Expected: 0.01 sell (buy was closed due to netting)
    all_positions_netting = tl.get_all_positions()
    assert len(all_positions_netting) == 1

    # Create another "sell" position, then create a position that should close this positions, as well as partially close the order_2 position
    order4_id = tl.create_order(
        default_instrument_id,
        quantity=0.01,
        side="sell",
        price=0,
        type_="market",
        position_netting=True,
    )
    sleep(1)
    order5_id = tl.create_order(
        default_instrument_id,
        quantity=0.02,
        side="buy",
        price=0,
        type_="market",
        position_netting=True,
    )
    sleep(1)

    all_positions_netting_partial = tl.get_all_positions()
    # Expected: the 0.02 buy actually cancelled the order_4 sell, and reduced the order2's sell side to 0.02
    assert len(all_positions_netting_partial) == 1
    assert all_positions_netting_partial["qty"].iloc[0] == 0.02

    tl.create_order(
        default_instrument_id,
        quantity=0.02,
        side="buy",
        price=0,
        type_="market",
        position_netting=True,
    )
    all_positions_netting_full = tl.get_all_positions()

    # Expected: the 0.02 buy actually cancelled the order_2's remaining sell (which was 0.02), so now there should be no open positions
    assert len(all_positions_netting_full) == 0


def position_id_from_order_id(order_id: int) -> int:
    all_orders = tl.get_all_orders(history=True)
    matching_orders = all_orders[all_orders["id"] == order_id]
    if len(matching_orders) == 0:
        raise ValueError(f"No order found with order_id = {order_id}")
    return int(matching_orders["positionId"].iloc[0])


def test_close_position_by_position_id():
    all_positions = tl.get_all_positions()
    order_id1 = tl.create_order(
        default_instrument_id, quantity=0.01, side="buy", price=0, type_="market"
    )
    all_positions_after_order = tl.get_all_positions()

    assert len(all_positions_after_order) == len(all_positions) + 1

    position_id1 = position_id_from_order_id(order_id1)
    tl.close_position(position_id=position_id1)
    all_positions_after_close = tl.get_all_positions()

    assert len(all_positions_after_close) == len(all_positions)


def test_close_all_positions():
    all_positions_initial = tl.get_all_positions()
    tl.close_all_positions()
    all_positions_after_close = tl.get_all_positions()
    assert len(all_positions_after_close) == 0

    # Create two market orders/positions
    order_id1 = tl.create_order(
        default_instrument_id, quantity=0.01, side="buy", price=0, type_="market"
    )
    sleep(1)
    order_id2 = tl.create_order(
        default_instrument_id, quantity=0.02, side="sell", price=0, type_="market"
    )
    sleep(1)
    instrument_id3 = tl.get_instrument_id_from_symbol_name("ETHUSD")
    order_id3 = tl.create_order(
        instrument_id3, quantity=0.01, side="sell", price=0, type_="market"
    )

    # Check that the positions were received
    assert order_id1
    assert order_id2
    assert order_id3

    # Crude way of waiting for the orders to be filled
    all_orders = tl.get_all_orders(history=False)
    for _ in range(5):
        try:
            position_id1 = position_id_from_order_id(order_id1)
            sleep(0.5)
            position_id2 = position_id_from_order_id(order_id2)
            sleep(0.5)
            position_id3 = position_id_from_order_id(order_id3)
            break
        except ValueError:
            sleep(2)

    orders_history = tl.get_all_orders(history=True)
    assert order_id1 in orders_history["id"].values
    assert order_id2 in orders_history["id"].values
    assert order_id3 in orders_history["id"].values

    assert position_id1, "Position not created after 10 seconds!"
    assert position_id2, "Position not created after 10 seconds!"
    assert position_id3, "Position not created after 10 seconds!"

    # Check that the orders were filled and became positions
    all_positions = tl.get_all_positions()
    assert position_id1 in all_positions["id"].values
    assert position_id2 in all_positions["id"].values
    assert position_id3 in all_positions["id"].values

    tl.close_all_positions(instrument_id_filter=instrument_id3)
    sleep(2)

    # Check that only position_id3 was closed
    all_positions = tl.get_all_positions()
    assert position_id1 in all_positions["id"].values
    assert position_id2 in all_positions["id"].values
    assert position_id3 not in all_positions["id"].values

    tl.close_all_positions()
    sleep(2)

    # Check that the remaining positions were closed
    all_positions_after_close = tl.get_all_positions()
    assert position_id1 not in all_positions_after_close["id"].values
    assert position_id2 not in all_positions_after_close["id"].values
    assert position_id3 not in all_positions_after_close["id"].values


def test_modify_and_delete_order():
    orders_before = tl.get_all_orders(history=False)

    # create a limit order
    order_id: int = tl.create_order(
        default_instrument_id,
        quantity=0.01,
        side="buy",
        price=0.01,
        type_="limit",
        validity="GTC",
    )
    assert order_id
    tl_check_type(order_id, int)

    orders_after_buy = tl.get_all_orders(history=False)
    assert len(orders_after_buy) == len(orders_before) + 1
    assert order_id in orders_after_buy["id"].values
    last_modified_buy = orders_after_buy[orders_after_buy["id"] == order_id][
        "lastModified"
    ].values[0]

    all_orders_history = tl.get_all_orders(history=True)
    assert not all_orders_history.empty

    # modify the limit order
    tl.modify_order(order_id, modification_params={"price": "0.02", "qty": "0.02"})

    orders_after_modify = tl.get_all_orders(history=False)
    assert order_id in orders_after_modify["id"].values
    assert len(orders_after_modify) == len(orders_after_buy)
    last_modified_modify = orders_after_modify[orders_after_modify["id"] == order_id][
        "lastModified"
    ].values[0]

    assert last_modified_modify > last_modified_buy

    tl.delete_order(order_id)
    sleep(0.5)

    orders_after_delete = tl.get_all_orders(history=False)
    assert len(orders_after_delete) == len(orders_before)
    assert order_id not in orders_after_delete["id"].values

    oh_after_delete = tl.get_all_orders(history=True)
    # check the order status of the deleted order
    assert (
        oh_after_delete[oh_after_delete["id"] == order_id]["status"].values[0]
        == "Cancelled"
    )


def test_orders_history_time_ranges_and_instrument_filter():
    oh_full = tl.get_all_orders(history=True)
    oh_last_5_days = tl.get_all_orders(history=True, lookback_period="5D")
    assert len(oh_full) >= len(oh_last_5_days)

    LTCUSD_instrument_id = tl.get_instrument_id_from_symbol_name("LTCUSD")

    oh_last_1_day_LTCUSD_before = tl.get_all_orders(
        history=True, lookback_period="1D", instrument_id_filter=LTCUSD_instrument_id
    )

    order_id = tl.create_order(
        instrument_id=LTCUSD_instrument_id, quantity=0.01, side="buy", type_="market"
    )
    sleep(1)

    oh_last_1_day_LTCUSD = tl.get_all_orders(
        history=True, lookback_period="1D", instrument_id_filter=LTCUSD_instrument_id
    )

    assert len(oh_last_1_day_LTCUSD) == len(oh_last_1_day_LTCUSD_before) + 1

    tl.close_position(order_id=order_id)
    sleep(1)

    oh_last_1_day_LTCUSD_after = tl.get_all_orders(
        history=True, lookback_period="1D", instrument_id_filter=LTCUSD_instrument_id
    )

    assert len(oh_last_1_day_LTCUSD_after) == len(oh_last_1_day_LTCUSD_before) + 2

    oh_full_after = tl.get_all_orders(history=True)
    assert len(oh_full_after) == len(oh_full) + 2


def test_delete_all_orders():
    # tl.delete_all_orders_manual()
    tl.delete_all_orders()
    sleep(1)

    orders_before = tl.get_all_orders(history=False)
    orders_history_before = tl.get_all_orders(history=True)

    # create a limit order
    order_id1: int = tl.create_order(
        default_instrument_id,
        quantity=0.01,
        side="buy",
        price=0.01,
        type_="limit",
        validity="GTC",
    )
    sleep(1)
    order_id2: int = tl.create_order(
        default_instrument_id,
        quantity=0.01,
        side="sell",
        price=1000000.0,
        type_="limit",
        validity="GTC",
    )
    sleep(1)
    instrument_id3 = tl.get_instrument_id_from_symbol_name("ETHUSD")
    order_id3: int = tl.create_order(
        instrument_id3,
        quantity=0.01,
        side="buy",
        price=0.01,
        type_="limit",
        validity="GTC",
    )
    sleep(1)
    instrument_id4 = tl.get_instrument_id_from_symbol_name("DOGEUSD")
    order_id4: int = tl.create_order(
        instrument_id4,
        quantity=0.01,
        side="buy",
        price=0.02,
        type_="limit",
        validity="GTC",
    )

    assert order_id1
    assert order_id2
    assert order_id3
    assert order_id4

    orders_after_buy = tl.get_all_orders(history=False)
    assert len(orders_after_buy) == len(orders_before) + 4

    tl.delete_all_orders(instrument_id_filter=instrument_id3)
    sleep(1)

    orders_history_after = tl.get_all_orders(history=True)
    orders_after = tl.get_all_orders(history=False)

    # Only one order has become final ("Cancelled") and will thus be "added" to ordersHistory)
    assert len(orders_history_after) == len(orders_history_before) + 1

    # The one order that was deleted should not be on the orders list anymore
    assert len(orders_after) == len(orders_before) + 3
    assert orders_after[orders_after["id"] == order_id1]["status"].values[0] == "New"

    assert orders_after[orders_after["id"] == order_id2]["status"].values[0] == "New"

    # Check order status for order3 to be "Cancelled"
    assert order_id3 not in orders_after["id"].values

    assert orders_after[orders_after["id"] == order_id4]["status"].values[0] == "New"

    tl.delete_all_orders()
    # tl.delete_all_orders_manual()

    orders_final = tl.get_all_orders(history=False)
    sleep(0.5)
    oh_final = tl.get_all_orders(history=True)

    # Check that all order statuses are "Cancelled"
    assert oh_final[oh_final["id"] == order_id1]["status"].values[0] == "Cancelled"
    assert oh_final[oh_final["id"] == order_id2]["status"].values[0] == "Cancelled"
    assert oh_final[oh_final["id"] == order_id4]["status"].values[0] == "Cancelled"

    assert len(orders_final) == len(orders_before)
    assert len(oh_final) == len(orders_history_before) + 4
