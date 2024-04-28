import datetime
import json
from math import floor
from functools import lru_cache
from typing import cast, Literal, Optional, Tuple

import requests
import pandas as pd

import logging

from tradelocker.utils import (
    color_logger,
    log_func,
    get_nested_key,
    resolve_lookback_and_timestamps,
    retry,
    tl_typechecked,
    tl_check_type,
    estimate_history_size,
    time_to_token_expiry,
)

from .__about__ import __version__
from .types import (
    DailyBarType,
    ColumnConfigKeysType,
    ColumnConfigValuesType,
    ConfigColumnType,
    ConfigType,
    InstrumentDetailsType,
    LimitsType,
    LocaleType,
    LogLevelType,
    MarketDepthlistType,
    ModificationParamsType,
    OrderTypeType,
    RequestsMappingType,
    ResolutionType,
    RouteType,
    RouteTypeType,
    SessionDetailsType,
    SessionStatusDetailsType,
    TradeAccountsType,
    ValidityType,
    DictValuesType,
    CredentialsType,
    QuotesType,
    JSONType,
    SideType,
    AccountsColumns,
    ExecutionsColumns,
    OrdersColumns,
    PositionsColumns,
    PriceHistoryColumns,
    InstrumentsColumns,
    int64,
)

from time import sleep

# More information about the API: https://tradelocker.com/api

# Constants
_TIMEOUT: Tuple[int, int] = (10, 30)  # (connection_timeout, read_timeout
_EPS: float = 0.00001
_MIN_LOT_SIZE: float = (
    0.01  ## TODO: this should probably be fetched per-instrument from BE
)


class TLAPI:
    """TradeLocker API Client

    Implements a REST connection to the TradeLocker REST API.

    See https://tradelocker.com/api/ for more information.
    """

    _instances = {}

    # This is here to ensure that users don't accidentally create multiple instances
    # of the same TLAPI object, which would lead to multiple connections to the API for no reason.
    # However, different instances can be created with different parameters,
    # or a new instance will be created in case the access token has expired.
    def __new__(cls, *args, **kwargs):
        multiton_key = (cls, args, frozenset(kwargs.items()))
        # Generate a new instance only if the key is not in the instances dict or the access token has expired

        if (
            multiton_key in cls._instances
            and hasattr(cls._instances[multiton_key], "_access_token")
            and time_to_token_expiry(cls._instances[multiton_key]._access_token) > 0
        ):
            # Just warn the user, and reuse the existing instance
            logging.warning(f"Reusing existing TLAPI instance for {cls.__name__}")
        else:
            # Create a new instance
            cls._instances[multiton_key] = super(TLAPI, cls).__new__(cls)
        return cls._instances[multiton_key]

    @tl_typechecked
    def __init__(
        self,
        environment: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        server: Optional[str] = None,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        account_id: int = 0,
        acc_num: int = 0,
        log_level: LogLevelType = "debug",
    ) -> None:
        # Those object with _initialized tag have already been initialized,
        # so there is no need to re-initialize anything.
        if hasattr(self, "_initialized") and self._initialized:
            return

        """Initializes the TradeLocker API client."""
        self._base_url: str = f"{environment}/backend-api"
        self._credentials: Optional[CredentialsType] = None

        self._access_token: str = ""
        self._refresh_token: str = ""
        self.acc_num: int = 0
        self.account_id: int = 0
        self.environment: str = environment

        if username and password and server:
            self._credentials = {
                "username": username,
                "password": password,
                "server": server,
            }

        self.log = color_logger

        if self._credentials:
            self._auth_with_password(
                username=self._credentials["username"],
                password=self._credentials["password"],
                server=self._credentials["server"],
            )
            self._set_account_id_and_acc_num(account_id, acc_num)
        elif access_token and refresh_token:
            self._auth_with_tokens(access_token, refresh_token)
            self._set_account_id_and_acc_num(account_id, acc_num)
        else:
            error_msg = f"Either username/pass/server, or access_token/refresh_token must be provided!"
            raise Exception(error_msg)

        self._initialized = True

    def get_base_url(self) -> str:
        """Returns the base URL of the API."""
        return self._base_url

    ############################## AUTH ROUTES ##########################

    def get_access_token(self) -> str:
        """Returns the access token. If the token is about to expire, it will be refreshed."""

        # If auth token is not set, or refresh token has expired, try fetching a completely new one
        if not self._access_token or time_to_token_expiry(self._refresh_token) < 0:
            if not self._credentials:
                error_msg = "Cannot fetch or refresh authentication tokens"
                self.log.critical(error_msg)
                raise Exception(error_msg)
            else:
                self._auth_with_password(
                    self._credentials["username"],
                    self._credentials["password"],
                    self._credentials["server"],
                )

        # If there is less than 30 minutes to token expiry, refresh the token
        if (
            time_to_token_expiry(self._access_token)
            < datetime.timedelta(minutes=30).total_seconds()
        ):
            self.refresh_access_tokens()

        return self._access_token

    def get_refresh_token(self) -> str:
        """Returns the refresh token."""
        if not self._refresh_token or time_to_token_expiry(self._refresh_token) < 0:
            if self._credentials:
                self._auth_with_password(
                    self._credentials["username"],
                    self._credentials["password"],
                    self._credentials["server"],
                )
            else:
                error_msg = "No refresh token found or token expired"
                self.log.critical(error_msg)
                raise Exception(error_msg)
        return self._refresh_token

    def _set_account_id_and_acc_num(self, account_id: int, acc_num: int) -> None:
        all_accounts: pd.DataFrame = self.get_all_accounts()

        if all_accounts.empty:
            self.log.critical("No accounts found")
            raise Exception("No accounts found")

        # Pick the correct account, either by having account_id, or acc_num specified
        if account_id != 0:
            if account_id not in all_accounts["id"].values:
                raise ValueError(
                    f"account_id '{account_id}' not found in all_accounts:\n{all_accounts} "
                )

            self.account_id = account_id
            # Find the acc_num for the specified account_id
            self.acc_num = int(
                all_accounts[all_accounts["id"] == account_id]["accNum"].iloc[0]
            )
            self.account_name = all_accounts[all_accounts["id"] == account_id][
                "name"
            ].iloc[0]

            self.log.debug(
                f"Logging in using the specified account_id: {account_id}, using acc_num: {self.acc_num}"
            )

        elif acc_num != 0:
            if acc_num not in all_accounts["accNum"].values:
                raise ValueError(
                    f"acc_num '{acc_num}' not found in all_accounts:\n{all_accounts}"
                )

            self.acc_num = acc_num
            # Find the account_id for the specified acc_num
            self.account_id = int(
                all_accounts[all_accounts["accNum"] == acc_num]["id"].iloc[0]
            )
            self.account_name = all_accounts[all_accounts["accNum"] == acc_num][
                "name"
            ].iloc[0]

            self.log.debug(
                f"Logging in using the specified acc_num: {acc_num}, using account_id: {self.account_id}"
            )
        else:
            self.log.debug(
                "Neither account_id nor acc_num specified, using the first account"
            )
            # use the last account in the list
            self.account_id = int(all_accounts["id"].iloc[0])
            self.acc_num = int(all_accounts["accNum"].iloc[0])
            self.account_name = all_accounts["name"].iloc[0]

            self.log.debug(
                f"Logging in using the first account, account_id: {self.account_id}, acc_num: {self.acc_num}"
            )

    def _auth_with_tokens(self, access_token: str, refresh_token: str) -> None:
        """Stores the access and refresh tokens."""
        self._access_token = access_token
        self._refresh_token = refresh_token

    ############################## PRIVATE UTILS #######################

    @lru_cache
    @tl_typechecked
    def _get_column_names(self, object_name: ColumnConfigKeysType) -> list[str]:
        """Returns the column names of values in orders/positions, etc. from the /config endpoint

        Args:
            object_name (ColumnConfigKeysType): The name of the object to get the column names for
        Returns:
            list[str]: The column names
        """
        config_dict: ConfigType = self.get_config()

        # Using cast because /config is really ugly and irregular, so mypy needs help.
        config_for_object: ColumnConfigValuesType = cast(
            dict[ColumnConfigKeysType, ColumnConfigValuesType], config_dict
        )[object_name]

        config_columns: ConfigColumnType = cast(
            dict[Literal["columns"], ConfigColumnType], config_for_object
        )["columns"]

        tl_check_type(config_columns, ConfigColumnType)

        object_columns: list[str] = [column["id"] for column in config_columns]
        return object_columns

    @lru_cache
    def _get_trade_route_id(self, instrument_id: int) -> str:
        """Returns the "TRADE" route_id for the specified instrument_id"""
        return self._get_route_id(instrument_id, "TRADE")

    @lru_cache
    def _get_info_route_id(self, instrument_id: int) -> str:
        """Returns the "INFO" route_id for the specified instrument_id"""
        return self._get_route_id(instrument_id, "INFO")

    @lru_cache
    def _get_max_history_rows(self) -> int:
        config_dict: ConfigType = self.get_config()
        limits: list[LimitsType] = get_nested_key(
            config_dict, ["limits"], list[LimitsType]
        )
        for limit in limits:
            if limit["limitType"] == "QUOTES_HISTORY_BARS":
                return limit["limit"]
        raise Exception("Failed to fetch max history rows")

    @tl_typechecked
    def _get_route_id(self, instrument_id: int, route_type: RouteTypeType) -> str:
        """Returns the route_id for the specified instrument_id and route_type (TRADE/INFO)"""
        all_instruments: pd.DataFrame = self.get_all_instruments()
        matching_instruments: pd.DataFrame = all_instruments[
            all_instruments["tradableInstrumentId"] == instrument_id
        ]
        routes: list[RouteType] = matching_instruments["routes"].iloc[0]
        # From the list of routes, find the one where "type" is route_type
        route_id = [route["id"] for route in routes if route["type"] == route_type][0]
        return str(route_id)

    @tl_typechecked
    def _get_headers(
        self,
        additional_headers: Optional[RequestsMappingType] = None,
        include_acc_num: bool = True,
    ) -> RequestsMappingType:
        """Returns a header with a fresh JWT token, additional_headers and (potentially) accNum

        Args:
            additional_headers: Additional headers to include in the request
            include_acc_num: Whether to include the accNum header in the request

        Returns:
            The final headers
        """
        headers: RequestsMappingType = {
            "Authorization": f"Bearer {self.get_access_token()}",
        }
        if include_acc_num:
            headers["accNum"] = str(self.acc_num)

        if additional_headers is not None:
            headers.update(cast(RequestsMappingType, additional_headers))

        return headers

    @tl_typechecked
    def _get_params(
        self, additional_params: Optional[DictValuesType] = None
    ) -> RequestsMappingType:
        """Converts all params values to strings and adds the referral values

        Args:
            additional_params: Additional parameters to include in the request

        Returns:
            The final parameters
        """
        final_params: RequestsMappingType = {"ref": "py_c", "v": __version__}
        if additional_params is not None:
            for key, value in cast(RequestsMappingType, additional_params).items():
                final_params[key] = str(value)

        return final_params

    @tl_typechecked
    # Raises HTTP related exceptions and tries extracting a JSON from the response
    def _get_response_json(self, response: requests.Response) -> JSONType:
        """Returns the JSON from a requests response.

        Args:
            response: The response to extract the JSON from

        Returns:
            The response JSON

        Raises:
            HTTPError: Will be raised if the request fails
            Exception: Will be raised if the response is empty
        """
        response.raise_for_status()

        if response.text == "":
            raise Exception(f"Empty response received from the API for {response.url}")

        try:
            response_json: JSONType = response.json()
            return response_json
        except json.decoder.JSONDecodeError as err:
            self.log.error(
                f"Failed to decode JSON response from {response.url}. Received response:\n'{response.text}'\n{err}"
            )
            raise err

    @retry
    @tl_typechecked
    def _request_get(
        self,
        url: str,
        additional_headers: Optional[RequestsMappingType] = None,
        additional_params: Optional[DictValuesType] = None,
        include_acc_num: bool = True,
    ) -> JSONType:
        """Performs a GET request to the specified URL.

        Args:
            url: The URL to send the request to
            additional_headers: Additional headers to include in the request
            additional_params: Additional parameters to include in the request
            include_acc_num: Whether to include the accNum header in the request

        Returns:
            The response JSON

        Raises:
            HTTPError: Will be raised if the request fails
        """

        headers = self._get_headers(additional_headers, include_acc_num=include_acc_num)
        params = self._get_params(additional_params)

        response = requests.get(
            url=url, headers=headers, params=params, timeout=_TIMEOUT
        )
        response_json = self._get_response_json(response)
        return response_json

    def _apply_typing(
        self, df: pd.DataFrame, column_types: dict[str, type]
    ) -> pd.DataFrame:
        """Converts columns of int and float type from str to numeric values.

        Args:
            columns_types (dict[str, type]): The column types to apply

        Returns:
            pd.DataFrame: The DataFrame with the types applied
        """
        for column in df.columns:
            if column not in column_types:
                self.log.error(
                    f"Missing type specification for column {column} in {column_types}"
                )
            else:
                try:
                    # Only convert the ints and floats after replacing "None" values with 0
                    if column_types[column] in [int64, float]:
                        df[column] = df[column].fillna(0).astype(column_types[column])

                except Exception as err:
                    self.log.warning(
                        f"Failed to apply type {column_types[column]} to column {column}: {err}"
                    )

    ############################## PUBLIC UTILS #######################

    @tl_typechecked
    @lru_cache
    @log_func
    def get_instrument_id_from_symbol_name(self, symbol_name: str) -> int:
        """Returns the instrument Id from the given symol's name.

        Args:
            symbol_name (str): Name of the symbol, for example `BTCUSD`

        Raises:
            ValueError: Will be raised if instrument was with given symbol name was not found

        Returns:
            int: On success the instrument Id will be returned
        """
        all_instruments: pd.DataFrame = self.get_all_instruments()
        matching_instruments = all_instruments[all_instruments["name"] == symbol_name]
        if len(matching_instruments) == 0:
            raise ValueError(f"No instrument found with {symbol_name=}")
        if len(matching_instruments) > 1:
            self.log.warning(
                f"Multiple instruments found with {symbol_name=}. Using the first one."
            )

        return int(matching_instruments["tradableInstrumentId"].iloc[0])

    @log_func
    @tl_typechecked
    def get_instrument_id_from_symbol_id(self, symbol_id: int) -> int:
        """Returns the instrument Id from the given symbol's id.

        Args:
            symbol_id (int): Id the symbol

        Raises:
            ValueError: Will be raised if instrument was with given symbol id was not found

        Returns:
            int: On success the instrument Id will be returned
        """
        all_instruments: pd.DataFrame = self.get_all_instruments()
        matching_instruments = all_instruments[all_instruments["id"] == symbol_id]
        if len(matching_instruments) == 0:
            raise ValueError(f"No instrument found with {symbol_id=}")
        if len(matching_instruments) > 1:
            self.log.warning(
                f"Multiple instruments found with {symbol_id=}. Using the first one."
            )

        return int(matching_instruments["tradableInstrumentId"].iloc[0])

    @log_func
    @tl_typechecked
    def get_symbol_name_from_instrument_id(self, instrument_id: int) -> str:
        """Returns the symbol name from the given instrument Id.

        Args:
            instrument_id (int): The instrument Id
        Returns:
            str: On success the symbol name will be returned
        """

        all_instruments: pd.DataFrame = self.get_all_instruments()
        matching_instruments = all_instruments[
            all_instruments["tradableInstrumentId"] == instrument_id
        ]
        if len(matching_instruments) == 0:
            raise ValueError(f"No instrument found with id = {instrument_id}")

        self.log.debug(
            f"(get_symbol_name_from_instrument_id) instrument_id: {instrument_id}"
        )
        self.log.debug(f"matching_instruments:\n{matching_instruments}")
        return matching_instruments["name"].iloc[0]

    @log_func
    @tl_typechecked
    def close_all_positions(self, instrument_id_filter: int = 0) -> bool:
        """Places an order to close all open positions.

        If instrument_id is provied, only positions in this instrument will be closed.

        IMPORTANT: Isn't guaranteed to close all positions, or close them immediately.
        Will attempt to place an IOC, then GTC closing order, so the execution might be delayed.

        Args:
            instrument_id_filter (int, optional): _description_. Defaults to 0.

        Returns:
            bool: True if executed successfully False otherwise
        """
        route_url = f"{self._base_url}/trade/accounts/{self.account_id}/positions"

        additional_params: Optional[DictValuesType] = None
        if instrument_id_filter != 0:
            additional_params = {"tradableInstrumentId": str(instrument_id_filter)}

        response = requests.delete(
            route_url,
            headers=self._get_headers(),
            params=self._get_params(additional_params=additional_params),
            timeout=_TIMEOUT,
        )
        response.raise_for_status()
        response_json = self._get_response_json(response)
        response_status: str = get_nested_key(response_json, ["s"], str)
        return response_status == "ok"

    @log_func
    @tl_typechecked
    def delete_all_orders_manual(self, instrument_id_filter: int = 0) -> bool:
        """Deletes all pending orders.

        If instrument_id is provided, only pending orders in this instrument will be closed

        Args:
            instrument_id_filter (int, optional): The instrument id to use. Defaults to 0.

        Returns:
            bool: True if executed successfully False otherwise
        """
        route_url = f"{self._base_url}/trade/accounts/{self.account_id}/orders"

        orders = self.get_all_orders(
            history=False, instrument_id_filter=instrument_id_filter
        )
        # iterate over all rows of the orders dataframe
        for index, row in orders.iterrows():
            order_id = row["id"]
            self.delete_order(order_id)
            sleep(1)

        return True

    @log_func
    @tl_typechecked
    def delete_all_orders(self, instrument_id_filter: int = 0) -> bool:
        """Deletes all pending orders.

        If instrument_id is provided, only pending orders in this instrument will be closed

        Args:
            instrument_id_filter (int, optional): The instrument id to use. Defaults to 0.

        Returns:
            bool: True if executed successfully False otherwise
        """
        route_url = f"{self._base_url}/trade/accounts/{self.account_id}/orders"

        additional_params: Optional[DictValuesType] = None
        if instrument_id_filter != 0:
            additional_params = {"tradableInstrumentId": str(instrument_id_filter)}

        response = requests.delete(
            route_url,
            headers=self._get_headers(),
            params=self._get_params(additional_params=additional_params),
            timeout=_TIMEOUT,
        )
        response.raise_for_status()
        response_json = self._get_response_json(response)
        response_status: str = get_nested_key(response_json, ["s"], str)
        return response_status == "ok"

    @log_func
    @tl_typechecked
    def _place_close_position_order(
        self, position_id: int, quantity: float = 0
    ) -> bool:
        route_url = f"{self._base_url}/trade/positions/{position_id}"

        data = {"qty": str(quantity)}

        response = requests.delete(
            url=route_url,
            json=data,
            headers=self._get_headers(),
            params=self._get_params(),
            timeout=_TIMEOUT,
        )
        response.raise_for_status()
        response_json = self._get_response_json(response)
        response_status: str = get_nested_key(response_json, ["s"], str)

        return response_status == "ok"

    @log_func
    @tl_typechecked
    def close_position(
        self, order_id: int = 0, position_id: int = 0, close_quantity: float = 0
    ) -> None:
        """Places an order to closee a position.

        Either the order_id or the position_id needs to be provided. If both are
        provided, the order_id will be used and the position_id will be ignored.

        IMPORTANT: Isn't guaranteed to close the position, or close it immediately.
        Will attempt to place an IOC, then GTC closing order, so the execution might be delayed.

        Args:
            order_id (int, optional): The order id. Defaults to 0.
            position_id (int, optional): The position id. Defaults to 0.
            close_quantity (float, optional): If a value bigger than 0 is provided the size of the position will be reduced by the given amount. Defaults to 0.

        Raises:
            ValueError: Will be raised if no order_id or position_id was provided
        """
        if order_id == 0 and position_id == 0:
            raise ValueError("Either order_id or position_id must be provided!")
        if order_id != 0 and position_id != 0:
            self.log.warning(
                "Both order_id and position_id provided. position_id will be ignored."
            )

        # Important: make sure to use ordersHistory since some orders might have been from previous sessions
        all_orders = self.get_all_orders(history=True)

        selection_criteria: str = ""
        if order_id != 0:
            matching_orders = all_orders[all_orders["id"] == order_id]
            selection_criteria = f"order_id: {order_id}"
        else:
            matching_orders = all_orders[all_orders["positionId"] == position_id]
            selection_criteria = f"position_id: {position_id}"

        rejected_matching_orders = matching_orders[
            matching_orders["status"] == "Rejected"
        ]
        if len(rejected_matching_orders.index) > 0:
            self.log.warning(f"Rejected orders found for {selection_criteria}!")

        # leave only filled orders
        matching_orders = matching_orders[matching_orders["status"] == "Filled"]

        if len(matching_orders.index) == 0:
            self.log.error(f"No matching position found for {selection_criteria}!")

        if len(matching_orders.index) > 1:
            self.log.warning(
                f"Multiple positions found for {selection_criteria}! Attempting to close all: \n{matching_orders}"
            )

        for _, row in matching_orders.iterrows():
            quantity_to_close: float = float(row["qty"])
            if close_quantity:
                quantity_to_close = min(quantity_to_close, close_quantity)
                close_quantity -= quantity_to_close

            if quantity_to_close < _MIN_LOT_SIZE:
                self.log.warning(
                    f"Quantity to close ({quantity_to_close}) is less than minimum lot size ({_MIN_LOT_SIZE}). Skipping."
                )
                continue

            self._place_close_position_order(
                position_id=int(row["positionId"]), quantity=quantity_to_close
            )

    ############################## AUTH ROUTES ##########################

    @tl_typechecked
    def _auth_with_password(self, username: str, password: str, server: str) -> None:
        """Fetches and sets access tokens for api access.

        Args:
            username (str): Username
            password (str): Password
            server (str): Server name

        Raises:
            Exception: Will be raised on authentication errors
        """
        route_url = f"{self._base_url}/auth/jwt/token"

        data = {"email": username, "password": password, "server": server}

        response = requests.post(url=route_url, json=data, timeout=_TIMEOUT)
        try:
            response_json = self._get_response_json(response)
            self._access_token = get_nested_key(response_json, ["accessToken"], str)
            self._refresh_token = get_nested_key(response_json, ["refreshToken"], str)
            assert self._access_token and self._refresh_token
            self.log.info("Successfully fetched authentication tokens")
        except Exception as err:
            self.log.critical(f"Failed to fetch authentication tokens: {err}")
            # Explicitly re-raise from err
            raise Exception(f"Failed to fetch authentication tokens: {err}") from err
            # raise Exception(f"Failed to fetch authentication tokens: {err}")

    @tl_typechecked
    def refresh_access_tokens(self) -> None:
        """Refreshes authentication tokens."""
        route_url = f"{self._base_url}/auth/jwt/refresh"

        data = {"refreshToken": self._refresh_token}

        response = requests.post(url=route_url, json=data, timeout=_TIMEOUT)
        response_json = self._get_response_json(response)

        self.log.info("Successfully refreshed authentication tokens")

        self._access_token = get_nested_key(response_json, ["accessToken"], str)
        self._refresh_token = get_nested_key(response_json, ["refreshToken"], str)

    @lru_cache(maxsize=1)
    @log_func
    @tl_typechecked
    def get_all_accounts(self) -> pd.DataFrame:
        """Returns all accounts associated with the account used for authentication.

        Raises:
            Exception: Will be raised if account informations could not be fetched

        Returns:
            pd.DataFrame[AccountsColumnsTypes]: DataFrame with user's accounts
        """
        route_url = f"{self._base_url}/auth/jwt/all-accounts"

        # Make sure we don't try including accNum into the header, as it is not chosen yet
        response_json = self._request_get(route_url, include_acc_num=False)
        accounts_json = get_nested_key(response_json, ["accounts"])

        accounts = pd.DataFrame(accounts_json)
        self._apply_typing(accounts, AccountsColumns)

        if not accounts_json or accounts.empty:
            self.log.critical("Failed to fetch user's accounts")
            raise Exception("Failed to fetch user's accounts")

        return accounts

    ############################## CONFIG ROUTES ##########################

    @lru_cache(maxsize=1)
    @log_func
    @tl_typechecked
    def get_config(self) -> ConfigType:
        """Returns the user's configuration.

        Returns:
            ConfigType: The configuration
        """
        route_url = f"{self._base_url}/trade/config"
        response_json = self._request_get(route_url)
        config_dict: ConfigType = get_nested_key(response_json, ["d"], ConfigType)
        return config_dict

    ############################## ACCOUNT ROUTES ##########################

    @log_func
    @tl_typechecked
    def get_trade_accounts(self) -> TradeAccountsType:
        """Returns the account information.

        The account is defined by the acc_num used in constructor.

        Returns:
            TradeAccountsType: The account details
        """
        route_url = f"{self._base_url}/trade/accounts"

        response_json = self._request_get(route_url)

        trade_accounts: TradeAccountsType = get_nested_key(
            response_json, ["d"], TradeAccountsType
        )
        return trade_accounts

    @log_func
    @tl_typechecked
    def get_all_executions(self) -> pd.DataFrame:
        """Returns a list of orders executed in account in current session.

        Returns:
            pd.DataFrame[ExecutionsColumnTypes]: DataFrame containing all executed orders
        """
        route_url = f"{self._base_url}/trade/accounts/{self.account_id}/executions"

        response_json = self._request_get(route_url)

        column_names = self._get_column_names("filledOrdersConfig")

        all_executions = pd.DataFrame(
            get_nested_key(response_json, ["d", "executions"]), columns=column_names
        )
        self._apply_typing(all_executions, ExecutionsColumns)

        return all_executions

    @lru_cache(maxsize=1)
    @log_func
    def get_all_instruments(self) -> pd.DataFrame:
        """Returns all available instruments for account.

        Returns:
            pd.DataFrame[InstrumentsColumnsTypes]: DataFrame with all available instruments
        """
        route_url = f"{self._base_url}/trade/accounts/{self.account_id}/instruments"

        response_json = self._request_get(route_url)

        all_instruments = pd.DataFrame.from_dict(
            get_nested_key(response_json, ["d", "instruments"])
        )
        self._apply_typing(all_instruments, InstrumentsColumns)
        return all_instruments

    # This function handles both the /orders and the /ordersHistory endpoint
    @log_func
    @tl_typechecked
    def get_all_orders(
        self,
        lookback_period: str = "",
        start_timestamp: int = 0,
        end_timestamp: int = 0,
        instrument_id_filter: int = 0,
        history: bool = False,
    ) -> pd.DataFrame:
        """Returns all orders associated with the account.
        If history is set to True, it will return all orders from the beginning of the session.
        If history is set to False, it will return only orders that have not been executed yet.
        The default value is False.
        If the account has no orders, an empty DataFrame is returned.

        Args:
            history (bool, optional): Should historical orders be returned. Defaults to False.

        Returns:
            pd.DataFrame[OrdersColumnsTypes]: DataFrame containing all orders
        """
        endpoint = "orders" + ("History" if history else "")
        route_url = f"{self._base_url}/trade/accounts/{self.account_id}/{endpoint}"

        if lookback_period != "":
            start_timestamp, end_timestamp = resolve_lookback_and_timestamps(
                lookback_period=lookback_period,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )

        additional_params: Optional[DictValuesType] = {}
        if instrument_id_filter != 0:
            additional_params["tradableInstrumentId"] = instrument_id_filter
        if start_timestamp != 0:
            additional_params["from"] = start_timestamp
        if end_timestamp != 0:
            additional_params["to"] = end_timestamp

        params: RequestsMappingType = self._get_params(
            additional_params=additional_params
        )

        response_json = self._request_get(route_url, additional_params=params)
        all_orders_raw = get_nested_key(response_json, ["d", endpoint])

        column_names = self._get_column_names(endpoint + "Config")
        all_orders = pd.DataFrame(all_orders_raw, columns=column_names)
        self._apply_typing(all_orders, OrdersColumns)

        return all_orders

    @log_func
    @tl_typechecked
    def get_all_positions(self) -> pd.DataFrame:
        """Returns all open positions for account.

        Returns:
            pd.DataFrame[PositionsColumnsTypes]: DataFrame containing all positions
        """
        route_url = f"{self._base_url}/trade/accounts/{self.account_id}/positions"

        response_json = self._request_get(route_url)
        all_positions_raw = get_nested_key(response_json, ["d", "positions"])

        all_positions_columns = self._get_column_names("positionsConfig")
        all_positions = pd.DataFrame(all_positions_raw, columns=all_positions_columns)
        self._apply_typing(all_positions, PositionsColumns)

        return all_positions

    @log_func
    @tl_typechecked
    def get_account_state(self) -> DictValuesType:
        route_url = f"{self._base_url}/trade/accounts/{self.account_id}/state"

        response_json = self._request_get(route_url)
        account_state_values = get_nested_key(
            response_json, ["d", "accountDetailsData"]
        )
        account_state = dict(
            zip(self._get_column_names("accountDetailsConfig"), account_state_values)
        )
        return account_state

    ############################## INSTRUMENT ROUTES #######################

    @log_func
    @tl_typechecked
    def get_instrument_details(
        self, instrument_id: int, locale: LocaleType = "en"
    ) -> InstrumentDetailsType:
        """Returns instrument details for a given instrument Id.

        Args:
            instrument_id (int): The instrument Id
            locale (LocaleType, optional): Locale (language) id. Defaults to "en".

        Returns:
            InstrumentDetailsType: The instrument details
        """
        route_url = f"{self._base_url}/trade/instruments/{instrument_id}"

        params: RequestsMappingType = self._get_params(
            {"routeId": self._get_info_route_id(instrument_id), "locale": locale}
        )

        response_json = self._request_get(route_url, additional_params=params)
        instrument_details: InstrumentDetailsType = get_nested_key(
            response_json, ["d"], InstrumentDetailsType
        )
        return instrument_details

    @log_func
    @tl_typechecked
    def get_session_details(self, session_id: int) -> SessionDetailsType:
        """Returns details about the session defined by session_id.

        Args:
            session_id (int): Session id

        Returns:
            SessionDetailsType: Session details
        """
        route_url = f"{self._base_url}/trade/sessions/{session_id}"

        response_json = self._request_get(route_url)
        session_details: SessionDetailsType = get_nested_key(
            response_json, ["d"], SessionDetailsType
        )
        return session_details

    @log_func
    @tl_typechecked
    def get_session_status_details(
        self, session_status_id: int
    ) -> SessionStatusDetailsType:
        """Returns details about the session status.

        Args:
            session_status_id (int): Session id

        Returns:
            SessionStatusDetailsType: Session details
        """
        route_url = f"{self._base_url}/trade/sessionStatuses/{session_status_id}"

        response_json = self._request_get(route_url)
        session_status_details: SessionStatusDetailsType = get_nested_key(
            response_json, ["d"], SessionStatusDetailsType
        )
        return session_status_details

    ############################## MARKET DATA ROUTES ######################

    @log_func
    @tl_typechecked
    def get_daily_bar(
        self, instrument_id: int, bar_type: Literal["BID", "ASK", "TRADE"] = "ASK"
    ) -> DailyBarType:
        """Returns daily candle data for requested instrument.

        Args:
            instrument_id (int): Instrument Id
            bar_type (Literal[BID, ASK, TRADE], optional): The type of candle data to return. Defaults to "ASK".

        Returns:
            DailyBarType: Daily candle data
        """
        route_url = f"{self._base_url}/trade/dailyBar"

        params: RequestsMappingType = self._get_params(
            {
                "tradableInstrumentId": instrument_id,
                "routeId": self._get_info_route_id(instrument_id),
                "barType": bar_type,
            }
        )

        response_json = self._request_get(route_url, additional_params=params)
        daily_bar: DailyBarType = get_nested_key(response_json, ["d"], DailyBarType)
        return daily_bar

    # Returns asks and bids
    @log_func
    @tl_typechecked
    def get_market_depth(self, instrument_id: int) -> MarketDepthlistType:
        """Returns market depth information for the requested instrument.

        Args:
            instrument_id (int): Instrument Id

        Returns:
            MarketDepthlistType: Market depth data
        """
        route_url = f"{self._base_url}/trade/depth"

        params: RequestsMappingType = self._get_params(
            {
                "tradableInstrumentId": instrument_id,
                "routeId": self._get_info_route_id(instrument_id),
            }
        )

        response_json = self._request_get(route_url, additional_params=params)
        market_depth: MarketDepthlistType = get_nested_key(
            response_json, ["d"], MarketDepthlistType
        )
        return market_depth

    @log_func
    @tl_typechecked
    def get_price_history(
        self,
        instrument_id: int,
        resolution: ResolutionType = "15m",
        lookback_period: str = "",
        start_timestamp: int = 0,  # timestamps are in miliseconds!
        end_timestamp: int = 0,
    ) -> pd.DataFrame:
        """Returns price history data for the requested instrument.

        Args:
            instrument_id (int): Instrument Id
            resolution (ResolutionType, optional): Data resolution. Defaults to "15m".
            lookback_period (str, optional): Lookback period (for example "5m"). Defaults to "".
            start_timestamp (int, optional): Start timestamp (in ms). Defaults to 0.
            end_timestamp: (int, optional): End timestamp (in ms). Defaults to 0.

        Raises:
            ValueError: Will be raised on a invalid response

        Returns:
            pd.DataFrame[PriceHistoryColumnsTypes]: DataFrame containing instrument's historical data
        """
        route_url = f"{self._base_url}/trade/history"

        start_timestamp, end_timestamp = resolve_lookback_and_timestamps(
            lookback_period, start_timestamp, end_timestamp
        )

        history_size = estimate_history_size(start_timestamp, end_timestamp, resolution)
        if history_size > self._get_max_history_rows():
            raise ValueError(
                f"No. of requested rows ({history_size}) larger than max allowed ({self._get_max_history_rows()})."
                "Try splitting your request in smaller chunks."
            )

        params: RequestsMappingType = self._get_params(
            {
                "tradableInstrumentId": instrument_id,
                "routeId": self._get_info_route_id(instrument_id),
                "resolution": resolution,
                "from": start_timestamp,  # convert to milliseconds
                "to": end_timestamp,
            }
        )

        response_json = self._request_get(route_url, additional_params=params)

        try:
            bar_details = pd.DataFrame(
                get_nested_key(response_json, ["d", "barDetails"])
            )
        except KeyError as err:
            if response_json["s"] == "no_data":
                self.log.warning("No data returned from the API for the given period")
                # Specify column names to make sure they exist even for empty returns
                bar_details = pd.DataFrame(columns=["t", "o", "h", "l", "c", "v"])
            else:
                raise err

        self._apply_typing(bar_details, PriceHistoryColumns)

        return bar_details

    @log_func
    @tl_typechecked
    def get_latest_asking_price(self, instrument_id: int) -> float:
        """Returns latest price informations for requested instrument.

        Args:
            instrument_id (int): Instrument Id

        Returns:
            float: Latest price of the instrument
        """
        current_quotes: dict[str, float] = cast(
            dict[str, float], self.get_quotes(instrument_id)
        )
        current_ap: float = get_nested_key(current_quotes, ["ap"], float)
        return current_ap

    @log_func
    @tl_typechecked
    def get_quotes(self, instrument_id: int) -> QuotesType:
        """Returns price quotes for requested instrument.

        Args:
            instrument_id (int): Instrument Id

        Returns:
            QuotesType: Price quotes for instrument
        """
        route_url = f"{self._base_url}/trade/quotes"

        params: RequestsMappingType = self._get_params(
            {
                "tradableInstrumentId": instrument_id,
                "routeId": self._get_info_route_id(instrument_id),
            }
        )

        response_json = self._request_get(route_url, additional_params=params)
        latest_price: QuotesType = get_nested_key(response_json, ["d"], QuotesType)
        return latest_price

    @log_func
    @tl_typechecked
    def _perform_order_netting(
        self, instrument_id: int, new_position_side: SideType, quantity: float
    ) -> float:
        """Closes opposite orders (smallest first) to net against the new order.

        Sorts the opposite orders by quantity (ascending) and closes them one by one until
        the total quantity of the new order is netted.

        Args:
            instrument_id (int): Instrument Id
            new_position_side (SideType): Side to which we want to increase the position
            quantity (float): Order size

        Returns:
            float: Total amount that was netted

        """
        opposite_side: str = "sell" if (new_position_side == "buy") else "buy"

        all_positions = self.get_all_positions()
        opposite_positions = all_positions.loc[
            (
                (all_positions["tradableInstrumentId"] == instrument_id)
                & (all_positions["side"] == opposite_side)
            )
        ]

        # Sort opposite positions by qty (ascending)
        opposite_positions = opposite_positions.sort_values(by="qty")

        total_netted: float = 0
        for _, position in opposite_positions.iterrows():
            if not position["stopLossId"] and not position["takeProfitId"]:
                # Compute how much to close in case a partial close would be needed
                quantity_to_close = min(position["qty"], float(quantity) - total_netted)

                self.log.info(
                    "Closing position {position_id}, {quantity_to_close} due to position_netting order {order}"
                )
                self.close_position(
                    position_id=position["id"], close_quantity=quantity_to_close
                )
                total_netted += quantity_to_close

                # If sufficient orders have been placed, return
                if abs(total_netted - float(quantity)) < _EPS:
                    self.log.debug(
                        "New position completely netted from opposite positions."
                    )
                    break

        return total_netted

    @log_func
    @tl_typechecked
    def create_order(
        self,
        instrument_id: int,
        quantity: float,
        side: SideType,
        price: float = 0,
        type_: OrderTypeType = "market",
        validity: Optional[ValidityType] = None,
        position_netting: bool = False,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
    ) -> Optional[int]:
        """Creates an order.

        Args:
            instrument_id (int): Instrument Id
            quantity (float): Order size
            side (SideType): Order side
            price (float, optional): Price for non-market orders. Defaults to 0.
            type_ (OrderTypeType, optional): Order type. Defaults to "market".
            validity (ValidityType, optional): Validity type of order. Defaults to "IOC".
            position_netting (bool, optional): Should position netting be used. Defaults to False.
            stop_loss (float, optional): Stop Loss. Defaults to None.
            take_profit (float, optional): Take Profit. Defaults to None.

        Returns:
            Optional[int]: order_id or None if order could not be placed
        """
        route_url = f"{self._base_url}/trade/accounts/{self.account_id}/orders"

        if type_ == "market" and price != 0:
            self.log.warning("Price specified for a market order. Ignoring the price.")
            price = 0

        if type_ == "market":
            if validity and validity != "IOC":
                error_msg = (
                    f"Market orders must use IOC as validity. Not placing the order."
                )
                self.log.error(error_msg)
                raise ValueError(error_msg)
            else:
                validity = "IOC"

        if type_ == "limit":
            if type_ in ["limit", "stop"] and validity and validity != "GTC":
                error_msg = (
                    f"{type_} orders must use GTC as validity. Not placing the order."
                )
                self.log.error(error_msg)
                raise ValueError(error_msg)
            else:
                validity = "GTC"

        # Make sure that quantity is positive. If not, switch the side of the order
        if quantity < 0:
            quantity = abs(quantity)
            side = "sell" if (side == "buy") else "buy"
            self.log.warning(
                "Quantity was negative, Continuing by changing the side of the order."
            )

        # Make sure that quantity is a multiple of 0.01
        floored_quantity = floor(quantity * 100) / 100
        if abs(quantity - floored_quantity) > _EPS:
            old_quantity = quantity
            quantity = floored_quantity
            self.log.warning(
                f"Quantity {old_quantity} was not a multiple of 0.01."
                f"Continuing by rounding down the quantity to {quantity}."
            )

        # If the quantity is smaller than the minimum lot size, return
        if quantity < _MIN_LOT_SIZE:
            self.log.warning(
                "Unable to place an order with quantity smaller than min lot size of {_MIN_LOT_SIZE}"
            )
            return None

        request_body: dict[str, str] = {
            "price": str(price),
            "qty": str(quantity),
            "routeId": self._get_trade_route_id(instrument_id),
            "side": side,
            "validity": validity,
            "tradableInstrumentId": str(instrument_id),
            "type": type_,
            "stopLoss": stop_loss,
            "takeProfit": take_profit,
        }

        if position_netting:
            # Try finding opposite orders to net against
            if type_ == "market":
                total_netted = self._perform_order_netting(
                    instrument_id, side, quantity
                )
                # Reduce the necessary quantity by the total_amount that was netted
                request_body["qty"] = str(float(request_body["qty"]) - total_netted)
                if float(request_body["qty"]) < _MIN_LOT_SIZE:
                    self.log.info(
                        "Not placing a new order after closing sufficient opposite orders due to netting."
                    )
                    return None
            else:
                self.log.warning(
                    "Order netting is only supported for market orders. Continuing without netting."
                )

        # Place the order
        response = requests.post(
            url=route_url,
            headers=self._get_headers({"Content-type": "application/json"}),
            json=request_body,
            timeout=_TIMEOUT,
        )
        response_json = self._get_response_json(response)
        try:
            order_id: int = int(get_nested_key(response_json, ["d", "orderId"], str))
            self.log.info(f"Order {request_body} placed with order_id: {order_id}")
            return order_id
        except KeyError as err:
            self.log.error(f"Unable to place order {request_body}. Error: {err}")
            return None

    @log_func
    @tl_typechecked
    def delete_order(self, order_id: int) -> bool:
        """Deletes a pending order.

        Args:
            order_id (int): Order Id

        Returns:
            bool: True on success, False on error
        """
        route_url = f"{self._base_url}/trade/orders/{order_id}"

        self.log.info(f"Deleting order with id {order_id}")

        response = requests.delete(
            url=route_url,
            headers=self._get_headers(),
            params=self._get_params(),
            timeout=_TIMEOUT,
        )
        response_json = self._get_response_json(response)

        self.log.info(f"Order deletion response: {response.json()}")
        response_status: str = get_nested_key(response_json, ["s"], str)

        return response_status == "ok"

    @log_func
    @tl_typechecked
    def modify_order(
        self, order_id: int, modification_params: ModificationParamsType
    ) -> bool:
        """Modifies a pending order -- a thin wrapper around PATCH /trade/orders/{order_id}.

        Args:
            order_id (int): Order Id
            modification_params (ModificationParamsType): Order modification details

        Returns:
            bool: True on success, False on error
        """
        route_url = f"{self._base_url}/trade/orders/{order_id}"

        self.log.info(f"Modifying the order with id {order_id}")

        response = requests.patch(
            url=route_url,
            headers=self._get_headers({"Content-type": "application/json"}),
            json=modification_params,
            timeout=_TIMEOUT,
        )
        response_json = self._get_response_json(response)
        self.log.info(f"Order modification response: {response_json}")
        response_status: str = get_nested_key(response_json, ["s"], str)
        return response_status == "ok"
