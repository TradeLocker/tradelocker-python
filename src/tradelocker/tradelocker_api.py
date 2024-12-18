from collections import defaultdict
import datetime
import json
from functools import lru_cache
from typing import Callable, cast, Literal, Optional, DefaultDict, Any
import logging
import requests

import jwt

import pandas as pd

from requests.exceptions import HTTPError
from tradelocker.utils import (
    get_logger,
    setup_utils_logging,
    log_func,
    get_nested_key,
    resolve_lookback_and_timestamps,
    retry,
    tl_typechecked,
    tl_check_type,
    estimate_history_size,
    time_to_token_expiry,
    disk_or_memory_cache,
    always_return_true,
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
    RouteNamesType,
    RateLimitType,
    StopLossType,
    TakeProfitType,
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
from .exceptions import TLAPIException, TLAPIOrderException

# Monkey-patch the original Joblib's method to not check code equality
from joblib.memory import MemorizedFunc

# We are "always returning true" to avoid checking the code of the function.
# Instead, we will be constructing _cache_key to include the release version
MemorizedFunc._check_previous_func_code = always_return_true

from joblib import Memory, expires_after

# More information about the API: https://tradelocker.com/api


class TLAPI:
    """TradeLocker API Client

    Implements a REST connection to the TradeLocker REST API.

    See https://tradelocker.com/api/ for more information.
    """

    # Constants
    _TIMEOUT: tuple[int, int] = (10, 30)  # (connection_timeout, read_timeout
    _EPS: float = 0.00001
    _MIN_LOT_SIZE: float = 0.01  ## TODO: this should probably be fetched per-instrument from BE
    _LOGGING_FORMAT = "[%(levelname)s %(asctime)s %(module)s.%(funcName)s:%(lineno)d]: %(message)s"
    _MAX_STRATEGY_ID_LEN = 32
    _SIZE_PRECISION = 6
    _MAX_DISK_CACHE_SIZE = "100M"  # To ensure that disk_cache uses at most 100 * 10^6 bytes

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
        developer_api_key: Optional[str] = None,
        log_level: LogLevelType = "debug",
        # If this is set, we will be using disk cache for several cachable requests
        disk_cache_location: Optional[str] = None,
        release: Optional[str] = None,
    ) -> None:
        # Those object with _initialized tag have already been initialized,
        # so there is no need to re-initialize anything.
        if hasattr(self, "_initialized") and self._initialized:
            return

        if disk_cache_location:
            verbosity_level = 100 if log_level == "debug" else 0
            self._disk_cache = Memory(location=disk_cache_location, verbose=verbosity_level)
            self._disk_cache.reduce_size(bytes_limit=self._MAX_DISK_CACHE_SIZE)

        """Initializes the TradeLocker API client."""
        self._base_url: str = f"{environment}/backend-api"
        self._credentials: Optional[CredentialsType] = None

        self._access_token: str = ""
        self._refresh_token: str = ""
        self.acc_num: int = 0
        self.account_id: int = 0
        self.environment: str = environment

        self.developer_api_key = developer_api_key
        self.release = release or str(__version__)

        if username and password and server:
            self._credentials = {
                "username": username,
                "password": password,
                "server": server,
            }

        self.log = get_logger(__name__, log_level=log_level, format=self._LOGGING_FORMAT)
        setup_utils_logging(self.log)

        self.log.debug(f"Initialized TLAPI with {log_level=} {self._LOGGING_FORMAT=} ")

        if self._credentials:
            self._auth_with_password(
                username=self._credentials["username"],
                password=self._credentials["password"],
                server=self._credentials["server"],
            )
            user_and_server_cache_key = (
                self._credentials["username"] + "|" + self._credentials["server"]
            )
        elif access_token and refresh_token:
            self._auth_with_tokens(access_token, refresh_token)

            token_payload = jwt.decode(access_token, options={"verify_signature": False})
            # token['sub'] is essentially user_id + server
            user_and_server_cache_key = token_payload.get("sub")
        else:
            error_msg = (
                "Either username/pass/server, or access_token/refresh_token must be provided!"
            )
            raise ValueError(error_msg)

        # Cache_key is user + server + environment + (account_id when it becomes available)
        self._cache_key = user_and_server_cache_key + "|" + self.environment + "|" + self.release

        self._set_account_id_and_acc_num(account_id, acc_num)

        # This redefines the cache key to also include the chosen account_id (different for each acc_num)
        self._cache_key = self._cache_key + "|" + str(self.account_id)

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
                raise TLAPIException(error_msg)
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
                raise TLAPIException(error_msg)
        return self._refresh_token

    def _set_account_id_and_acc_num(self, account_id: int, acc_num: int) -> None:
        all_accounts: pd.DataFrame = self.get_all_accounts()

        if all_accounts.empty:
            self.log.critical("No accounts found")
            raise TLAPIException("No accounts found")

        # Pick the correct account, either by having account_id, or acc_num specified
        if account_id != 0:
            if account_id not in all_accounts["id"].values:
                raise ValueError(
                    f"account_id '{account_id}' not found in all_accounts:\n{all_accounts} "
                )

            self.account_id = account_id
            # Find the acc_num for the specified account_id
            self.acc_num = int(all_accounts[all_accounts["id"] == account_id]["accNum"].iloc[0])
            self.account_name = all_accounts[all_accounts["id"] == account_id]["name"].iloc[0]

            self.log.debug(
                f"Logging in using the specified account_id: {account_id}, using acc_num: {self.acc_num}"
            )

        elif acc_num != 0:
            if acc_num not in all_accounts["accNum"].values:
                raise ValueError(f"acc_num '{acc_num}' not found in all_accounts:\n{all_accounts}")

            self.acc_num = acc_num
            # Find the account_id for the specified acc_num
            self.account_id = int(all_accounts[all_accounts["accNum"] == acc_num]["id"].iloc[0])
            self.account_name = all_accounts[all_accounts["accNum"] == acc_num]["name"].iloc[0]

            self.log.debug(
                f"Logging in using the specified acc_num: {acc_num}, using account_id: {self.account_id}"
            )
        else:
            self.log.debug("Neither account_id nor acc_num specified, using the first account")
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
    def get_trade_route_id(self, instrument_id: int) -> str:
        """Returns the "TRADE" route_id for the specified instrument_id"""
        all_trade_routes = self._get_route_ids(instrument_id, "TRADE")
        return all_trade_routes[0]

    @lru_cache
    @log_func
    def _info_route_valid(self, route_id: str, instrument_id: int) -> bool:
        """Checks if the INFO route is valid for the specified instrument_id by making a simple /trade/quotes request"""
        route_url = f"{self.get_base_url()}/trade/quotes"

        additional_params: DictValuesType = {
            "tradableInstrumentId": instrument_id,
            "routeId": route_id,
        }
        try:
            response_json = self._request("get", route_url, additional_params=additional_params)
            assert "s" in response_json and response_json["s"] == "ok"
        except:
            return False

        return True

    @lru_cache
    @log_func
    def get_info_route_id(self, instrument_id: int) -> str:
        """Returns the "INFO" route_id for the specified instrument_id"""
        all_info_routes = self._get_route_ids(instrument_id, "INFO")

        # We avoid the need to check if the route is valid by returning the first one
        # If this one is not OK, everything else will fail anyways
        if len(all_info_routes) == 1:
            return all_info_routes[0]

        # Multiple routes found -- we need to find which one works
        # These checks exist because some users had multiple info routes, and the first one was invalid
        # Testing them in reverse reduces the number of calls to make
        all_info_routes_reverted = all_info_routes[::-1]
        for route_id in all_info_routes_reverted:
            if self._info_route_valid(route_id, instrument_id):
                return route_id

        raise TLAPIException("No valid INFO route found for instrument_id")

    @lru_cache
    def max_price_history_rows(self) -> int:
        config_dict: ConfigType = self.get_config()
        limits: list[LimitsType] = get_nested_key(config_dict, ["limits"], list[LimitsType])
        for limit in limits:
            if limit["limitType"] == "QUOTES_HISTORY_BARS":
                return limit["limit"]
        raise TLAPIException("Failed to fetch max history rows")

    @lru_cache
    def get_route_rate_limit(self, route_name: RouteNamesType) -> RateLimitType:
        config_dict: ConfigType = self.get_config()
        limits: list[LimitsType] = get_nested_key(config_dict, ["rateLimits"], list[RateLimitType])

        for limit in limits:
            if limit["rateLimitType"] == route_name:
                return limit

        raise TLAPIException("Failed to fetch trade rate limit")

    def get_price_history_rate_limit(self) -> RateLimitType:
        return self.get_route_rate_limit("QUOTES_HISTORY")

    @lru_cache
    @log_func
    @tl_typechecked
    def _get_route_ids(self, instrument_id: int, route_type: RouteTypeType) -> list[str]:
        """Returns the route_id for the specified instrument_id and route_type (TRADE/INFO)"""
        all_instruments: pd.DataFrame = self.get_all_instruments()
        matching_instruments: pd.DataFrame = all_instruments[
            all_instruments["tradableInstrumentId"] == instrument_id
        ]
        try:
            routes: list[RouteType] = matching_instruments["routes"].iloc[0]
            # filter routes by type
            matching_routes: list[str] = [
                str(route["id"]) for route in routes if route["type"] == route_type
            ]
            return matching_routes
        except IndexError:
            raise TLAPIException(f"No {route_type} route found for {instrument_id=}")

    @tl_typechecked
    def _get_headers(
        self,
        include_access_token: bool = True,
        include_acc_num: bool = True,
        additional_headers: Optional[RequestsMappingType] = None,
    ) -> RequestsMappingType:
        """Returns a header with a fresh JWT token, additional_headers and (potentially) accNum

        Args:
            additional_headers: Additional headers to include in the request
            include_acc_num: Whether to include the accNum header in the request

        Returns:
            The final headers
        """
        headers: RequestsMappingType = {}

        if include_access_token:
            headers["Authorization"] = f"Bearer {self.get_access_token()}"

        if include_acc_num:
            headers["accNum"] = str(self.acc_num)

        # If available, Developer API key is attached to all requests
        if self.developer_api_key:
            headers["developer-api-key"] = self.developer_api_key

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
        final_params: RequestsMappingType = {"ref": "py_c", "v": self.release}
        if additional_params is not None:
            for key, value in cast(RequestsMappingType, additional_params).items():
                final_params[key] = str(value)

        return final_params

    def _raise_from_response_status(self, response: requests.Response) -> None:
        """Raises an exception if the response status is not OK

        Args:
            response: The response to check
        """
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            error_msg = f"Received response: '{response.text}' from {response.url}: '{err}'"
            self.log.error(error_msg)
            raise requests.exceptions.HTTPError(error_msg)

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
            ValueError: Will be raised if the response is empty or invalid
        """
        self._raise_from_response_status(response)

        if response.text == "":
            raise ValueError(f"Empty response received from the API for {response.url}")

        try:
            response_json: JSONType = response.json()
            return response_json
        except json.decoder.JSONDecodeError as err:
            error_msg = f"Failed to decode JSON response from {response.url}. Received response:\n'{response.text}'\n{err}"
            raise ValueError(error_msg) from err

    @tl_typechecked
    def _request(
        self,
        request_type: Literal["get", "post", "delete", "patch"],
        url: str,
        additional_headers: Optional[RequestsMappingType] = None,
        additional_params: Optional[DictValuesType] = None,
        include_acc_num: bool = True,
        include_access_token: bool = True,
        json_data: Optional[JSONType] = None,
        retry_request: bool = True,
    ) -> JSONType:
        """Performs a request to the specified URL.

        Args:
            request_type: The type of request to perform (post, delete, patch, get)
            url: The URL to send the request to
            additional_headers: Additional headers to include in the request
            additional_params: Additional parameters to include in the request
            include_acc_num: Whether to include the accNum header in the request
            include_access_token: Whether to include the access token in the request
            json_data: The JSON to send in the request body
            retry_request: Whether to retry the request if it fails

        Returns:
            The response JSON

        Raises:
            HTTPError: Will be raised if the request fails
        """

        headers = self._get_headers(
            additional_headers=additional_headers,
            include_acc_num=include_acc_num,
            include_access_token=include_access_token,
        )
        params = self._get_params(additional_params)
        request_method = getattr(requests, request_type)
        kwargs = {
            "url": url,
            "headers": headers,
            "params": params,
            "json": json_data,
            "timeout": self._TIMEOUT,
        }
        self.log.debug(f"=> REST REQUEST: {request_type.upper()} {url} {kwargs}")

        if retry_request:
            response = self._retry_request(request_method, **kwargs)
        else:
            response = request_method(**kwargs)
        response_json = self._get_response_json(response)

        return response_json

    @retry
    def _retry_request(self, method: Callable, *args, **kwargs) -> Any:
        """Retries to execute the given method using a decorator

        This method is used by _request to retry execution of a request. If the
        request raises any RequestException, the request will be re-run.
        """
        return method(*args, **kwargs)

    def _apply_typing(self, df: pd.DataFrame, column_types: dict[str, type]) -> pd.DataFrame:
        """Converts columns of int and float type from str to numeric values.

        Args:
            columns_types (dict[str, type]): The column types to apply

        Returns:
            pd.DataFrame: The DataFrame with the types applied
        """
        for column in df.columns:
            if column not in column_types:
                self.log.error(f"Missing type specification for column {column} in {column_types}")
            else:
                try:
                    # Only convert the ints and floats after replacing "None" values with 0
                    if column_types[column] in [int64, float]:
                        df[column] = df[column].fillna(0).astype(column_types[column])

                except TLAPIException as err:
                    self.log.warning(
                        f"Failed to apply type {column_types[column]} to column {column}: {err}"
                    )

    ############################## PUBLIC UTILS #######################

    @lru_cache
    @log_func
    @tl_typechecked
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
            self.log.warning(f"Multiple instruments found with {symbol_id=}. Using the first one.")

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

        self.log.debug(f"(get_symbol_name_from_instrument_id) instrument_id: {instrument_id}")
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
        route_url = f"{self.get_base_url()}/trade/accounts/{self.account_id}/positions"

        additional_params: DictValuesType = {}
        if instrument_id_filter != 0:
            additional_params["tradableInstrumentId"] = str(instrument_id_filter)

        response_json = self._request("delete", route_url, additional_params=additional_params)
        response_status: str = get_nested_key(response_json, ["s"], str)
        return response_status == "ok"

    @log_func
    @tl_typechecked
    def delete_all_orders_manual(self, instrument_id_filter: int = 0) -> bool:
        """DEPRECATED -- Use delete_all_orders instead -- Deletes all pending orders, one by one.

        If instrument_id is provided, only pending orders in this instrument will be closed

        Args:
            instrument_id_filter (int, optional): The instrument id to use. Defaults to 0.

        Returns:
            bool: True if executed successfully False otherwise
        """
        self.log.warning(
            f"delete_all_orders_manual is deprecated and will be removed in the future. Use delete_all_orders instead."
        )

        orders = self.get_all_orders(history=False, instrument_id_filter=instrument_id_filter)
        # iterate over all rows of the orders dataframe
        for index, row in orders.iterrows():
            order_id = row["id"]
            self.delete_order(order_id)

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
        route_url = f"{self.get_base_url()}/trade/accounts/{self.account_id}/orders"

        additional_params: DictValuesType = {}
        if instrument_id_filter != 0:
            additional_params["tradableInstrumentId"] = str(instrument_id_filter)

        response_json = self._request("delete", route_url, additional_params=additional_params)
        response_status: str = get_nested_key(response_json, ["s"], str)
        return response_status == "ok"

    @log_func
    @tl_typechecked
    def _place_close_position_order(self, position_id: int, quantity: float = 0) -> bool:
        route_url = f"{self.get_base_url()}/trade/positions/{position_id}"

        data = {"qty": str(quantity)}

        response_json = self._request("delete", route_url, json_data=data)
        response_status: str = get_nested_key(response_json, ["s"], str)

        return response_status == "ok"

    @log_func
    @tl_typechecked
    def close_position(
        self, order_id: int = 0, position_id: int = 0, close_quantity: float = 0
    ) -> bool:
        """Places an order to close a position.

        Either the order_id or the position_id needs to be provided. If both values are provided
        then a ValueError will be raised.

        IMPORTANT: Isn't guaranteed to close the position, or close it immediately.
        Will attempt to place an IOC, then GTC closing order, so the execution might be delayed.

        Args:
            order_id (int, optional): The order id. Defaults to 0.
            position_id (int, optional): The position id. Defaults to 0.
            close_quantity (float, optional): If a value larger than 0 is provided the size of the position will be reduced by the given amount. Defaults to 0 (which closes the position completely).

        Returns:
            bool: True if executed successfully False otherwise

        Raises:
            ValueError: Will be raised if no order_id or position_id was provided or both ids were provided
        """
        if order_id == 0 and position_id == 0:
            raise ValueError("Either order_id or position_id must be provided!")
        if order_id != 0 and position_id != 0:
            raise ValueError("Both order_id and position_id provided!")

        if position_id != 0:
            return self._place_close_position_order(
                position_id=position_id, quantity=close_quantity
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

        rejected_matching_orders = matching_orders[matching_orders["status"] == "Rejected"]
        if len(rejected_matching_orders.index) > 0:
            self.log.warning(f"Rejected orders found for {selection_criteria}!")

        # leave only filled orders
        matching_orders = matching_orders[matching_orders["status"] == "Filled"]

        if len(matching_orders.index) == 0:
            self.log.error(f"No matching position found for {selection_criteria}!")
            return False

        # get the total size of found orders and all related positions
        position_sizes: DefaultDict[int, float] = defaultdict(float)
        for _, row in matching_orders.iterrows():
            position_id = int(row["positionId"])
            qty_order = -row["qty"] if row["side"] == "sell" else row["qty"]
            position_sizes[position_id] += qty_order
        for position_id in position_sizes:
            # make sure all position sizes are positive
            position_sizes[position_id] = abs(position_sizes[position_id])

        # check positions found, only one position can be closed at a time
        if len(position_sizes) > 1:
            # This code and execption below is only theoretical. In practice there will never
            # be more than one position available.
            for position_id in position_sizes.keys():
                quantity_to_close: float = position_sizes[position_id]
                self._place_close_position_order(
                    position_id=position_id, quantity=quantity_to_close
                )
            position_ids = ",".join(list(position_sizes.keys()))
            raise TLAPIException(
                f"CRITICAL ERROR: found multiple positions ({position_ids}) for {selection_criteria}! Closing all matching positions. Reach out to TL Support if this happens again."
            )

        # prepare closing details
        position_id: int = list(position_sizes.keys())[0]
        # since the size is calculated using floats make sure that no float artefacts are submitted
        # by rounding the size to a specific precision which cuts of the artefacts
        quantity_to_close: float = round(position_sizes[position_id], self._SIZE_PRECISION)
        if close_quantity:
            quantity_to_close = min(quantity_to_close, close_quantity)
        if quantity_to_close < self._MIN_LOT_SIZE:
            self.log.error(
                f"Quantity to close ({quantity_to_close}) is less than minimum lot size ({self._MIN_LOT_SIZE}). Not executing close_position."
            )
            return False
        # close position
        return self._place_close_position_order(position_id=position_id, quantity=quantity_to_close)

    ############################## AUTH ROUTES ##########################

    @tl_typechecked
    def _auth_with_password(self, username: str, password: str, server: str) -> None:
        """Fetches and sets access tokens for api access.

        Args:
            username (str): Username
            password (str): Password
            server (str): Server name

        Raises:
            ValueError: Will be raised on authentication errors
        """
        route_url = f"{self.get_base_url()}/auth/jwt/token"

        data = {"email": username, "password": password, "server": server}

        try:
            response_json = self._request(
                "post", route_url, json_data=data, include_access_token=False, include_acc_num=False
            )
            self._access_token = get_nested_key(response_json, ["accessToken"], str)
            self._refresh_token = get_nested_key(response_json, ["refreshToken"], str)
            assert self._access_token and self._refresh_token
            self.log.info("Successfully fetched authentication tokens")
        except Exception as err:
            self.log.critical(f"Failed to fetch authentication tokens: {err}")
            # Explicitly re-raise from err
            raise ValueError(f"Failed to fetch authentication tokens: {err}") from err

    @tl_typechecked
    def refresh_access_tokens(self) -> None:
        """Refreshes authentication tokens."""
        route_url = f"{self.get_base_url()}/auth/jwt/refresh"

        data = {"refreshToken": self._refresh_token}

        response_json = self._request(
            "post", route_url, json_data=data, include_access_token=False, include_acc_num=False
        )

        self.log.info("Successfully refreshed authentication tokens")

        self._access_token = get_nested_key(response_json, ["accessToken"], str)
        self._refresh_token = get_nested_key(response_json, ["refreshToken"], str)

    @disk_or_memory_cache(cache_validation_callback=expires_after(days=1))
    @log_func
    @tl_typechecked
    def get_all_accounts(self) -> pd.DataFrame:
        """Returns all accounts associated with the account used for authentication.

        Raises:
            TLAPIException: Will be raised if account information could not be fetched

        Returns:
            pd.DataFrame[AccountsColumnsTypes]: DataFrame with user's accounts
        """
        route_url = f"{self.get_base_url()}/auth/jwt/all-accounts"

        # Make sure we don't try including accNum into the header, as it is not chosen yet
        response_json = self._request("get", route_url, include_acc_num=False)
        accounts_json = get_nested_key(response_json, ["accounts"])

        accounts = pd.DataFrame(accounts_json)
        self._apply_typing(accounts, AccountsColumns)

        if not accounts_json or accounts.empty:
            self.log.critical("Failed to fetch user's accounts")
            raise TLAPIException("Failed to fetch user's accounts")

        return accounts

    ############################## CONFIG ROUTES ##########################

    @disk_or_memory_cache(cache_validation_callback=expires_after(days=1))
    @log_func
    @tl_typechecked
    def get_config(self) -> ConfigType:
        """Returns the user's configuration.

        Route Name: GET_CONFIG

        Returns:
            ConfigType: The configuration
        """
        route_url = f"{self.get_base_url()}/trade/config"
        response_json = self._request("get", route_url)
        config_dict: ConfigType = get_nested_key(response_json, ["d"], ConfigType)
        return config_dict

    ############################## ACCOUNT ROUTES ##########################

    @log_func
    @tl_typechecked
    def get_trade_accounts(self) -> TradeAccountsType:
        """Returns the account information.

        The account is defined by the acc_num used in constructor.

        Route Name: GET_ACCOUNTS

        Returns:
            TradeAccountsType: The account details
        """
        route_url = f"{self.get_base_url()}/trade/accounts"

        response_json = self._request("get", route_url)

        trade_accounts: TradeAccountsType = get_nested_key(response_json, ["d"], TradeAccountsType)
        return trade_accounts

    @log_func
    @tl_typechecked
    def get_all_executions(self) -> pd.DataFrame:
        """Returns a list of orders executed in account in current session.

        Route Name: GET_EXECUTIONS

        Returns:
            pd.DataFrame[ExecutionsColumnTypes]: DataFrame containing all executed orders
        """
        route_url = f"{self.get_base_url()}/trade/accounts/{self.account_id}/executions"

        response_json = self._request("get", route_url)

        column_names = self._get_column_names("filledOrdersConfig")

        all_executions = pd.DataFrame(
            get_nested_key(response_json, ["d", "executions"]), columns=column_names
        )
        self._apply_typing(all_executions, ExecutionsColumns)

        return all_executions

    @disk_or_memory_cache(cache_validation_callback=expires_after(days=1))
    @log_func
    @tl_typechecked
    def get_all_instruments(self) -> pd.DataFrame:
        """Returns all available instruments for account.

        route_name = GET_INSTRUMENTS

        Returns:
            pd.DataFrame[InstrumentsColumnsTypes]: DataFrame with all available instruments
        """
        route_url = f"{self.get_base_url()}/trade/accounts/{self.account_id}/instruments"

        response_json = self._request("get", route_url)

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

        Route Name: GET_ORDERS
        Route Name: GET_ORDERS_HISTORY

        Args:
            lookback_period (str, optional): This will set the start and end timestamp based on the
                given lookback period. The lookback_period needs to be in
                the format of 1Y, 1M, 1D, 1H, 1m, 1s. Defaults to "".
            start_timestamp (int, optional): Minimal timestamp of returned orders. Defaults to 0.
            end_timestamp (int, optional): Maximal timestamp of returned orders. Defaults to 0.
            instrument_id_filter (int, optional): Filter for instrument id, returns only orders that
                use the given instrument. Defaults to 0.
            history (bool, optional): Should historical orders be returned. Defaults to False.

        Returns:
            pd.DataFrame[OrdersColumnsTypes]: DataFrame containing all orders
        """
        endpoint = "orders" + ("History" if history else "")
        route_url = f"{self.get_base_url()}/trade/accounts/{self.account_id}/{endpoint}"

        if lookback_period != "":
            start_timestamp, end_timestamp = resolve_lookback_and_timestamps(
                lookback_period=lookback_period,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )

        additional_params: DictValuesType = {}
        if instrument_id_filter != 0:
            additional_params["tradableInstrumentId"] = instrument_id_filter
        if start_timestamp != 0:
            additional_params["from"] = start_timestamp
        if end_timestamp != 0:
            additional_params["to"] = end_timestamp

        response_json = self._request("get", route_url, additional_params=additional_params)
        all_orders_raw = get_nested_key(response_json, ["d", endpoint])

        column_names = self._get_column_names(endpoint + "Config")
        all_orders = pd.DataFrame(all_orders_raw, columns=column_names)
        self._apply_typing(all_orders, OrdersColumns)

        return all_orders

    @log_func
    @tl_typechecked
    def get_all_positions(self) -> pd.DataFrame:
        """Returns all open positions for account.

        Route Name: GET_POSITIONS

        Returns:
            pd.DataFrame[PositionsColumnsTypes]: DataFrame containing all positions
        """
        route_url = f"{self.get_base_url()}/trade/accounts/{self.account_id}/positions"

        response_json = self._request("get", route_url)
        all_positions_raw = get_nested_key(response_json, ["d", "positions"])

        all_positions_columns = self._get_column_names("positionsConfig")
        all_positions = pd.DataFrame(all_positions_raw, columns=all_positions_columns)
        self._apply_typing(all_positions, PositionsColumns)

        return all_positions

    @log_func
    @tl_typechecked
    def get_account_state(self) -> DictValuesType:
        """Returns the account state.

        Route Name: GET_ACCOUNT_STATE

        Returns:
            DictValuesType: The account state
        """
        route_url = f"{self.get_base_url()}/trade/accounts/{self.account_id}/state"

        response_json = self._request("get", route_url)
        account_state_values = get_nested_key(response_json, ["d", "accountDetailsData"])
        account_state = dict(
            zip(self._get_column_names("accountDetailsConfig"), account_state_values)
        )
        return account_state

    ############################## INSTRUMENT ROUTES #######################

    @disk_or_memory_cache(cache_validation_callback=expires_after(days=1))
    @log_func
    @tl_typechecked
    def get_instrument_details(
        self, instrument_id: int, locale: LocaleType = "en"
    ) -> InstrumentDetailsType:
        """Returns instrument details for a given instrument Id.

        Route Name: GET_INSTRUMENT_DETAILS

        Args:
            instrument_id (int): The instrument Id
            locale (LocaleType, optional): Locale (language) id. Defaults to "en".

        Returns:
            InstrumentDetailsType: The instrument details
        """
        route_url = f"{self.get_base_url()}/trade/instruments/{instrument_id}"

        additional_params: DictValuesType = {
            "routeId": self.get_info_route_id(instrument_id),
            "locale": locale,
        }

        response_json = self._request("get", route_url, additional_params=additional_params)
        instrument_details: InstrumentDetailsType = get_nested_key(
            response_json, ["d"], InstrumentDetailsType
        )
        return instrument_details

    @log_func
    @tl_typechecked
    def get_session_details(self, session_id: int) -> SessionDetailsType:
        """Returns details about the session defined by session_id.

        Route Name: GET_SESSION_DETAILS

        Args:
            session_id (int): Session id

        Returns:
            SessionDetailsType: Session details
        """
        route_url = f"{self.get_base_url()}/trade/sessions/{session_id}"

        response_json = self._request("get", route_url)
        session_details: SessionDetailsType = get_nested_key(
            response_json, ["d"], SessionDetailsType
        )
        return session_details

    @log_func
    @tl_typechecked
    def get_session_status_details(self, session_status_id: int) -> SessionStatusDetailsType:
        """Returns details about the session status.

        Route Name: GET_SESSION_STATUSES

        Args:
            session_status_id (int): Session id

        Returns:
            SessionStatusDetailsType: Session details
        """
        route_url = f"{self.get_base_url()}/trade/sessionStatuses/{session_status_id}"

        response_json = self._request("get", route_url)
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

        Route Name: DAILY_BAR

        Args:
            instrument_id (int): Instrument Id
            bar_type (Literal[BID, ASK, TRADE], optional): The type of candle data to return. Defaults to "ASK".

        Returns:
            DailyBarType: Daily candle data
        """
        route_url = f"{self.get_base_url()}/trade/dailyBar"
        additional_params: DictValuesType = {
            "tradableInstrumentId": instrument_id,
            "routeId": self.get_info_route_id(instrument_id),
            "barType": bar_type,
        }
        response_json = self._request("get", route_url, additional_params=additional_params)
        daily_bar: DailyBarType = get_nested_key(response_json, ["d"], DailyBarType)
        return daily_bar

    # Returns asks and bids
    @log_func
    @tl_typechecked
    def get_market_depth(self, instrument_id: int) -> MarketDepthlistType:
        """Returns market depth information for the requested instrument.

        Route Name: DEPTH

        Args:
            instrument_id (int): Instrument Id

        Returns:
            MarketDepthlistType: Market depth data
        """
        route_url = f"{self.get_base_url()}/trade/depth"

        additional_params: DictValuesType = {
            "tradableInstrumentId": instrument_id,
            "routeId": self.get_info_route_id(instrument_id),
        }
        response_json = self._request("get", route_url, additional_params=additional_params)
        market_depth: MarketDepthlistType = get_nested_key(
            response_json, ["d"], MarketDepthlistType
        )
        return market_depth

    @disk_or_memory_cache()
    @log_func
    @tl_typechecked
    def _request_history_cacheable(
        self, instrument_id: int, route_id: str, resolution: ResolutionType, _from: int, to: int
    ) -> JSONType:
        """Performs a (cacheable) request to the specified URL and handles the response.

        The get_price_history is not cacheable based on its parameters due to the lookback_period and end_timestamp,
           which can cause the same function params to require a different answer.
        This is a helper function for get_price_history, which does not have these params and is thus cacheable.
        """
        route_url = f"{self.get_base_url()}/trade/history"

        additional_params = {
            "tradableInstrumentId": instrument_id,
            "routeId": route_id,
            "resolution": resolution,
            "from": _from,
            "to": to,
        }

        response_json = self._request("get", route_url, additional_params=additional_params)
        return response_json

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

        Route Name: QUOTES_HISTORY

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
        route_url = f"{self.get_base_url()}/trade/history"

        start_timestamp, end_timestamp = resolve_lookback_and_timestamps(
            lookback_period, start_timestamp, end_timestamp
        )

        history_size = estimate_history_size(start_timestamp, end_timestamp, resolution)
        if history_size > self.max_price_history_rows():
            raise ValueError(
                f"No. of requested rows ({history_size}) larger than max allowed ({self.max_price_history_rows()})."
                "Try splitting your request in smaller chunks."
            )

        response_json = self._request_history_cacheable(
            instrument_id=instrument_id,
            route_id=self.get_info_route_id(instrument_id),
            resolution=resolution,
            _from=start_timestamp,
            to=end_timestamp,
        )

        try:
            bar_details = pd.DataFrame(get_nested_key(response_json, ["d", "barDetails"]))
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
        """Returns latest asking price for requested instrument.

        Args:
            instrument_id (int): Instrument Id

        Returns:
            float: Latest asking price of the instrument
        """
        current_quotes: dict[str, float] = cast(dict[str, float], self.get_quotes(instrument_id))
        current_ap: float = get_nested_key(current_quotes, ["ap"], float)
        return current_ap

    @log_func
    @tl_typechecked
    def get_latest_bid_price(self, instrument_id: int) -> float:
        """Returns latest bid price for requested instrument.

        Args:
            instrument_id (int): Instrument Id

        Returns:
            float: Latest bid price of the instrument
        """
        current_quotes: dict[str, float] = cast(dict[str, float], self.get_quotes(instrument_id))
        current_bp: float = get_nested_key(current_quotes, ["bp"], float)
        return current_bp

    @log_func
    @tl_typechecked
    def get_quotes(self, instrument_id: int) -> QuotesType:
        """Returns price quotes for requested instrument.

        Route Name: QUOTES

        Args:
            instrument_id (int): Instrument Id

        Returns:
            QuotesType: Price quotes for instrument
        """
        route_url = f"{self.get_base_url()}/trade/quotes"

        additional_params: DictValuesType = {
            "tradableInstrumentId": instrument_id,
            "routeId": self.get_info_route_id(instrument_id),
        }
        response_json = self._request("get", route_url, additional_params=additional_params)
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
                self.close_position(position_id=position["id"], close_quantity=quantity_to_close)
                total_netted += quantity_to_close

                # If sufficient orders have been placed, return
                if abs(total_netted - float(quantity)) < self._EPS:
                    self.log.debug("New position completely netted from opposite positions.")
                    break

        return total_netted

    # TODO(2): add tests for sl/tp
    @log_func
    @tl_typechecked
    def create_order(
        self,
        instrument_id: int,
        quantity: float,
        side: SideType,
        price: Optional[float] = None,
        type_: OrderTypeType = "market",
        validity: Optional[ValidityType] = None,
        position_netting: bool = False,
        take_profit: Optional[float] = None,
        take_profit_type: Optional[TakeProfitType] = None,
        stop_loss: Optional[float] = None,
        stop_loss_type: Optional[StopLossType] = None,
        stop_price: Optional[float] = None,
        strategy_id: Optional[str] = None,
        _ignore_len_check: bool = False,  # Temporary value that allows us to better test the function
    ) -> Optional[int]:
        """Creates an order.

        Route Name: PLACE_ORDER

        Args:
            instrument_id (int): Instrument Id
            quantity (float): Order size
            side (SideType): Order side
            price (float, optional): Price for non-market orders. Defaults to 0.
            type_ (OrderTypeType, optional): Order type. Defaults to "market".
            validity (ValidityType, optional): Validity type of order. Defaults to "IOC".
            position_netting (bool, optional): Should position netting be used. Defaults to False.
            take_profit (float, optional): Take profit value. Defaults to None.
            take_profit_type (_TakeProfitType, optional): Take profit type. Defaults to None.
            stop_loss (float, optional): Stop loss value. Defaults to None.
            stop_loss_type (_StopLossType, optional): Stop loss type. Defaults to None.

        Returns:
            Optional[int]: Order Id if order created, otherwise None

        Raises:
            ValueError: Will be raised if any of the parameters are invalid
            TLAPIException: Will be raised if the request failed or no valid json received.
            TLAPIOrderException: Will be raised if broker rejected the order.
        """
        route_url = f"{self.get_base_url()}/trade/accounts/{self.account_id}/orders"

        if type_ == "market" and price:
            self.log.warning("Price specified for a market order. Ignoring the price.")
            price = None

        if type_ == "market":
            if validity and validity != "IOC":
                error_msg = f"Market orders must use IOC as validity. Not placing the order."
                self.log.error(error_msg)
                raise ValueError(error_msg)
            else:
                validity = "IOC"
        elif not validity:
            error_msg = (
                "Validity not specified for a non-market order. You must specify validity='GTC'"
            )
            raise ValueError(error_msg)
        elif validity != "GTC":
            error_msg = f"{type_} orders must use GTC as validity. Not placing the order."
            self.log.error(error_msg)
            raise ValueError(error_msg)

        if stop_loss and not stop_loss_type:
            error_msg = "Stop loss value specified, but no stop_loss_type specified. Please set stop_loss_type to 'absolute' or 'offset'"
            self.log.error(error_msg)
            raise ValueError(error_msg)

        if take_profit and not take_profit_type:
            error_msg = "Take profit value specified, but no take_profit_type specified. Please set take_profit_type to 'absolute' or 'offset'"
            self.log.error(error_msg)
            raise ValueError(error_msg)

        if type_ == "stop" and stop_price is None:
            if not price:
                error_msg = "Stop orders must have a stop price set. Not placing the order."
            else:
                error_msg = f"Order of {type_ = } specified with a price, instead of stop_price. Please set the stop_price instead"

            self.log.error(error_msg)
            raise ValueError(error_msg)

        if not _ignore_len_check and strategy_id and len(strategy_id) > self._MAX_STRATEGY_ID_LEN:
            error_msg = (
                f"Strategy ID {strategy_id} is too long. Max length is {self._MAX_STRATEGY_ID_LEN}"
            )
            self.log.error(error_msg)
            raise ValueError(error_msg)

        request_body: dict[str, Any] = {
            "price": price,
            "qty": str(quantity),
            "routeId": self.get_trade_route_id(instrument_id),
            "side": side,
            "validity": validity,
            "tradableInstrumentId": str(instrument_id),
            "type": type_,
            "takeProfit": take_profit,
            "takeProfitType": take_profit_type,
            "stopLoss": stop_loss,
            "stopLossType": stop_loss_type,
            "stopPrice": stop_price,
            "strategyId": strategy_id,
        }

        if position_netting:
            self.log.warning(
                "Position netting support is deprecated and will be removed after January 1st 2025. Please stop using it by that date."
            )
            # Try finding opposite orders to net against
            if type_ == "market":
                total_netted = self._perform_order_netting(instrument_id, side, quantity)
                # Reduce the necessary quantity by the total_amount that was netted
                request_body["qty"] = str(float(request_body["qty"]) - total_netted)
                if float(request_body["qty"]) < self._MIN_LOT_SIZE:
                    self.log.info(
                        "Not placing a new order after closing sufficient opposite orders due to netting."
                    )
                    return None
            else:
                error_msg = (
                    "Order netting is only supported for market orders. Continuing without netting."
                )
                self.log.error(error_msg)
                raise ValueError(error_msg)

        try:
            # Place the order
            response_json: JSONType = self._request("post", route_url, json_data=request_body)
            order_id: int = int(get_nested_key(response_json, ["d", "orderId"], str))
            self.log.info(f"Order {request_body} placed with order_id: {order_id}")
            return order_id
        except (HTTPError, ValueError) as err:
            # HTTPError will be raised if a non-200 response or any request related issues
            # occur. In that case the response_json will not be available since the excetion happens
            # in _request. A ValueError will be raised if the response received is invalid json.
            raise TLAPIException(f"Request failed {err} with {request_body}") from err
        except KeyError as err:
            raise TLAPIOrderException(request_body, response_json) from err

    @log_func
    @tl_typechecked
    def delete_order(self, order_id: int) -> bool:
        """Deletes a pending order.


        Args:
            order_id (int): Order Id

        Returns:
            bool: True on success, False on error
        """
        route_url = f"{self.get_base_url()}/trade/orders/{order_id}"

        self.log.info(f"Deleting order with id {order_id}")

        response_json = self._request("delete", url=route_url)
        self.log.info(f"Order deletion response: {response_json}")
        response_status: str = get_nested_key(response_json, ["s"], str)

        return response_status == "ok"

    @log_func
    @tl_typechecked
    def modify_order(self, order_id: int, modification_params: ModificationParamsType) -> bool:
        """Modifies a pending order -- a thin wrapper around PATCH /trade/orders/{order_id}.

        Route Name: MODIFY_ORDER

        Args:
            order_id (int): Order Id
            modification_params (ModificationParamsType): Order modification details

        Returns:
            bool: True on success, False on error
        """
        route_url = f"{self.get_base_url()}/trade/orders/{order_id}"

        self.log.info(f"Modifying the order with id {order_id}")

        response_json = self._request("patch", route_url, json_data=modification_params)
        response_status: str = get_nested_key(response_json, ["s"], str)
        return response_status == "ok"

    @log_func
    @tl_typechecked
    def modify_position(
        self, position_id: int, modification_params: ModificationParamsType
    ) -> bool:
        """Modifies an open position.

        Route Name: MODIFY_POSITION

        Args:
            position_id (int): Position Id
            modification_params (_ModificationParamsType): Position modification details

        Returns:
            bool: True on success, False on error
        """
        route_url = f"{self.get_base_url()}/trade/positions/{position_id}"

        self.log.info(f"Modifying the position with id {position_id}")

        response_json = self._request("patch", route_url, json_data=modification_params)
        response_status: str = get_nested_key(response_json, ["s"], str)
        return response_status == "ok"

    @log_func
    @tl_typechecked
    def get_position_id_from_order_id(self, order_id: int) -> Optional[int]:
        """Retrieves position_id from the given order_id (if one exists).

        Args:
            order_id (int): An order id

        Returns:
            Optional[int]: position_id or None
        """
        self.log.info(f"Getting execution id from orders history")
        orders_history = self.get_all_orders(history=True)

        matching_orders = orders_history[orders_history["id"] == order_id]
        if len(matching_orders) == 0:
            self.log.info(f"No matching order found for order_id: {order_id}")
            return None

        position_id = int(matching_orders["positionId"].iloc[0])
        return position_id
