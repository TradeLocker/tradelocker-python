from typing import TypeAlias as TA, Literal, Optional
from numpy import int64

# Custom type aliases
RequestsMappingType: TA = dict[str, str | bytes]
StopLossType: TA = Literal["absolute", "offset", "trailingOffset"]
TakeProfitType: TA = Literal["absolute", "offset"]
ValidityType: TA = Literal["GTC", "IOC"]
MarketDepthlistType: TA = dict[Literal["asks", "bids"], list[list[float]]]
OrderTypeType: TA = Literal["limit", "market", "stop"]
TradingRulesType: TA = dict[str, bool | int]
RiskRulesType: TA = dict[str, Optional[int | float]]
TradeAccountsType: TA = list[dict[str, str | TradingRulesType | RiskRulesType]]
DailyBarType: TA = dict[Literal["c", "h", "l", "o", "v"], float]
SessionType: TA = dict[str, str | list[dict[str, str]]]
SideType: TA = Literal["buy", "sell"]
SessionHolidayType: TA = dict[str, Optional[str | None]]
JSONType: TA = dict[str, object]
SessionDetailsType: TA = dict[
    str, str | bool | None | list[SessionHolidayType] | SessionType
]
SessionStatusDetailsType: TA = dict[
    Literal["allowedOperations", "allowedOrderTypes"], list[Literal[0, 1]]
]
InstrumentDetailsType: TA = dict[str, str | int | float | list[dict[str, int | float]]]
ColumnConfigKeysType: TA = Literal[
    "positionsConfig",
    "ordersConfig",
    "ordersHistoryConfig",
    "filledOrdersConfig",
    "accountDetailsConfig",
]
ConfigColumnType: TA = list[dict[Literal["id", "description"], str]]
ColumnConfigValuesType: TA = (
    dict[Literal["id", "title"], str] | dict[Literal["columns"], ConfigColumnType]
)

LimitsType: TA = dict[Literal["limitType", "limit"], str | int | float]
RateLimitsType: TA = dict[
    Literal["rateLimitType", "measure", "intervalNum", "limit"],
    str | int | float,
]

ConfigType: TA = (
    dict[Literal["customerAccess"], dict[str, bool]]
    | dict[ColumnConfigKeysType, ColumnConfigValuesType]
    | dict[Literal["limits"], list[LimitsType]]
    | dict[
        Literal["rateLimits"],
        list[RateLimitsType],
    ]
)

ResolutionType: TA = Literal["1M", "1W", "1D", "4H", "1H", "30m", "15m", "5m", "1m"]
ModificationParamsType: TA = dict[
    str, str | StopLossType | TakeProfitType | ValidityType
]
LocaleType: TA = Literal[
    "ar", "en", "es", "fr", "ja", "ko", "pl", "pt", "ru", "tr", "ua", "zh_sm", "zh_tr"
]
EnvironmentsType: TA = Literal["demo", "live"]
DevEnvironmentsType: TA = Literal["dev", "stg", "exp"]  # For internal use
RouteType: TA = dict[Literal["id", "type"], int | str]
RouteTypeType: TA = Literal["INFO", "TRADE"]
LogLevelType: TA = Literal["debug", "info", "warning", "error", "critical"]
DictValuesType: TA = dict[str, str | float | int]
CredentialsType: TA = dict[Literal["username", "password", "server"], str]
QuotesKeyType: TA = Literal["ap", "bp", "as", "bs"]
QuotesType: TA = dict[QuotesKeyType, float]


AccountsColumns: dict[str, type] = {
    "id": int64,
    "name": str,
    "currency": str,
    "accNum": int64,
    "accountBalance": float,
}

ExecutionsColumns: dict[str, type] = {
    "id": int64,
    "price": float,
    "side": SideType,
    "createdDate": int64,
    "qty": float,
    "orderId": int64,
    "positionId": int64,
}

OrdersColumns: dict[str, type] = {
    "id": int64,
    "tradableInstrumentId": int64,
    "routeId": int64,
    "qty": float,
    "side": SideType,
    "type": OrderTypeType,
    "status": str,
    "filledQty": float,
    "avgPrice": float,
    "price": float,
    "stopPrice": float,
    "validity": ValidityType,
    "expireDate": int64,
    "createdDate": int64,
    "lastModified": int64,
    "isOpen": bool,
    "positionId": int64,
    "stopLoss": float,
    "stopLossType": StopLossType,
    "takeProfit": float,
    "takeProfitType": TakeProfitType,
}

PositionsColumns: dict[str, type] = {
    "id": int64,
    "tradableInstrumentId": int64,
    "routeId": int64,
    "side": SideType,
    "qty": float,
    "avgPrice": float,
    "stopLossId": int64,
    "takeProfitId": int64,
    "openDate": int64,
    "unrealizedPl": float,
}

PriceHistoryColumns: dict[str, type] = {
    "t": int64,
    "o": float,
    "h": float,
    "l": float,
    "c": float,
    "v": float,
}

InstrumentsColumns: dict[str, type] = {
    "tradableInstrumentId": int64,
    "id": int64,
    "name": str,
    "description": str,
    "type": str,
    "tradingExchange": str,
    "marketDataExchange": str,
    "country": str,
    "logoUrl": str,
    "localizedName": str,
    "routes": list[RouteType],
    "barSource": str,
    "hasIntraday": bool,
    "hasDaily": bool,
}

AccountDetailsColumns: dict[str, type] = {
    "balance": float,
    "projectedBalance": float,
    "availableFunds": float,
    "blockedBalance": float,
    "cashBalance": float,
    "unsettledCash": float,
    "withdrawalAvailable": float,
    "stocksValue": float,
    "optionValue": float,
    "initialMarginReq": float,
    "maintMarginReq": float,
    "marginWarningLevel": float,
    "blockedForStocks": float,
    "stockOrdersReq": float,
    "stopOutLevel": float,
    "warningMarginReq": float,
    "marginBeforeWarning": float,
    "todayGross": float,
    "todayNet": float,
    "todayFees": float,
    "todayVolume": float,
    "todayTradesCount": int64,
    "openGrossPnL": float,
    "openNetPnL": float,
    "positionsCount": int64,
    "ordersCount": int64,
}

order_history_statuses = ["Filled", "Cancelled", "Refused", "Unplaced", "Removed"]
