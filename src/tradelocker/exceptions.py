from typing import Any
from tradelocker.tradelocker_api import JSONType


class TLAPIException(Exception):
    """Generic API exception"""


class TLAPIOrderException(TLAPIException):
    """Exception for order-related errors"""

    def __init__(self, request_body: dict[str, Any], response_json: JSONType) -> None:
        self.request_body: dict = request_body
        self.response_json: JSONType = response_json

    def __str__(self) -> str:
        # return the errmsg which contains a reason for a rejection
        if "errmsg" in self.response_json and self.response_json["errmsg"]:
            return self.response_json["errmsg"]
        # otherwise, use the whole response
        return f"Response: {self.response_json}"
