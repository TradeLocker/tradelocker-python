from functools import wraps
from typing import Any, Callable, Optional, Tuple, TypeVar, cast
import datetime
import logging as logging_module
import time
import os

from dotenv import dotenv_values
import jwt

from tradelocker.types import ResolutionType, LogLevelType

# This will allow us to keep track of the return type of the functions
# being decorated.
RT = TypeVar("RT")  # Return Type

# ----------- Import conditional dependencies ---------------
try:
    # Try importing typechecked from typeguard
    logging_module.info("typechecked imported from typeguard")
    from typeguard import typechecked as tl_typechecked

except ImportError:
    logging_module.info("typechecked defined as a noop decatorator")

    # If it fails, define a noop decorator
    def tl_typechecked(func: Callable[..., RT]) -> Callable[..., RT]:
        return func


try:
    from typeguard import check_type as tl_check_type
except ImportError:
    # Define a noop check_type function
    def tl_check_type(arg: Any, arg_type: Any) -> None:
        pass


# ------------------------------------------------------------

# Constants
MS_COEFF = 1000
RESOLUTION_COEFF_MS = {
    "s": 1 * MS_COEFF,
    "m": 60 * MS_COEFF,
    "H": 60 * 60 * MS_COEFF,
    "D": 24 * 60 * 60 * MS_COEFF,
    "W": 7 * 24 * 60 * 60 * MS_COEFF,
    "M": 30 * 24 * 60 * 60 * MS_COEFF,
    "Y": 365 * 24 * 60 * 60 * MS_COEFF,
}

# Default logging if setup_utils_logging was not called
logging = logging_module


# Overwrites the logging module
def setup_utils_logging(logger: logging_module.Logger) -> None:
    global logging
    logging = logger


def get_logger(name: str, log_level: LogLevelType, format: str) -> logging_module.Logger:
    """Returns a logger with the specified name, log_level and format."""
    logger = logging_module.getLogger(name)
    logger.setLevel(log_level.upper())
    # NOTE: this is needed to avoid duplicate logs for some reason?!
    # https://stackoverflow.com/questions/6729268/log-messages-appearing-twice-with-python-logging
    logger.propagate = False
    # add handler only once
    if len(logger.handlers) == 0:
        handler = logging_module.StreamHandler()
        handler.setFormatter(logging_module.Formatter(format))
        logger.addHandler(handler)
    return logger


# This decorator logs the function call and its arguments
def log_func(func: Callable[..., RT]) -> Callable[..., RT]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> RT:
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)

        logging.debug(f"**** CALLING {func.__name__}({signature})")

        return_value = func(*args, **kwargs)

        max_return_string_length = 1000
        return_string = repr(return_value)
        if len(return_string) > max_return_string_length:
            return_string = (
                return_string[:max_return_string_length]
                + "    ...   ===<< TRUNCATED DUE TO LENGTH >>===   "
            )
        logging.debug(f"**** RETURN from {func.__name__}({signature}):\n{return_string}")

        return return_value

    return cast(Callable[..., RT], wrapper)


def retry(func: Callable[..., RT], delay: float = 1) -> Callable[..., RT]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> RT:
        last_err: Exception = Exception()
        max_retries = 3
        for attempt in range(max_retries):
            time.sleep(delay)  # Must be below delay limit
            try:
                return func(*args, **kwargs)
            except Exception as err:
                logging.warning(f"Retry #{attempt}, Error: {err}, retrying...")
                last_err = err

        raise Exception(f"Received error: {last_err}, too many times. Exiting...")

    return cast(Callable[..., RT], wrapper)


# Returns the value of a nested key in a JSON object
@tl_typechecked
def get_nested_key(
    json_data: dict[str, Any], keys: list[str], return_type_assertion: Any = None
) -> Any:
    current_data: Any = json_data
    for key in keys:
        if key not in current_data:
            logging.error(f"Key {key} ({keys}) missing from JSON data {str(json_data)}")
            raise KeyError(f"Key {key} ({keys}) missing from JSON data {str(json_data)}")

        current_data = current_data[key]

    if return_type_assertion:
        # check whether current_data is of type return_type_assertion
        tl_check_type(current_data, return_type_assertion)

    return current_data


@tl_typechecked
def timestamps_from_lookback(lookback_period: str) -> Tuple[int, int]:
    assert (
        len(lookback_period) > 1
    ), f"lookback_period ({lookback_period}) must be at least 2 characters long"

    lookback_period_num = int(lookback_period[:-1])

    if lookback_period[-1] not in RESOLUTION_COEFF_MS:
        raise ValueError(
            f"last character ({lookback_period[-1]}) not among {RESOLUTION_COEFF_MS.keys()}"
        )

    end_timestamp = int(datetime.datetime.now().timestamp() * MS_COEFF)
    # Depending on the lookback_period, we need to calculate the start_timestamp
    start_timestamp = end_timestamp - lookback_period_num * RESOLUTION_COEFF_MS[lookback_period[-1]]

    logging.debug(f"start_timestamp: {start_timestamp}")
    logging.debug(f"end_timestamp: {end_timestamp}")

    return start_timestamp, end_timestamp


def convert_resolution_to_mins(resolution: ResolutionType) -> int:
    # if last character is "m", then it is minutes, "H" is for hours, "D" for days, W weeks, M monthts

    logging.debug(f"Converting {resolution} to minutes")

    if resolution[-1] in RESOLUTION_COEFF_MS:
        val = int(resolution[:-1])
        val_ms = val * RESOLUTION_COEFF_MS[resolution[-1]]
        if val_ms < 60 * 1000:
            raise ValueError(f"Resolution {resolution} is too small. Minimum is 1 minute.")
        return_value = val_ms // (60 * 1000)
        logging.debug(f"RETURNING {return_value}")
        return return_value

    raise ValueError(f"last character of {resolution[-1]} not among {RESOLUTION_COEFF_MS.keys()}")


@tl_typechecked
def resolve_lookback_and_timestamps(
    lookback_period: str, start_timestamp: int, end_timestamp: int
) -> Tuple[int, int]:
    """This assumes that either lookback_period or start timestamp is provided.
    lookback_period needs to be in the format of 1Y, 1M, 1D, 1H, 1m, 1s, where M = 30 days and Y = 365 days
    """

    # If end_timestamp is 0, we can assume that we want to get data until now
    if end_timestamp == 0:
        end_timestamp = int(datetime.datetime.now().timestamp() * MS_COEFF)

    if lookback_period == "" and (start_timestamp == 0 or start_timestamp > end_timestamp):
        raise ValueError(
            "Neither lookback_period nor valid start_timestamp/end_timestamp provided."
        )

    if start_timestamp != 0 and end_timestamp != 0 and start_timestamp <= end_timestamp:
        return start_timestamp, end_timestamp

    try:
        start_timestamp, end_timestamp = timestamps_from_lookback(lookback_period)
        logging.warning(
            "Both valid lookback_period and start_timestamp/end_timestamp were provided.\n"
            "Continuing with only the start_timestamp/end_timestamp"
        )
    except Exception as err:
        logging.warning(
            f"Invalid lookback_period provided: {err}\nContinuing with only the start_timestamp/end_timestamp"
        )

    return start_timestamp, end_timestamp


@tl_typechecked
def estimate_history_size(
    start_timestamp: int, end_timestamp: int, resolution: ResolutionType
) -> int:
    total_miliseconds: float = end_timestamp - start_timestamp
    coeff = int(resolution[:-1]) * RESOLUTION_COEFF_MS[resolution[-1]]
    total_bars: int = int(total_miliseconds / coeff)
    return total_bars


@tl_typechecked
def time_to_token_expiry(access_token: str) -> float:
    if not access_token:
        logging.warning(f"invalid access token: |{access_token}|")
        return 0

    # No explicit need to verify the signature as there is a direct https connection between the client and the server
    decoded_payload: dict[str, Any] = jwt.decode(access_token, options={"verify_signature": False})
    expiration_time: float = decoded_payload["exp"]
    remaining_time: float = expiration_time - datetime.datetime.now().timestamp()
    return remaining_time


@tl_typechecked
# Should be called with callers_file = __file__
def load_env_config(callers_file: str, backup_env_file=".env") -> dict[str, str | int]:
    """Load the .env file from the path defined in ENV_FILE_PATH or the backup_env_file, relative to current dir"""

    # Get the current script's directory
    script_dir = os.path.abspath(os.path.dirname(callers_file))

    env_var_name = "ENV_FILE_PATH"

    # read the "$(env_var_name)" environment variable if it exists, otherwise use .env or .env-test
    env_path = os.environ.get(env_var_name, os.path.join(script_dir, backup_env_file))

    # Load the .env file from that directory
    config: dict[str, str] = dotenv_values(env_path)
    if "tl_acc_num" not in config:
        config["tl_acc_num"] = 0

    return config


@tl_typechecked
def is_more_frequent(resolution1: ResolutionType, resolution2: ResolutionType) -> bool:
    all_resolutions = ["1m", "5m", "15m", "30m", "1H", "4H", "1D", "1W", "1M"]
    return all_resolutions.index(resolution1) < all_resolutions.index(resolution2)
