import os
from collections import defaultdict
from datetime import datetime
from typing import Any

from cryptography.fernet import Fernet, InvalidToken


def ensure_list(input_item: Any) -> list:
    """Ensure that item is a list, generally for iterability."""
    if isinstance(input_item, (list, tuple, set)):
        return list(input_item)
    if input_item is None:
        return []
    return [input_item]


SPLIT_BY_MONTH_KEY = "__split_by_month"


def group_by_month(values, date_format):
    dates = [datetime.strptime(val, date_format) for val in values]
    months = defaultdict(list)
    for date in dates:
        month = date.replace(day=1)
        months[month].append(date.strftime(date_format))
    return list(months.values())


def split_requests_on_keys(
    requests: list[dict[str, Any]],
    split_on_keys: list[str],
    context=None,
    mapping: dict[str, Any] = dict(),
) -> list[dict]:
    """Split a request on keys, returning a list of requests."""
    import cads_adaptors.mapping as mapping_module

    if len(split_on_keys) == 0:
        return requests

    split_by_month = SPLIT_BY_MONTH_KEY in split_on_keys
    if split_by_month:
        mapping_options = mapping.get("options", {})
        if not mapping_options.get("wants_dates", False):
            split_by_month = False
            if context:
                context.error(
                    "For the time being, split-by-month is only supported for wants_dates=True!"
                )

    if split_by_month:
        date_keyword_configs = mapping_options.get(
            "date_keyword_config", mapping_module.DATE_KEYWORD_CONFIGS
        )
        if isinstance(date_keyword_configs, dict):
            date_keyword_configs = [date_keyword_configs]

    for key in split_on_keys:
        out_requests = []
        for request in requests:
            if key in request:
                values = ensure_list(request[key])
                if len(values) == 1:
                    out_requests.append(request)
                else:
                    if split_by_month:
                        for date_keyword_config in date_keyword_configs:
                            date_key = date_keyword_config.get("date_keyword", "date")
                            format_key = date_keyword_config.get(
                                "format_keyword", "date_format"
                            )
                            date_format = mapping_options.get(format_key, "%Y-%m-%d")
                            if key == date_key:
                                values = group_by_month(values, date_format)
                    for value in values:
                        new_request = request.copy()
                        new_request[key] = value
                        out_requests.append(new_request)
            else:
                out_requests.append(request)
        requests = out_requests

    return out_requests


def decrypt(token: str, key_name: str, raises: bool = True) -> str:
    try:
        key = os.environ[key_name]
    except KeyError:
        if not raises:
            return token
        raise

    f = Fernet(key.encode())
    try:
        decrypted = f.decrypt(token.encode())
    except InvalidToken:
        if not raises:
            return token
        raise

    return decrypted.decode()
