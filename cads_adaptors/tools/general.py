from typing import Any


def ensure_list(input_item: Any) -> list:
    """Ensure that item is a list, generally for iterability."""
    if isinstance(input_item, (list, tuple, set)):
        return list(input_item)
    if input_item is None:
        return []
    return [input_item]


def split_requests_on_keys(
    requests: list[dict[str, Any]], split_on_keys: list[str]
) -> list[dict]:
    """Split a request on keys, returning a list of requests."""
    if len(split_on_keys) == 0:
        return requests

    out_requests = []
    for request in requests:
        for key in split_on_keys:
            if key in request:
                values = ensure_list(request[key])
                if len(values) == 1:
                    out_requests.append(request)
                else:
                    for value in values:
                        new_request = request.copy()
                        new_request[key] = value
                        out_requests.append(new_request)
            else:
                out_requests.append(request)
    return out_requests
