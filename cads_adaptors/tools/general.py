from typing import Any


def ensure_list(input_item: Any) -> list:
    """Ensure that item is a list, generally for iterability."""
    if isinstance(input_item, (list, tuple, set)):
        return list(input_item)
    if input_item is None:
        return []
    return [input_item]
