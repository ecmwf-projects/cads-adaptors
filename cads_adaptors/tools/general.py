def ensure_list(input_item):
    """Ensure that item is a list, generally for iterability."""
    if isinstance(input_item, list):
        return input_item
    if isinstance(input_item, (tuple, set)):
        return list(input_item)
    return [input_item]
