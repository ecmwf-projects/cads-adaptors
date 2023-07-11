
def ensure_list(input_item):
    """
    Ensure that item is a list, generally for iterability
    """
    if not isinstance(input_item, list):
        return [input_item]
    return input_item

