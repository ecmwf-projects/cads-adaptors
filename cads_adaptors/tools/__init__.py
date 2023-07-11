def ensure_list(input_item):
    if not isinstance(input_item, list):
        return [input_item]
    return input_item
