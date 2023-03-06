from typing import Any

from cacholote import decode

from . import adaptor


def instantiate_adaptor(
    entry_point: str, setup_code: str | None = None, **kwargs: Any
) -> adaptor.AbstractAdaptor:
    try:
        adaptor_class = decode.import_object(entry_point)
        if setup_code is not None:
            raise TypeError
    except ValueError:
        exec(setup_code)
        adaptor_class = eval(entry_point)
    return adaptor_class(**kwargs)
