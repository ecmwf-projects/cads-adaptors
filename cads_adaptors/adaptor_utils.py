from cacholote import decode

from . import adaptor


def get_adaptor_class(
    entry_point: str, setup_code: str | None = None
) -> type[adaptor.AbstractAdaptor]:
    try:
        adaptor_class = decode.import_object(entry_point)
        if setup_code is not None:
            raise TypeError
    except ValueError:
        exec(setup_code)
        adaptor_class = eval(entry_point)
    if not issubclass(adaptor_class, adaptor.AbstractAdaptor):
        raise TypeError(f"{adaptor_class!r} is not subclass of AbstractAdaptor")
    return adaptor_class
