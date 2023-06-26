from typing import Any

from cacholote import decode

from .. import adaptor


def get_adaptor_class(
    entry_point: str, setup_code: str | None = None
) -> type[adaptor.AbstractAdaptor]:
    try:
        adaptor_class = decode.import_object(entry_point)
        if setup_code is not None:
            raise TypeError
    except ValueError:
        if setup_code is None:
            raise TypeError
        exec(setup_code)
        adaptor_class = eval(entry_point)
    if not issubclass(adaptor_class, adaptor.AbstractAdaptor):
        raise TypeError(f"{adaptor_class!r} is not subclass of AbstractAdaptor")
    return adaptor_class  # type: ignore


def get_adaptor(config: dict[str, Any], form: dict[str, Any] | None = None):
    config = config.copy()
    entry_point = config.pop("entry_point")
    setup_code = config.pop("setup_code", None)

    adaptor_class = get_adaptor_class(entry_point=entry_point, setup_code=setup_code)
    adaptor = adaptor_class(form=form, **config)  # type: ignore

    return adaptor
