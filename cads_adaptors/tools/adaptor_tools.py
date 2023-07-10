from typing import Any

from cds_adaptors.adaptors import Base


def get_adaptor_class(
    entry_point: str, setup_code: str | None = None
) -> type[Base]:
    from cacholote import decode

    try:
        adaptor_class = decode.import_object(entry_point)
        if setup_code is not None:
            raise TypeError
    except ValueError:
        if setup_code is None:
            raise TypeError
        exec(setup_code)
        adaptor_class = eval(entry_point)
    if not issubclass(adaptor_class, Base):
        raise TypeError(f"{adaptor_class!r} is not subclass of Base")
    return adaptor_class  # type: ignore


def get_adaptor(config: dict[str, Any], form: dict[str, Any] | None = None):
    config = config.copy()
    entry_point = config.pop("entry_point")
    setup_code = config.pop("setup_code", None)

    adaptor_class = get_adaptor_class(entry_point=entry_point, setup_code=setup_code)
    adaptor = adaptor_class(form=form, **config)  # type: ignore

    return adaptor
