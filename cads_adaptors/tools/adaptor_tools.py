from typing import Any

from cads_adaptors.adaptors import AbstractAdaptor
from cads_adaptors.exceptions import CdsConfigError


def handle_data_format(data_format: Any) -> str:
    if isinstance(data_format, (list, tuple, set)):
        data_format = list(data_format)
        assert len(data_format) == 1, "Only one value of data_format is allowed"
        data_format = data_format[0]

    if data_format in ["netcdf4", "netcdf", "nc"]:
        data_format = "netcdf"
    elif data_format in ["grib", "grib2", "grb", "grb2"]:
        data_format = "grib"
    return data_format


def get_data_format_from_mapped_requests(mapped_requests: list[dict[str, Any]]) -> str:
    """Extract the data_format from a list of mapped_requests.

    Ensures that there is a single value accross all requests, and that the value is normalised.

    Args:
        mapped_requests (list[dict[str, Any]]): _description_

    Raises
    ------
        CdsConfigError: Raised if a problem is encountered, indicating a problem with the configuration
        of the dataset.

    Returns
    -------
        str: The single, normalised data_format value extracted from the mapped_requests.
    """
    data_formats = [
        handle_data_format(req.pop("data_format", None)) for req in mapped_requests
    ]
    data_formats = list(set(data_formats))
    if len(data_formats) != 1 or data_formats[0] is None:
        # It should not be possible to reach here, if it is, there is a problem.
        raise CdsConfigError(
            "Something has gone wrong in preparing your request, "
            "please try to submit your request again. "
            "If the problem persists, please contact user support."
        )
    return data_formats[0]


def get_adaptor_class(
    entry_point: str, setup_code: str | None = None
) -> type[AbstractAdaptor]:
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
    if not issubclass(adaptor_class, AbstractAdaptor):
        raise TypeError(f"{adaptor_class!r} is not subclass of AbstractAdaptor")
    return adaptor_class  # type: ignore


def get_adaptor(
    config: dict[str, Any], form: list[dict[str, Any]] | dict[str, Any] | None = None
):
    config = config.copy()
    entry_point = config.pop("entry_point")
    setup_code = config.pop("setup_code", None)

    adaptor_class = get_adaptor_class(entry_point=entry_point, setup_code=setup_code)
    adaptor = adaptor_class(form=form, **config)  # type: ignore

    return adaptor
