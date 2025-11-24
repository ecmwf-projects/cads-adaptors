import h5netcdf
import numpy
import xarray

from cads_adaptors.exceptions import CadsObsRuntimeError


def get_code_mapping(
    incobj: h5netcdf.File | xarray.Dataset, inverse: bool = False
) -> dict:
    import h5netcdf

    if isinstance(incobj, h5netcdf.File):
        attrs = incobj.variables["observed_variable"].attrs
    elif isinstance(incobj, xarray.Dataset):
        attrs = incobj["observed_variable"].attrs
    else:
        raise CadsObsRuntimeError("Unsupported input type")
    # Take into account that if there is only one value, these attrs are not iterable.
    if isinstance(attrs["codes"], numpy.ndarray):
        labels, codes = attrs["labels"], attrs["codes"]
    else:
        labels = numpy.array(
            [
                attrs["labels"],
            ]
        )
        codes = numpy.array(
            [
                attrs["codes"],
            ]
        )
    if inverse:
        mapping = {c: v for v, c in zip(labels, codes)}
    else:
        mapping = {v: c for v, c in zip(labels, codes)}
    return mapping
