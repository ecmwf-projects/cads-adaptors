from typing import Tuple

import h5netcdf
import numpy
from fsspec.implementations.http import HTTPFileSystem

from cads_adaptors.adaptors.cadsobs.codes import _get_code_mapping
from cads_adaptors.adaptors.cadsobs.filter import _get_url_ncobj


def handle_string_dims(
    char_sizes: dict[str, int],
    chunksize: Tuple[int, ...],
    dimensions: Tuple[str, ...],
    ivar: str,
    oncobj: h5netcdf.File,
) -> Tuple[Tuple[int, ...], Tuple[str, ...]]:
    """Add dimensions for character variables."""
    ivar_str_dim = ivar + "_stringdim"
    ivar_str_dim_size = char_sizes[ivar]
    if ivar_str_dim not in oncobj.dimensions:
        oncobj.dimensions[ivar_str_dim] = ivar_str_dim_size
    dimensions += (ivar_str_dim,)
    chunksize += (ivar_str_dim_size,)
    return chunksize, dimensions


def get_char_sizes(fs: HTTPFileSystem, object_urls: list[str]) -> dict[str, int]:
    """
    Iterate over the input files to get the size of the string variables.

    We need to know this beforehand so we can stream to the output file.
    """
    char_sizes = {}
    for url in object_urls:
        with _get_url_ncobj(fs, url) as incobj:
            for var, varobj in incobj.items():
                if varobj.dtype.kind == "S":
                    char_size = varobj.shape[1]
                else:
                    continue
                if var not in char_sizes:
                    char_sizes[var] = char_size
                else:
                    char_sizes[var] = max(char_sizes[var], char_size)

    return char_sizes


def concat_str_array(iarray: numpy.ndarray) -> numpy.ndarray:
    """Concatenate an array of strings to get a 1D array."""
    field_len, strlen = iarray.shape
    return iarray.view(f"S{strlen}").reshape(field_len)


def dump_char_variable(
    current_size: int,
    incobj: h5netcdf.File,
    ivar: str,
    ivarobj: h5netcdf.Variable,
    mask: numpy.typing.NDArray,
    new_size: int,
    ovar: h5netcdf.Variable,
    download_all_chunk: bool,
):
    if ivar != "observed_variable":
        actual_str_dim_size = ivarobj.shape[-1]
        if download_all_chunk:
            data = ivarobj[:, 0:actual_str_dim_size][mask, :]
        else:
            data = ivarobj[mask, 0:actual_str_dim_size]
        ovar[current_size:new_size, 0:actual_str_dim_size] = data
    else:
        # For observed variable, we use the attributes to decode the integers.
        if download_all_chunk:
            data = ivarobj[:][mask]
        else:
            data = ivarobj[mask]
        code2var = _get_code_mapping(incobj, inverse=True)
        codes_in_data, inverse = numpy.unique(data, return_inverse=True)
        variables_in_data = numpy.array(
            [code2var[c].encode("utf-8") for c in codes_in_data]
        )
        data_decoded = variables_in_data[inverse]
        data_decoded = data_decoded.view("S1").reshape(data.size, -1)
        actual_str_dim_size = data_decoded.shape[-1]
        ovar[current_size:new_size, 0:actual_str_dim_size] = data_decoded
