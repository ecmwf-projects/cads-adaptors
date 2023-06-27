import xarray
import pandas
import requests
from ..baron_csv_cdm import names_conversion
from cds_common.system import utils
from datetime import datetime

# all columns that will be used as coordinates
_coordinates = [
    "secondary_id",
    "report_timestamp",
    "time_since_launch",
    "report_id",
    "height_of_station_above_sea_level",
    'longitude', 'latitude', 'air_pressure',
]


# other columns will be variables


def convert(csv_url, service_definition=None, context=None, columns=[], source='GRUAN'):
    """
    convert a csv file into a set of dataarrays
      :param csv_url:
      :param context: if none no file will be saved, otherwise, each data array will be added to a zip file
      :param columns: if not empty only the columns specified will be used as varaibles
      :param source:
      :return:
    """

    source_def = service_definition['sources'][source]
    df = pandas.read_csv(csv_url, comment='#', parse_dates=['report_timestamp'], infer_datetime_format=True)
    downcast = [c for c in df.columns if df[c].dtype == float]
    # downcasting is always good practice. Pandas by default will try to use only 64 bit numbers.
    # we will be downcasting all variables that are float64. For Integers we let the default precision
    # because they may be indexes
    for c in downcast:
        df[c] = pandas.to_numeric(df[c], downcast='float')
    coordinates = [c for c in df.columns if c in _coordinates]
    variables = [c for c in df.columns if c not in _coordinates]
    if len(columns) > 0:
        variables = [v for v in variables if v in columns]
    out = [] if context is None else {}
    attributes = {
        'Convention': 'CF-1.7',
        'institution': 'not specified',
        'history': 'csv converted to collection of netcdf files',
        'source': 'not specified'
    }
    if context is not None:
        resource = context.request['metadata']['resource']
        attributes = dict(
            Convention='CF-1.7',
            licences="; ".join(utils.get_licences(resource)),
            istitution="Copernicus Climate Change Services",
            history=f"Data retrieved from {utils.get_public_hostname() + '/cdsapp#!/dataset/' + resource} and converted to netcdf via xarray",
            source="C3S - CNR-IMAA - ECMWF"
        )
    for v in variables:
        df_tmp = df[_coordinates + [v]]
        ds = df_tmp.to_xarray()
        ds = ds.set_coords(_coordinates)
        ds.attrs.update(attributes)
        for _v in ds.variables:
            try:
                _n = names_conversion(v, source_def=source_def, out2in=False)
                print(_n)
                ds.variables[v].attrs.update(
                    {
                        'standard_name': v,
                        'comment': source_def['descriptions'][_n]['description'],
                        'units': source_def['descriptions'][_n]['units']
                    }
                )
            except Exception as err:
                print(err)
        if context is not None:
            out[v] = context.create_temp_file('.nc')
            ds.to_netcdf(out[v])
        else:
            out.append(ds)

    return out


def convert_station_split(csv_url, context=None, columns=[], source='GRUAN', service_definition={}):
    '''
    convert a csv file into a set of dataarrays
    :param csv_url:
    :param context: if none no file will be saved, otherwise, each data array will be added to a zip file
    :param columns: if not empty only the columns specified will be used as varaibles
    :param service_definition:
    :param columns: if not empty only the columns specified will be used as varaibles
    :return:
    '''
    source_def = service_definition['sources'][source]
    df = pandas.read_csv(csv_url, comment='#', parse_dates=['report_timestamp'], infer_datetime_format=True)
    downcast = [c for c in df.columns if df[c].dtype == float]
    # downcasting is always good practice. Pandas by default will try to use only 64 bit numbers.
    # we will be downcasting all variables that are float64. For Integers we let the default precision
    # because they may be indexes
    for c in downcast:
        df[c] = pandas.to_numeric(df[c], downcast='float')
    coordinates = [c for c in df.columns if c in _coordinates]
    variables = [c for c in df.columns if c not in _coordinates]
    if len(columns) > 0:
        variables = [v for v in variables if v in columns]
    out = [] if context is None else {}
    attributes = {
        'Convention': 'CF-1.7',
        'institution': 'not specified',
        'history': 'csv converted to collection of netcdf files',
        'source': 'not specified'
    }
    if context is not None:
        resource = context.request['metadata']['resource']
        attributes = dict(
            Convention='CF-1.7',
            licences="; ".join(utils.get_licences(resource)),
            istitution="Copernicus Climate Change Services",
            history=f"Data retrieved from {utils.get_public_hostname() + '/cdsapp#!/dataset/' + resource} and converted to netcdf via xarray",
            source="C3S - CNR-IMAA - ECMWF"
        )
    for v in variables:
        df_tmp = df[_coordinates + [v]]
        ds = df_tmp.to_xarray()
        ds = ds.set_coords(_coordinates)
        ds.attrs.update(attributes)
        for _v in ds.variables:
            try:
                _n = names_conversion(v, source_def=source_def, out2in=False)
                print(_n)
                ds.variables[v].attrs.update(
                    {
                        'standard_name': v,
                        'comment': source_def['descriptions'][_n]['description'],
                        'units': source_def['descriptions'][_n]['units']
                    }
                )
            except Exception as err:
                print(err)
        if context is not None:
            out[v] = context.create_temp_file('.nc')
            ds.to_netcdf(out[v])
        else:
            out.append(ds)

    return out


if __name__ == '__main__':
    csv_url = 'http://136.156.132.150/cache-compute-0000/cache/data1/adaptor.insitu_reference.retrieve-1588681629.420458-14963-1-16efdd30-9bfc-4783-80d2-c17281d3ae6b.zip'
    print(convert(csv_url, context=None, columns=[], source='GRUAN'))
