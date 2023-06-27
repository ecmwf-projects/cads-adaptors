import xarray as xr
import pandas as pd
import requests
import numpy as np
from cds_common.system import utils

from dateutil.parser._parser import ParserError

def out_name_from_sd(sd, source, col):
    return sd['sources'][source]['descriptions'][col]['name_for_output']

def coordinates_from_sd(sd, source):
    out = [list(x.keys())[0] for x in sd['sources'][source]['header_columns']]
    out += [out_name_from_sd(sd, source, x) for x in sd['sources'][source]['mandatory_columns']]
    out = list(set(out))
    return [o for o in sd['out_columns_order'] if o in out]

def sql_2_netcdf(
    query, db_engine,
    dataset: str, source: str, endpoint: str,
    context=None, chunksize = 10000,
    table=None
): 
    """
    Function to stream data from a postgres database into a netCDF file.
    To reduce computational expense, we read chunks of 10000 rows at a time
    and create temporary nectCDF files. We then merge these and modify attributes.
    """
    resource = context.request['metadata']['resource']
    service_definition = requests.get(f'http://{endpoint}/api/{dataset}/service_definition').json()
    context.debug(f'service definition location: http://{endpoint}/api/{dataset}/service_definition')
    source_def = service_definition['sources'][source]
    out2in_names = {
        source_def['descriptions'].get(_var,{}).get('name_for_output',_var): _var
        for _var in source_def.get('descriptions', {})
    }
    # datetime_variables = source_def.get('datetime_columns', [])
    array_variables = source_def.get('array_columns', {})
    _coordinates = coordinates_from_sd(service_definition, source)
    if table is None: table = source.lower()
    conn = db_engine.connect().execution_options(
        stream_results=True
    )
    cdt_query = (
        "select column_name, data_type from information_schema.columns where"
        f" table_name='{table}';"
    )
    context.debug(f'cdt_query: {cdt_query}')
    column_data_types = pd.read_sql(
        f"{cdt_query}", conn,
    ).set_index('column_name').to_dict()['data_type']
    context.debug(f'column_data_types: {column_data_types}')
    
    coordinates = [c for c in column_data_types.keys() if c in _coordinates]
    # variables = [c for c in column_data_types.keys() if c not in _coordinates]

    iterable_dataframe =  pd.read_sql(
        f"{query}", conn, chunksize=chunksize,
    )
    dindex=0
    tmpncfiles = []
    index_dims = {}   # Dimensions made up on the fly based on DB content
    index_dim_count = 1
    for i, iterable in enumerate(iterable_dataframe):
        ds = iterable.to_xarray()
        # Update index coordinate to start from the end of previous
        ds = ds.assign_coords({'index':ds['index'].values+dindex})
        dindex = ds['index'].values[-1]
        for var in [v for v in ds if ds[v].dtype==object]:
            in_var = out2in_names.get(var, var)
            if 'timestamp' in column_data_types.get(var, ''):
                # convert any datetime variables
                try:
                    dt_values = pd.to_datetime(ds[var].values).tz_localize(None)
                    dt_dtype = dt_values.dtype
                    ds[var].values = dt_values
                    ds[var] = ds[var].astype(dt_dtype)
                except ParserError:
                    # Non compatible date so leave as string
                    pass

            if in_var in array_variables:
                context.debug(f'db to netcdf, array variable: {var}')
                new_dimension = array_variables.get(in_var, {}).get(
                    'new_dimension', array_variables.get(
                        '_new_dimension', {}
                    )
                )
                if 'array' in column_data_types[var].lower():
                    # If no new dimension is defined we must construct an
                    # arbitary one based on the shape of the data in the DB
                    # Doing this here so no need to pass DB connections into other functions
                    if new_dimension.get('values') is None:
                        # First see if we have constructed the index_dim before:
                        if var in index_dims:
                            new_dimension = index_dims[var]
                        else:
                            if 'nvalues' in new_dimension:
                                dim_length = new_dimension.get('nvalues')
                            else:
                                dim_length_query = f'select distinct array_length({var}, 1) from {source.lower()};'
                                dim_length_res = pd.read_sql(f"{dim_length_query}", conn)
                                dim_length = dim_length_res['array_length'].max()
                            
                            new_dimension['values'] = np.arange(dim_length)
                            new_dimension.setdefault('units', '1')
                            new_dimension.setdefault('name', f'dimension_{index_dim_count}')
                            new_dimension['name'] = new_dimension['name'].replace(' ', '_')
                            index_dims[var] = new_dimension
                            index_dim_count += 1

                    new_var_da = sqlarray_to_xarray(
                        ds[var], new_dimension
                    )
                    ds[var] = new_var_da
                else:
                    # Assume it is a string that will be cut using a delimiter
                    delimiter = array_variables.get('_delimiter', ',')
                    dtype = array_variables.get('_dtype', 'float')
                    ds[var] = sqlstr_to_xarray(
                        ds[var], new_dimension, delimiter, dtype
                    )
                
        # Assign coordinate vars (if necessary)
        ds = ds.assign_coords({var: ds[var] for var in _coordinates})
        context.debug(f'db to netcdf: {ds}')
        tmpncfiles.append(context.create_temp_file('.nc'))
        ds.to_netcdf(tmpncfiles[-1])

    # Open all the temporary netCDF files
    ds = xr.open_mfdataset(tmpncfiles)

    # Add any variable attributes
    for var in ds.variables:
        _var = out2in_names.get(var, var)
        var_description = source_def['descriptions'].get(_var, {})
        var_output_attributes = var_description.get('output_attributes', {})

        varname = var_description.get('name_for_output', var)
        description = var_description.get('description', '')
        long_name = var_description.get('long_name', varname.title().replace('_',' '))
        units = var_description.get('units', '')
        dtype = var_description.get('dtype', '')

        var_output_attributes.setdefault('long_name', long_name)
        if description: var_output_attributes.setdefault('comment', description)
        if units: var_output_attributes.setdefault('units', units)
        ds[var] = ds[var].assign_attrs(var_output_attributes)
        if dtype: ds[var] = ds[var].astype(dtype)
    
    # Add any global attributes:
    global_attributes = source_def.get('global_attributes', {})
    global_attributes.setdefault("licences", "; ".join(utils.get_licences(resource)))
    global_attributes.setdefault("institution", "Copernicus Climate Change Service (C3S)")
    global_attributes.setdefault("history", (
        f"Data retrieved from {utils.get_public_hostname()}/cdsapp#!/dataset/{resource}"
        " and converted to netcdf using xarray."
    ))
    ds = ds.assign_attrs(global_attributes)

    # Write output to temporary netcdf file
    netcdf = context.create_temp_file('.nc')
    ds.to_netcdf(netcdf)
    return netcdf
    

def sqlstr_to_xarray(
    da, new_dimension, delimiter=',', dtype='float',
):
    split_values = [line.split(delimiter) for line in da.values]
    # check all lines are same length (fill with NaNs if any lines not long enough)
    lengths = [len(line) for line in split_values]
    max_length = max(lengths)
    if any([l<max_length for l in lengths]):
        for i in [i for i, l in enumerate(lengths) if l<max_length]:
            # As it is not possible to determine which values
            # have been omitted, we make the whole line nan
            split_values[i] = ['nan' for j in range(max_length)]
    
    array_values = np.array(split_values).astype(dtype)

    return update_dataarray(da, array_values, new_dimension, max_length)


def sqlarray_to_xarray(
    da, new_dimension
):
    dim_length = len(new_dimension['values'])
    values_as_lists = []
    for vals in da.values:
        if vals is None: vals=[]
        if len(vals)<dim_length:
            # if length of array is not long enough, fill with nans
            vals += [np.nan]*int(dim_length-len(vals))
        elif len(vals)>dim_length:
            # if length of array is too long, we chop it down to length of dim
            vals = vals[:dim_length]
        values_as_lists.append(vals)

    array_values = np.array(values_as_lists)

    return update_dataarray(da, array_values, new_dimension, dim_length)


def update_dataarray(da, array_values, new_dimension, dim_length):
    new_dim_name = new_dimension.get('name', 'new_dim')
    attrs=new_dimension.get('attrs', {})
    attrs.setdefault('units', '1') # Set default units
    new_dim = xr.DataArray(
        new_dimension.get('values', np.arange(dim_length)),
        name=new_dim_name,
        dims=[new_dim_name],
        attrs=attrs
    )
    new_dim.assign_coords({new_dim_name:new_dim})
    # Create new variable dataarray
    new_da_coords = {c:da[c] for c in da.coords}
    new_da_coords.update({new_dim_name: new_dim})
    new_da = xr.DataArray(
        array_values,
        dims=list(da.dims)+[new_dim_name],
        coords=new_da_coords,
        name=da.name,
        attrs=da.attrs
    )
    return new_da


# Regression test for the netCDF convertor
def test():
    import cdsapi
    import xarray as xr
    import shutil
    import zipfile

    c = cdsapi.Client()
    zip_file = 'download.zip'
    c.retrieve(
        'insitu-observations-woudc-ozone-total-column-and-profiles',
        {
            'observation_type': 'total_column',
            'variable': 'total_ozone_column',
            'year': '2020',
            'month': '01',
            'day': ['16', '31'],
            'format': 'netcdf',
        },
        zip_file,
    )
    with zipfile.ZipFile('download.zip', 'r') as z:
        temp_file= [f for f in z.namelist()][0]
        z.extract(temp_file)

    ds = xr.open_dataset(temp_file)
    assert float(ds.total_ozone_column.mean().values) == 326.38508474576275
    print('Regression test passed, removing files')
    shutil.rmtree(temp_file, ignore_errors=True)
    shutil.rmtree(zip_file, ignore_errors=True)